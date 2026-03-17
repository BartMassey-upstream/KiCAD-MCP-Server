#!/usr/bin/python3
"""
Download JLCPCB parts database from jlcparts (yaqwsx).

Downloads ~960MB of split zip archives, extracts to a ~12GB
SQLite database (cache.sqlite3), then imports parts into the
local jlcpcb_parts.db used by this MCP server.

Source: https://github.com/yaqwsx/jlcparts

Requirements: 7z (p7zip-full), ~13GB free disk space
"""
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# -- Load our JLCPCBPartsManager without triggering the full
#    commands package __init__.py --
def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_base = Path(__file__).parent / "python" / "commands"
_jlcpcb_parts = _load_module(
    "jlcpcb_parts", _base / "jlcpcb_parts.py"
)
JLCPCBPartsManager = _jlcpcb_parts.JLCPCBPartsManager

# -- Config --
BASE_URL = "https://yaqwsx.github.io/jlcparts/data"
# 20 volumes: cache.z01 .. cache.z19 + cache.zip
PARTS = (
    [f"cache.z{i:02d}" for i in range(1, 20)]
    + ["cache.zip"]
)
MAX_RETRIES = 5
RETRY_WAIT = 10  # seconds


def download_file(url, dest):
    """Download a file with progress and resume support."""
    headers = {}
    mode = "wb"
    existing = 0
    if os.path.exists(dest):
        existing = os.path.getsize(dest)
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                url, stream=True, timeout=60,
                headers=headers,
            )
            if resp.status_code == 416:
                # Already fully downloaded
                return
            resp.raise_for_status()

            total = int(
                resp.headers.get("content-length", 0)
            ) + existing
            downloaded = existing

            with open(dest, mode) as f:
                for chunk in resp.iter_content(1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded * 100 // total
                        print(
                            f"\r  {downloaded/1024/1024:.1f}/"
                            f"{total/1024/1024:.1f}MB "
                            f"({pct}%)",
                            end="", flush=True,
                        )
            print()
            return

        except (
            requests.exceptions.RequestException,
            ConnectionError,
        ) as e:
            wait = RETRY_WAIT * (attempt + 1)
            if attempt < MAX_RETRIES - 1:
                print(
                    f"\n  Error: {e}\n"
                    f"  Retrying in {wait}s "
                    f"({attempt+1}/{MAX_RETRIES})..."
                )
                time.sleep(wait)
                # Update resume position for next attempt
                if os.path.exists(dest):
                    existing = os.path.getsize(dest)
                    headers["Range"] = f"bytes={existing}-"
                    mode = "ab"
            else:
                raise


def download_archives(dl_dir):
    """Download all split archive parts."""
    for i, part in enumerate(PARTS, 1):
        url = f"{BASE_URL}/{part}"
        dest = os.path.join(dl_dir, part)

        # Skip if we can verify size via HEAD
        try:
            head = requests.head(
                url, timeout=15, allow_redirects=True
            )
            expected = int(
                head.headers.get("content-length", 0)
            )
            if (
                os.path.exists(dest)
                and os.path.getsize(dest) == expected
                and expected > 0
            ):
                print(
                    f"  [{i}/{len(PARTS)}] {part}: "
                    f"already complete "
                    f"({expected/1024/1024:.1f}MB)"
                )
                continue
        except Exception:
            pass

        print(f"  [{i}/{len(PARTS)}] {part}:")
        download_file(url, dest)


def extract_database(dl_dir):
    """Extract split zip to get cache.sqlite3."""
    zip_path = os.path.join(dl_dir, "cache.zip")
    db_path = os.path.join(dl_dir, "cache.sqlite3")

    if os.path.exists(db_path):
        size = os.path.getsize(db_path)
        if size > 1_000_000_000:  # > 1GB = likely valid
            print(
                f"Database already extracted: "
                f"{size/1024/1024/1024:.1f}GB"
            )
            return db_path

    print("Extracting with 7z (this may take a while)...")
    result = subprocess.run(
        ["7z", "x", "-y", f"-o{dl_dir}", zip_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"7z stdout: {result.stdout[-500:]}")
        print(f"7z stderr: {result.stderr[-500:]}")
        sys.exit(1)

    if not os.path.exists(db_path):
        print("ERROR: cache.sqlite3 not found after extraction")
        sys.exit(1)

    size = os.path.getsize(db_path) / 1024 / 1024 / 1024
    print(f"Extracted: {size:.1f}GB")
    return db_path


def import_into_local_db(source_path):
    """Import from jlcparts cache.sqlite3 into our DB.

    The jlcparts schema stores category and manufacturer in
    separate lookup tables.  The ``v_components`` view joins
    them so we get human-readable strings directly.  When the
    view is unavailable we fall back to the raw ``components``
    table (category/manufacturer will be empty).

    The raw ``description`` column is often blank; the ``extra``
    JSON column usually carries a richer ``description`` field
    that we prefer.
    """
    src = sqlite3.connect(source_path)
    src.row_factory = sqlite3.Row

    # Prefer the denormalized view when available
    views = [
        v["name"] for v in src.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='view'"
        ).fetchall()
    ]

    if "v_components" in views:
        source_table = "v_components"
    else:
        source_table = "components"

    src_cols = [
        c["name"] for c in
        src.execute(
            f"PRAGMA table_info([{source_table}])"
        ).fetchall()
    ]
    total = src.execute(
        f"SELECT COUNT(*) as n FROM [{source_table}]"
    ).fetchone()["n"]
    print(
        f"Importing from '{source_table}' "
        f"({total} rows, cols={src_cols})"
    )

    db = JLCPCBPartsManager()
    cursor = db.conn.cursor()

    def get_col(row, *names):
        """Get first matching column value."""
        for n in names:
            try:
                val = row[n]
                if val is not None:
                    return val
            except (IndexError, KeyError):
                continue
        return None

    batch_size = 10000
    imported = 0
    now_ts = int(datetime.now().timestamp())

    print(f"Importing {total} parts...")

    for offset in range(0, total, batch_size):
        rows = src.execute(
            f"SELECT * FROM [{source_table}] "
            f"LIMIT {batch_size} OFFSET {offset}"
        ).fetchall()

        for row in rows:
            try:
                lcsc = get_col(
                    row, "lcsc", "LCSC",
                    "lcsc_part", "componentCode",
                )
                if lcsc is None:
                    continue

                # Ensure C prefix
                lcsc = str(lcsc)
                if not lcsc.startswith("C"):
                    lcsc = f"C{lcsc}"

                # Extract price - may be JSON string or number
                price_raw = get_col(row, "price", "price1")
                if isinstance(price_raw, str):
                    try:
                        price_json = price_raw
                        json.loads(price_raw)
                    except (json.JSONDecodeError, TypeError):
                        price_json = json.dumps(
                            [{"qty": 1, "price": price_raw}]
                        )
                elif price_raw is not None:
                    price_json = json.dumps(
                        [{"qty": 1, "price": float(price_raw)}]
                    )
                else:
                    price_json = "[]"

                # Library type
                basic = get_col(
                    row, "basic", "is_basic", "library_type",
                    "libraryType",
                )
                preferred = get_col(row, "preferred")
                if preferred in (1, True):
                    library_type = "Preferred"
                elif basic in (
                    1, True, "true", "Basic", "base",
                ):
                    library_type = "Basic"
                elif basic in ("Preferred",):
                    library_type = "Preferred"
                else:
                    library_type = "Extended"

                # Description: prefer the value from the
                # ``extra`` JSON blob when the direct column
                # is empty (common in jlcparts).
                description = str(
                    get_col(row, "description", "describe")
                    or ""
                )
                if not description:
                    extra_raw = get_col(row, "extra")
                    if extra_raw and isinstance(extra_raw, str):
                        try:
                            extra = json.loads(extra_raw)
                            description = extra.get(
                                "description", ""
                            ) or ""
                        except (json.JSONDecodeError, TypeError):
                            pass

                cursor.execute('''
                    INSERT OR REPLACE INTO components (
                        lcsc, category, subcategory, mfr_part,
                        package, solder_joints, manufacturer,
                        library_type, description, datasheet,
                        stock, price_json, last_updated
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''', (
                    lcsc,
                    str(get_col(
                        row, "category", "firstSortName",
                    ) or ""),
                    str(get_col(
                        row, "subcategory", "secondSortName",
                    ) or ""),
                    str(get_col(
                        row, "mfr", "mfr_part",
                        "componentModelEn",
                    ) or ""),
                    str(get_col(
                        row, "package",
                        "componentSpecificationEn",
                    ) or ""),
                    int(get_col(
                        row, "joints", "solder_joints",
                        "soldPoint",
                    ) or 0),
                    str(get_col(
                        row, "manufacturer",
                        "componentBrandEn",
                    ) or ""),
                    library_type,
                    description,
                    str(get_col(
                        row, "datasheet", "dataManualUrl",
                    ) or ""),
                    int(get_col(
                        row, "stock", "stockCount",
                    ) or 0),
                    price_json,
                    now_ts,
                ))
                imported += 1

            except Exception as e:
                # Skip bad rows silently in bulk import
                pass

        db.conn.commit()
        pct = min(100, (offset + batch_size) * 100 // total)
        print(
            f"\r  {imported} parts imported ({pct}%)",
            end="", flush=True,
        )

    print()

    # Rebuild FTS index
    print("Rebuilding full-text search index...")
    cursor.execute(
        "INSERT INTO components_fts(components_fts) "
        "VALUES('rebuild')"
    )
    db.conn.commit()

    stats = db.get_database_stats()
    print(f"\nDone! {stats}")

    src.close()
    db.close()


def main():
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    dl_dir = str(data_dir / "jlcparts_download")
    os.makedirs(dl_dir, exist_ok=True)

    print("=== JLCPCB Parts Database Download ===")
    print(f"Download dir: {dl_dir}")
    print(f"Requires: ~960MB download, ~12GB extracted\n")

    print("Step 1/3: Download archives")
    download_archives(dl_dir)

    print("\nStep 2/3: Extract database")
    source_db = extract_database(dl_dir)

    print("\nStep 3/3: Import into local database")
    import_into_local_db(source_db)

    print(
        "\nYou can delete the download dir to free space:"
    )
    print(f"  rm -rf {dl_dir}")


if __name__ == "__main__":
    main()
