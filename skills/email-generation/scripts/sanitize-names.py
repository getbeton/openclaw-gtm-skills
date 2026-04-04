#!/usr/bin/env python3
"""
sanitize-names.py — Clean contact CSV names before email generation.

Removes/fixes:
- Name prefixes (Dr, Prof, Mr, Mrs, Ms, Sir, etc.)
- All-caps names → Title Case
- Single-character names
- Junk values (N/A, Test, -, ?, n/a, none, null)
- Names with emoji or non-printable characters

Usage:
    python3 sanitize-names.py input.csv [output.csv]

Output:
    {input}_sanitized.csv (or specified output path)
    Prints removed rows and fixes to stdout
"""

import csv
import re
import sys
import unicodedata
from pathlib import Path

PREFIXES = {
    "dr", "dr.", "prof", "prof.", "mr", "mr.", "mrs", "mrs.", "ms", "ms.",
    "miss", "sir", "lord", "dame", "rev", "rev.", "hon", "hon.",
}

JUNK_VALUES = {
    "n/a", "na", "-", "?", "none", "null", "test", "unknown", "tbd",
    "first", "last", "firstname", "lastname", "name", "user", "",
}

EMOJI_PATTERN = re.compile(
    "[\U00010000-\U0010ffff"
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "]+",
    flags=re.UNICODE,
)


def strip_prefix(name: str) -> str:
    parts = name.strip().split()
    if parts and parts[0].lower().rstrip(".") in PREFIXES:
        parts = parts[1:]
    return " ".join(parts)


def fix_casing(name: str) -> str:
    if name.isupper() or name.islower():
        return name.title()
    return name


def has_emoji(text: str) -> bool:
    return bool(EMOJI_PATTERN.search(text))


def is_junk(name: str) -> bool:
    clean = name.strip().lower()
    return clean in JUNK_VALUES


def is_single_char(name: str) -> bool:
    return len(name.strip().replace(".", "").replace(" ", "")) <= 1


def sanitize_name(name: str) -> tuple[str, list[str]]:
    """Returns (cleaned_name, list_of_changes). cleaned_name is None if row should be removed."""
    changes = []
    original = name

    if has_emoji(name):
        return None, [f"removed: emoji in name '{original}'"]

    if is_junk(name):
        return None, [f"removed: junk value '{original}'"]

    cleaned = strip_prefix(name)
    if cleaned != name:
        changes.append(f"stripped prefix: '{name}' → '{cleaned}'")
        name = cleaned

    if is_single_char(name):
        return None, [f"removed: single-char name '{original}'"]

    fixed = fix_casing(name)
    if fixed != name:
        changes.append(f"fixed casing: '{name}' → '{fixed}'")
        name = fixed

    return name, changes


def detect_name_columns(headers: list[str]) -> tuple[str | None, str | None]:
    """Detect first_name and last_name columns (case-insensitive)."""
    first, last = None, None
    for h in headers:
        hl = h.lower().strip()
        if hl in ("first_name", "firstname", "first"):
            first = h
        elif hl in ("last_name", "lastname", "last"):
            last = h
    return first, last


def main():
    if len(sys.argv) < 2:
        print("Usage: sanitize-names.py input.csv [output.csv]")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else \
        input_path.parent / (input_path.stem + "_sanitized.csv")

    rows = []
    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    first_col, last_col = detect_name_columns(headers)

    if not first_col:
        print("WARNING: Could not detect first_name column. Passing through unchanged.")
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        sys.exit(0)

    kept, removed, fixes = [], [], []

    for i, row in enumerate(rows, 1):
        first = row.get(first_col, "").strip()
        last = row.get(last_col, "").strip() if last_col else ""

        clean_first, changes_first = sanitize_name(first)
        clean_last, changes_last = sanitize_name(last) if last else (last, [])

        if clean_first is None:
            removed.append(f"  Row {i} ({row.get('company_name', '?')}): {changes_first[0]}")
            continue

        if last_col and clean_last is None:
            removed.append(f"  Row {i} ({row.get('company_name', '?')}): {changes_last[0]}")
            continue

        row[first_col] = clean_first
        if last_col and clean_last is not None:
            row[last_col] = clean_last

        all_changes = changes_first + (changes_last or [])
        if all_changes:
            fixes.extend([f"  Row {i}: {c}" for c in all_changes])

        kept.append(row)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(kept)

    print(f"\nInput:   {len(rows)} rows")
    print(f"Output:  {len(kept)} rows → {output_path}")
    print(f"Removed: {len(removed)} rows")

    if removed:
        print("\nRemoved rows:")
        for r in removed:
            print(r)

    if fixes:
        print(f"\nFixed ({len(fixes)} changes):")
        for f in fixes[:20]:
            print(f)
        if len(fixes) > 20:
            print(f"  ... and {len(fixes) - 20} more")

    print("\nReview removed rows before proceeding.")


if __name__ == "__main__":
    main()
