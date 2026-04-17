"""Use Claude to discover name mappings for unmatched MSCI announcement companies.

Sends batches of unmatched company names + the current ACWI holdings list to Claude,
asking it to identify mergers, renames, and restructurings.

Usage:
    cd backend
    ANTHROPIC_API_KEY=sk-... uv run python -m index_universe.discover_overrides [--dry-run]
"""

from __future__ import annotations

import json
import os
import sys

import anthropic

from .acwi import (
    compute_net_additions,
    load_acwi_holdings,
    _load_name_overrides,
    _NAME_OVERRIDES_FILE,
)

BATCH_SIZE = 40  # companies per Claude call


def _build_holdings_reference(holdings: list[dict]) -> str:
    """Build a compact reference list of current holdings."""
    lines = []
    for h in sorted(holdings, key=lambda x: x["Name"]):
        lines.append(f"{h['Ticker']:10s} | {h['Name']}")
    return "\n".join(lines)


def _ask_claude(
    client: anthropic.Anthropic,
    unmatched: list[str],
    holdings_ref: str,
) -> dict[str, str]:
    """Send a batch of unmatched names to Claude and get mappings back."""
    unmatched_list = "\n".join(f"- {name}" for name in unmatched)

    resp = client.messages.create(
        model="claude-sonnet-4-5-20241022",
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": f"""I have a list of company names from historical MSCI index announcements that I need to match against current ACWI ETF holdings. Many of these companies have been renamed, merged, or restructured since the announcement.

For each unmatched company below, determine if it corresponds to a company in the current holdings list (via merger, rename, spin-off, or acquisition). Only include matches you are confident about.

Return ONLY a JSON object mapping the old announcement name (exactly as written) to the current holding name (exactly as it appears in the holdings list). If a company has no match in the current holdings (e.g., it went private, was delisted, or was acquired by a non-ACWI company), do not include it.

UNMATCHED COMPANIES:
{unmatched_list}

CURRENT ACWI HOLDINGS (Ticker | Name):
{holdings_ref}

Return only the JSON object, no other text. Example format:
{{"CABOT OIL & GAS CORP": "COTERRA ENERGY INC", "OLD NAME": "NEW NAME"}}""",
            }
        ],
    )

    text = resp.content[0].text.strip()
    # Extract JSON from response (might be wrapped in ```json blocks)
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        text = text.rsplit("```", 1)[0]
    return json.loads(text)


def main():
    dry_run = "--dry-run" in sys.argv

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: Set ANTHROPIC_API_KEY environment variable")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    # Load current state
    holdings, _ = load_acwi_holdings()
    holdings_ref = _build_holdings_reference(holdings)
    existing_overrides = _load_name_overrides()

    results = compute_net_additions()
    unmatched = [
        r["company_name"]
        for r in results
        if not r["matched"] and r["company_name"].upper() not in existing_overrides
    ]

    print(f"Unmatched companies: {len(unmatched)}")
    print(f"Existing overrides: {len(existing_overrides)}")
    print(f"Holdings: {len(holdings)}")
    print()

    if not unmatched:
        print("Nothing to discover!")
        return

    # Process in batches
    all_mappings: dict[str, str] = {}
    for i in range(0, len(unmatched), BATCH_SIZE):
        batch = unmatched[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(unmatched) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"Batch {batch_num}/{total_batches} ({len(batch)} companies)...")

        try:
            mappings = _ask_claude(client, batch, holdings_ref)
            print(f"  Found {len(mappings)} mappings")
            for old, new in mappings.items():
                print(f"    {old} -> {new}")
            all_mappings.update(mappings)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

    print(f"\nTotal new mappings discovered: {len(all_mappings)}")

    if dry_run:
        print("\n[DRY RUN] Would write these mappings:")
        print(json.dumps(all_mappings, indent=2, ensure_ascii=False))
        return

    if not all_mappings:
        print("No new mappings to save.")
        return

    # Merge with existing overrides and save
    try:
        with open(_NAME_OVERRIDES_FILE, "r", encoding="utf-8") as f:
            existing = json.load(f)
    except Exception:
        existing = {}

    existing.update(all_mappings)

    with open(_NAME_OVERRIDES_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(existing)} total overrides to {_NAME_OVERRIDES_FILE}")


if __name__ == "__main__":
    main()
