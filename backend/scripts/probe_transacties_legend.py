"""What does the Transacties `Tt` column mean? — the cheapest probe, read-only.

We know `A` = Aankoop and `V` = Verkoop because the money columns say so. We do NOT know `D`. One
instance has been measured (KLA-Tencor, 2026-06-12, shares in, no cash, exactly 10:1 on two
different books) and it is beyond reasonable doubt a stock split — but that is a measurement of a
CASE, not of the CODE. If `D` is a general "not a buy or sell" bucket it would also carry
spin-offs, mergers, reverse splits and transfers, each needing different arithmetic and one of them
carrying money that is not on this sheet.

Three cheap places the answer may already be sitting, in order of cost:

  1. THE HTML RENDERING OF THE SAME REPORT. The endpoint is `rapportFrontofficeClientAfdrukkenHtml`
     — `type=xls` is an OVERRIDE, so dropping it should return the printable HTML, which is where a
     legend or a column tooltip would live.
  2. A SECOND WORKSHEET. `pd.read_excel` reads sheet 0 only, so a legend tab would be invisible to
     the parser and to everything downstream.
  3. A PREAMBLE OR FOOTER IN SHEET 0. The parser takes row 0 as the header; anything above it is
     consumed and anything far below the data is dropped.

⚠ READ-ONLY, AND ONE LOGIN. It downloads two reports for one account and writes nothing — no DB,
no cache, no state. Safe to run against production, which is the whole point of it being the
cheapest option.
"""
from __future__ import annotations

import argparse
import re
import sys
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402, F401  — loads .env before anything reads a credential

DEFAULT_ACCOUNT = "BUS_Offensief_Dyn"
DEFAULT_VAN = "2026-01-01"
DEFAULT_TOT = "2026-08-05"

# The codes we already understand, so the report's own words about them can be recognised.
KNOWN = {"A": "Aankoop (buy)", "V": "Verkoop (sell)"}


def _say(step: str, detail: str = "") -> None:
    """⚠ Every step announced. A probe that prints nothing for thirty seconds behind a headless
    browser is indistinguishable from one that has hung."""
    print(f"[probe] {step}{': ' + detail if detail else ''}", flush=True)


def fetch(account: str, van: str, tot: str, as_xls: bool) -> bytes:
    from airs_scanner import BASE_URL, _session  # noqa: PLC0415

    params = {"rapport_types": "TRANS", "Portefeuille": account,
              "datum_van": van, "datum_tot": tot}
    if as_xls:
        params["type"] = "xls"
    url = f"{BASE_URL}/rapportFrontofficeClientAfdrukkenHtml.php?{urlencode(params)}"
    _say("GET", f"{'xls' if as_xls else 'html'} — {url}")
    body = _session.get(url)
    _say("  <-", f"{len(body):,} bytes")
    return body


def scan_html(body: bytes) -> None:
    """Look for anything that explains the codes.

    ⚠ DECODED VIA THE SCANNER'S OWN HELPER. AirSPMS serves ISO-8859-1 and decoding it as utf-8
    silently turns `Azië` into `Azi?` — measured, 3 of 95 rows on the model-portfolio list.
    """
    # ⚠ `_decode_html` takes a RESPONSE (it reads the declared charset off the header), not bytes.
    # AirSPMS serves ISO-8859-1, and decoding as utf-8 turns `Azië` into `Azi?` — so the charset
    # still has to be honoured here; it is just wrapped by hand because this probe holds raw bytes.
    from airs_scanner import AirsHttpResponse, _decode_html  # noqa: PLC0415

    text = _decode_html(AirsHttpResponse(body=body, status=200,
                                         content_type="text/html; charset=ISO-8859-1", url=""))
    plain = re.sub(r"<[^>]+>", " ", text)
    plain = re.sub(r"\s+", " ", plain)

    _say("html", f"{len(text):,} chars")
    # A legend usually sits near the letter itself. Print every window around a standalone `D`
    # that also mentions a word we would expect in a legend.
    hits = 0
    for m in re.finditer(r"\bTt\b|\bTransactietype\b|\bsoort\b|\bLegenda\b|\btoelichting\b",
                         plain, re.I):
        lo, hi = max(0, m.start() - 200), min(len(plain), m.end() + 400)
        print(f"  … {plain[lo:hi].strip()}\n", flush=True)
        hits += 1
        if hits >= 6:
            break
    if not hits:
        _say("html", "no 'Tt' / 'Transactietype' / 'Legenda' anchor found")

    # And any place a lone capital letter is glossed, e.g. "A = Aankoop" or "D  Dividend".
    for m in re.finditer(r"\b([AVD])\b\s*[=:\-–]\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ ]{2,30})", plain):
        print(f"  GLOSS  {m.group(1)} = {m.group(2).strip()}", flush=True)


def scan_xls(body: bytes) -> None:
    from airs_scanner import _strip_spreadsheet_preamble  # noqa: PLC0415

    body = _strip_spreadsheet_preamble(body)
    xl = pd.ExcelFile(BytesIO(body))
    # ⚠ THE PARSER READS SHEET 0 ONLY. A legend tab would be invisible to it and to every consumer
    # downstream — which is exactly the kind of thing this probe exists to find.
    _say("xls sheets", ", ".join(map(repr, xl.sheet_names)))
    for name in xl.sheet_names:
        raw = xl.parse(name, header=None, nrows=12)
        _say(f"sheet {name!r} first rows (header=None, so any preamble shows)")
        with pd.option_context("display.max_columns", None, "display.width", 250):
            print(raw.to_string(max_colwidth=28), flush=True)
        # Anything below the data that mentions a code.
        full = xl.parse(name, header=None)
        tail = full.tail(8).astype(str)
        joined = " ".join(tail.to_numpy().ravel())
        if re.search(r"\b(Aankoop|Verkoop|Dividend|Divers|Splits|Legenda)\b", joined, re.I):
            _say(f"sheet {name!r} FOOTER mentions a code word")
            print(tail.to_string(max_colwidth=40), flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--account", default=DEFAULT_ACCOUNT)
    ap.add_argument("--van", default=DEFAULT_VAN)
    ap.add_argument("--tot", default=DEFAULT_TOT)
    args = ap.parse_args()

    _say("known codes", ", ".join(f"{k} = {v}" for k, v in KNOWN.items()) + ", D = ?")
    _say("account", f"{args.account}  {args.van}..{args.tot}")

    try:
        scan_html(fetch(args.account, args.van, args.tot, as_xls=False))
    except Exception as e:  # noqa: BLE001 — one probe failing must not lose the other
        _say("html probe FAILED", f"{type(e).__name__}: {e}")
    try:
        scan_xls(fetch(args.account, args.van, args.tot, as_xls=True))
    except Exception as e:  # noqa: BLE001
        _say("xls probe FAILED", f"{type(e).__name__}: {e}")

    _say("done", "nothing was written — this probe only reads")
    return 0


if __name__ == "__main__":
    sys.exit(main())
