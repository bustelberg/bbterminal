"""The RISK VARIANT of a model portfolio: Offensief / Beperkt Offensief / Neutraal / Defensief.

A product line (Bustelberg, Pensioen, Toppenberg Beheer, Vermogensopbouw, and each of the
TopSelectie families) is offered at several risk profiles, and AIRS encodes the profile in the
portfolio's name. Filtering the correlation matrix to ONE profile is what makes its rows
comparable: "how correlated are my Neutraal products with each other" is a question about the
line, not about the risk level, and it can only be asked once the risk level is held constant.

⚠ THE ORDER OF THE RULES IS THE WHOLE THING. "bep offensief" CONTAINS "offensief".

    BUS_Bep_offensief_FX   normalises to  "bus bep offensief fx"

    ...whose tokens include a standalone `offensief`. Test Offensief before Beperkt Offensief and
    that portfolio is classified Offensief — it lands in the wrong filter, correlates against the
    wrong peers, and nothing anywhere says so.

    ⚠⚠ AND IT IS THE KIND OF BUG A REASONABLE TEST MISSES. Measured 2026-07-16 across the five
    Beperkt-Offensief models, the wrong order misclassifies exactly ONE:

        BUS_Bep_offensief_FX   -> Offensief          *** wrong ***
        BUS_DUTS_BEPOF_AFS     -> Beperkt Offensief  (survives: "bepof" has no standalone "off")
        BUS_FTS_Bepoff_AFS     -> Beperkt Offensief  (survives)
        BUS_MTS_BEPOFF_AFS     -> Beperkt Offensief  (survives)
        TOPS_BEOFF_BEH         -> Beperkt Offensief  (survives)

    Six of seven cases pass with the rules in the wrong order. Check any of those five and you
    would conclude the ordering does not matter. Same shape as EBIT-vs-Operating-Income, where
    Apple's two figures are identical and Mitsui's are not.

⚠ THE NAME IS READ BEFORE THE DESCRIPTION, AND THAT IS NOT A STYLE CHOICE.
    The description spells the profile out in Dutch and looks like the friendlier source — but it
    is prose, and prose has typos. `TOPS_OFF_BEH`'s description reads "Toppenberg beheer
    **offenisef**". Description-first loses that portfolio to `None`; the name's `OFF` token
    catches it. AIRS's code is ugly precisely because it is a code.

    The description is still worth reading second: it is what rescues a name with no profile token
    at all, if one ever appears.

⚠ `None` IS AN ANSWER, NOT A FAILURE. 8 of the 42 have no risk profile because they ARE not
    offered at one — the themed TopSelectie funds (Azië, Momentum, Alternatives), the WTS
    thematics (Dividend, Duurzaam, Familie), and Risicodragend/Risicomijdend, which are a
    DIFFERENT axis entirely (risk-bearing vs risk-avoiding is not one of the four profiles, and
    mapping it onto them would invent a variant the product does not have). They appear under
    "All" and under no profile filter, which is the truth about them.
"""
from __future__ import annotations

import re

# The four profiles, in the order the UI offers them: most to least offensive.
VARIANTS: tuple[str, ...] = ("Offensief", "Beperkt Offensief", "Neutraal", "Defensief")

_SEP = re.compile(r"[^a-z0-9]+")

# ⚠ ORDERED. Beperkt Offensief MUST be tested before Offensief — see the module docstring.
_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    # Every spelling AIRS actually uses, measured: `Bep_offensief` (two tokens), `BEPOF`,
    # `Bepoff`, `BEPOFF` and — alone among them — `BEOFF` (TOPS_BEOFF_BEH, no `p`).
    ("Beperkt Offensief", re.compile(r"\b(bep\w*\s*off\w*|bepof+|beoff)\b")),
    # `offenisef` is not a typo in THIS file: it is the typo in AIRS's own description of
    # TOPS_OFF_BEH, and it is here so the description fallback can still read it.
    ("Offensief", re.compile(r"\b(off|offensief|offenisef)\b")),
    ("Neutraal", re.compile(r"\b(neu|neutraal)\b")),
    ("Defensief", re.compile(r"\b(def|defensief)\b")),
)


def _norm(s: str | None) -> str:
    """Lowercase, and every separator AIRS uses (`_`, ` `, `.`) collapsed to one space — so
    `BUS_2.0_NEU_FX` and `AITopSelectie OFF FX` tokenise the same way."""
    return _SEP.sub(" ", (s or "").lower()).strip()


def portfolio_variant(name: str | None, omschrijving: str | None = None) -> str | None:
    """The risk profile this model is offered at, or None if it is not offered at one.

    Reads the NAME first (a code survives a typo; prose does not), then the description.
    """
    for source in (name, omschrijving):
        s = _norm(source)
        if not s:
            continue
        for label, rx in _RULES:
            if rx.search(s):
                return label
    return None
