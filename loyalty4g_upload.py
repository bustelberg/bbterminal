"""Automate creating a "share" (Aandeel) with a logo in the loyalty4g
Sonata admin, driven by Playwright with the logged-in session.

Flow (per company):
  /admin/tops/share/create
    -> fill [share_name] + [share_identifier] (same value)
    -> click the logo "Nieuwe toevoegen" (Sonata media add) -> popup/dialog
    -> set the [binaryContent] file input to the logo PNG (Playwright sets
       it directly; no OS file dialog)
    -> submit

IDs in the form are dynamic Sonata uniqids (s6a26a8ab9e07c...), so we
select by stable `name$="[...]"` suffixes, never the id.

Modes:
  --explore         dump the create-page form + the logo dialog structure
                    (read-only; does NOT submit) so we can confirm selectors
  --name / --logo   create ONE share with that name + logo file

Run from the backend env (has Playwright):
  uv run --project backend python loyalty4g_upload.py --explore
  uv run --project backend python loyalty4g_upload.py --name "2G Energy AG" --logo backend/leonteq_logos/images/86505899.png
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from playwright.sync_api import sync_playwright

BASE_URL = "https://tops.loyalty4g.com"
_DEFAULT_CREDS_FILE = "loyalty4g.creds"

# Set once after the initial login so create_share can transparently
# re-authenticate if the session expires during a multi-hour run.
_CREDS: dict[str, str] = {}


def _load_creds(path: str) -> dict[str, str]:
    creds: dict[str, str] = {}
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    creds[k.strip()] = v.strip().strip('"').strip("'")
    return creds


def _login(page, username: str, password: str) -> None:
    page.goto(f"{BASE_URL}/login", wait_until="domcontentloaded", timeout=30_000)
    page.fill("#_username", username)
    page.fill("#_password", password)
    page.click("button[type=submit], input[type=submit]")
    page.wait_for_load_state("networkidle", timeout=20_000)
    if page.locator("#_username").count() > 0:
        raise RuntimeError("Login failed — still on the login form.")
    print(f"  - logged in, at {page.url}", flush=True)


def _explore(page) -> None:
    page.goto(f"{BASE_URL}/admin/tops/share/create", wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_load_state("networkidle", timeout=15_000)
    print(f"\n=== create page: {page.url} ===", flush=True)

    fields = page.eval_on_selector_all(
        "form input, form select, form textarea",
        "els => els.map(e => ({tag:e.tagName, type:e.type||'', name:e.name||'', id:e.id||''}))",
    )
    print("--- form fields (name / type / id) ---", flush=True)
    for f in fields:
        if f["name"] or f["type"] == "file":
            print(f"  {f['tag']:<8} type={f['type']:<10} name={f['name']!r} id={f['id']!r}", flush=True)

    print("--- logo add link(s) (context=share_logo) ---", flush=True)
    links = page.eval_on_selector_all(
        "a[href*='share_logo'], a[onclick*='share_logo']",
        "els => els.map(e => ({href:e.getAttribute('href'), onclick:e.getAttribute('onclick'), text:e.innerText.trim()}))",
    )
    for ln in links:
        print(f"  text={ln['text']!r}\n    href={ln['href']}\n    onclick={ln['onclick']}", flush=True)

    # Click the logo-ADD link (NOT "Lijst" — both carry context=share_logo;
    # the add one has /create in its href + form_add in its onclick).
    add = page.locator(
        "a[onclick*='form_add'][href*='share_logo'], "
        "a[href*='sonatamediamedia/create'][href*='share_logo']"
    ).first
    if add.count() == 0:
        print("!! no logo-add link found", flush=True)
        return
    print("\n--- clicking the logo 'Nieuwe toevoegen' ---", flush=True)
    add.click()
    # Wait specifically for the file input to materialize inside the modal.
    try:
        page.wait_for_selector("input[type=file]", timeout=10_000)
        print("  file input appeared.", flush=True)
    except Exception:
        print("  file input did NOT appear within 10s.", flush=True)

    dlg = page.locator(".modal, .ui-dialog, [role=dialog]").first
    html = dlg.evaluate("el => el.innerHTML") if dlg.count() else ""
    print(f"  modal HTML length={len(html)}", flush=True)
    for needle in ("binaryContent", 'type="file"', "btn_create_and_edit", "Aanmaken", "provider"):
        print(f"    contains {needle!r}: {needle in html}", flush=True)

    # All file inputs + submit buttons anywhere on the page.
    files = page.eval_on_selector_all(
        "input[type=file]", "els => els.map(e => ({name:e.name, id:e.id}))",
    )
    print(f"  file inputs on page: {files}", flush=True)
    btns = page.eval_on_selector_all(
        ".modal button, .modal input[type=submit]",
        "els => els.map(e => ({type:e.type||'', name:e.name||'', text:(e.innerText||e.value||'').trim()}))",
    )
    print(f"  modal submit buttons: {btns}", flush=True)
    if "binaryContent" in html:
        i = html.index("binaryContent")
        print(f"\n  ...around binaryContent...\n{html[max(0,i-400):i+200]}", flush=True)


_MEDIA_ID_POPULATED = (
    "() => { const el = document.querySelector(\"input[name$='[share_logo_media]']\");"
    " return el && el.value && el.value.trim().length > 0; }"
)


def _attach_logo_once(page, logo_path: str) -> str:
    """One attempt at the Sonata media dialog: open it, set the file
    directly (no OS dialog), submit the media form (scoped to the file
    input's OWN form so a stale modal can't be hit), and wait for the new
    media id to land on the parent [share_logo_media] field. Returns the
    media id, or '' if it didn't populate."""
    page.click(
        "a[onclick*='form_add'][href*='share_logo'], "
        "a[href*='sonatamediamedia/create'][href*='share_logo']"
    )
    try:
        file_input = page.wait_for_selector(
            "input[name$='[binaryContent]']", state="visible", timeout=15_000,
        )
    except Exception:
        return ""
    file_input.set_input_files(os.path.abspath(logo_path))
    page.wait_for_timeout(600)  # let any client-side hook register the file
    # Submit the media form via its own <form>, not a global .modal selector.
    page.eval_on_selector(
        "input[name$='[binaryContent]']",
        "el => { const b = el.form && el.form.querySelector(\"button[name='btn_create']\");"
        " if (b) b.click(); }",
    )
    try:
        page.wait_for_function(_MEDIA_ID_POPULATED, timeout=30_000)
    except Exception:
        pass
    return page.eval_on_selector("input[name$='[share_logo_media]']", "el => el.value") or ""


def create_share(
    page, name: str, logo_path: str, *, do_submit: bool = True, attempts: int = 3,
) -> bool:
    """Create one share: name + identifier + logo. Returns True on success.

    Each attempt reloads the create page fresh (clean state), re-fills the
    fields, and re-runs the media dialog — so a transient logo-upload race
    is retried instead of aborting. Only submits the share once the logo
    media id has actually populated (never creates a logo-less share)."""
    if not os.path.isfile(logo_path):
        raise FileNotFoundError(logo_path)

    media_val = ""
    for attempt in range(1, attempts + 1):
        page.goto(f"{BASE_URL}/admin/tops/share/create", wait_until="domcontentloaded", timeout=30_000)
        # Session expired over a long run? Symfony bounces to /login — re-auth.
        if page.locator("#_username").count() > 0 and _CREDS.get("u"):
            print("  - session expired; re-logging in", flush=True)
            _login(page, _CREDS["u"], _CREDS["p"])
            page.goto(f"{BASE_URL}/admin/tops/share/create", wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_selector("input[name$='[share_name]']", timeout=15_000)
        page.fill("input[name$='[share_name]']", name)
        page.fill("input[name$='[share_identifier]']", name)
        if attempt == 1:
            print(f"  - filled name/identifier, uploading logo", flush=True)
        media_val = _attach_logo_once(page, logo_path)
        if media_val:
            break
        print(f"  - logo didn't attach (attempt {attempt}/{attempts}); retrying", flush=True)

    print(f"  - share_logo_media id = {media_val!r}", flush=True)
    if not media_val:
        print("  !! logo never attached — aborting BEFORE creating a broken share.", flush=True)
        return False

    if not do_submit:
        print("  (dry-run: not submitting the share form)", flush=True)
        return True

    # Main submit (Sonata's primary create button is btn_create_and_edit).
    page.locator("button[name='btn_create_and_edit']").first.click()
    page.wait_for_load_state("networkidle", timeout=20_000)
    ok = "/create" not in page.url
    flash = ""
    if page.locator(".alert-success, .alert.alert-success").count():
        flash = page.locator(".alert-success, .alert.alert-success").first.inner_text().strip()
    print(f"  - landed on {page.url} | success={ok} flash={flash!r}", flush=True)
    return ok


def _fetch_existing_share_names(page) -> set[str]:
    """Scrape every existing share name from the admin list (all pages) so
    we never create a duplicate. The 'Naam' column is the 3rd <td>."""
    names: set[str] = set()
    pnum = 1
    while pnum <= 200:  # hard cap so a pagination quirk can't loop forever
        page.goto(
            f"{BASE_URL}/admin/tops/share/list?page={pnum}",
            wait_until="networkidle", timeout=30_000,
        )
        page_names = page.eval_on_selector_all(
            "table tbody tr",
            "els => els.map(tr => { const t = tr.querySelectorAll('td');"
            " return t.length >= 3 ? t[2].textContent.trim() : ''; }).filter(Boolean)",
        )
        if not page_names:
            break
        before = len(names)
        names.update(page_names)
        if len(names) == before:  # page added nothing new -> wrapped / last page
            break
        pnum += 1
    return names


def _load_created(path: str) -> set[str]:
    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as fh:
                return set(json.load(fh))
        except Exception:
            return set()
    return set()


def _record_created(path: str, names: set[str]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(sorted(names), fh, indent=2, ensure_ascii=False)


def run_loop(
    page, *, manifest_path: str, images_dir: str, limit: int,
    real_logos_only: bool, check_server: bool, created_file: str,
) -> int:
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)

    # Skip-set = locally-recorded creations UNION whatever already exists on
    # the server (authoritative dedup, survives a lost local log).
    done_names = _load_created(created_file)
    if check_server:
        print("  - scraping existing shares from the server for dedup…", flush=True)
        existing = _fetch_existing_share_names(page)
        print(f"    found {len(existing)} existing share(s) on the server", flush=True)
        done_names |= existing

    created_this_run = 0
    skipped_dup = skipped_nologo = skipped_letter = failed = 0
    for entry in manifest:
        if created_this_run >= limit:
            break
        name = (entry.get("name") or "").strip()
        logo_file = entry.get("logo_file")
        if not name or not logo_file:
            skipped_nologo += 1
            continue
        if real_logos_only and entry.get("status") == "letter":
            skipped_letter += 1
            continue
        if name in done_names:
            skipped_dup += 1
            continue
        logo_path = os.path.join(images_dir, logo_file)
        if not os.path.isfile(logo_path):
            print(f"  ! [{name}] logo file missing: {logo_path}", flush=True)
            skipped_nologo += 1
            continue

        print(f"\n[{created_this_run + 1}/{limit}] creating {name!r}", flush=True)
        try:
            ok = create_share(page, name, logo_path, do_submit=True)
        except Exception as e:  # one bad company never stops the loop
            ok = False
            print(f"  !! exception: {type(e).__name__}: {e}", flush=True)
        if ok:
            created_this_run += 1
            done_names.add(name)
            _record_created(created_file, done_names)  # persist after EACH success → resumable
        else:
            failed += 1

    print(
        f"\n=== loop done: created={created_this_run} failed={failed} "
        f"skipped(dup={skipped_dup}, no_logo={skipped_nologo}, letter={skipped_letter}) ===",
        flush=True,
    )
    return 0 if failed == 0 else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--creds-file", default=_DEFAULT_CREDS_FILE)
    ap.add_argument("--explore", action="store_true")
    ap.add_argument("--name")
    ap.add_argument("--logo")
    ap.add_argument("--dry-run", action="store_true", help="Upload logo but DON'T submit the share form.")
    ap.add_argument("--headed", action="store_true", help="Show the browser window.")
    ap.add_argument("--slow-mo", type=int, default=0,
                    help="Delay (ms) between Playwright actions; 0 = full speed (default).")
    # Loop mode + safeguards.
    ap.add_argument("--loop", action="store_true", help="Create shares from the manifest in a loop.")
    ap.add_argument("--limit", type=int, default=2, help="Max NEW shares to create this run (default 2).")
    ap.add_argument("--manifest", default="backend/leonteq_logos/companies.json")
    ap.add_argument("--images-dir", default="backend/leonteq_logos/images")
    ap.add_argument("--created-file", default="loyalty4g_created.json",
                    help="Local log of names this script created (for resume/dedup).")
    ap.add_argument("--real-logos-only", action="store_true",
                    help="Skip the generated first-letter badge logos (status=letter).")
    ap.add_argument("--no-check-server", action="store_true",
                    help="Skip the startup scrape of existing shares (faster, less safe).")
    args = ap.parse_args()

    creds = _load_creds(args.creds_file)
    username = os.environ.get("LOYALTY4G_USERNAME") or creds.get("LOYALTY4G_USERNAME")
    password = os.environ.get("LOYALTY4G_PASSWORD") or creds.get("LOYALTY4G_PASSWORD")
    if not (username and password):
        print("Missing credentials (creds file / env).", file=sys.stderr)
        return 2

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.headed, slow_mo=args.slow_mo)
        page = browser.new_page()
        try:
            _login(page, username, password)
            _CREDS["u"], _CREDS["p"] = username, password  # for transparent re-login
            if args.explore:
                _explore(page)
            elif args.loop:
                return run_loop(
                    page,
                    manifest_path=args.manifest,
                    images_dir=args.images_dir,
                    limit=args.limit,
                    real_logos_only=args.real_logos_only,
                    check_server=not args.no_check_server,
                    created_file=args.created_file,
                )
            elif args.name and args.logo:
                ok = create_share(page, args.name, args.logo, do_submit=not args.dry_run)
                print(f"\n{'CREATED' if ok else 'FAILED'}: {args.name}", flush=True)
                if args.headed:
                    page.wait_for_timeout(8000)  # let you see the result
                return 0 if ok else 1
            else:
                print("Pass --explore, --loop, or --name and --logo.", flush=True)
        finally:
            browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
