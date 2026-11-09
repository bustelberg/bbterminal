"""One in-memory LYNQS browser, owned exclusively by one worker thread."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
import hashlib
import time


def _new_browser():
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    browser = None
    try:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(15000)
        return pw, browser, context, page
    except BaseException:
        for resource, method in ((browser, 'close'), (pw, 'stop')):
            if resource is not None:
                try:
                    getattr(resource, method)()
                except Exception:
                    pass
        raise


class PortalSession:
    def __init__(self, factory=None, clock=None, max_age=15 * 60):
        self.factory = factory or _new_browser
        self.clock = clock or time.monotonic
        self.max_age = max_age
        self.resources = None
        self.identity = None
        self.opened_at = 0
        self.portfolio_ids = {}

    def close(self):
        resources, self.resources = self.resources, None
        self.identity = None
        self.portfolio_ids.clear()
        if resources:
            pw, browser, context, page = resources
            for resource, method in ((page, 'close'), (context, 'close'), (browser, 'close'), (pw, 'stop')):
                try:
                    getattr(resource, method)()
                except Exception:
                    pass

    def get(self, user, password):
        # Both credentials participate, so changing a password cannot reuse the old session.
        identity = hashlib.sha256(f'{len(user)}:{user}{password}'.encode()).digest()
        reused = bool(self.resources and self.identity == identity
                      and self.clock() - self.opened_at < self.max_age
                      and self.resources[1].is_connected() and not self.resources[3].is_closed())
        if not reused:
            self.close()
            self.resources = self.factory()
            self.identity = identity
            self.opened_at = self.clock()
        return self.resources[2], self.resources[3], reused


session = PortalSession()
_owner = ThreadPoolExecutor(max_workers=1, thread_name_prefix='lynqs-session')


def on_session_thread(fn, *args):
    # Playwright objects never cross threads; progress/cancellation context does.
    return _owner.submit(copy_context().run, fn, *args).result()
