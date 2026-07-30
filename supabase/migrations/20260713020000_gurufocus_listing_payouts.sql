-- Two fixes, both about a cell being EMPTY for a reason the UI could not name.
--
-- 1. EVICT THE ROWS POISONED BY THE MISSING `ARCA` CODE.
--    GuruFocus's `exchange_list` puts nine codes in region USA:
--        NAS  NYSE  OTCPK  OTCBB  AMEX  ARCA  IEXG  BATS  GREY
--    We knew none of ARCA / IEXG / BATS, and ARCA (NYSE Arca) is where most US ETFs
--    actually list — of SPY, IWM, VOO, XLU, EDV, GLD and QQQ, SIX are ARCA and only
--    QQQ is NAS. So `is_gf_subscribed_exchange('ARCA')` was False and every one of
--    them cached as status='unsubscribed' (iShares Russell 2000, US4642876555, is the
--    one that surfaced it — GuruFocus has 106 dividend payments for it).
--
--    It was not merely lost coverage. SPY and GLD ALSO list on SGX, which we DO
--    subscribe to, so an unknown ARCA would have silently resolved SPY to its
--    SINGAPORE line. A negative cache is only as good as the predicate behind it, and
--    that predicate was wrong — so drop the verdicts it produced and let them
--    re-resolve. (`ok` rows were resolved against a predicate that was right for them.)
DELETE FROM public.gurufocus_listing WHERE status <> 'ok';

-- 2. "NO PAYOUTS" IS A REASON, NOT A BLANK.
--    A resolved listing whose dividend feed comes back EMPTY is not a failure and not
--    a gap — it is an answer: an ACCUMULATING fund distributes nothing (iShares Core
--    MSCI World, IE00B4L5Y983, is exactly this). Until we've fetched once we don't
--    know, so this is deliberately THREE-valued:
--        NULL   never fetched      -> the cell offers "Fetch"
--        true   has payments       -> "View"
--        false  fetched, none      -> "NO PAYOUTS" badge, and we don't re-ask
--    A two-valued boolean would collapse "we haven't looked" into "there is nothing",
--    which is the same lie the bare "—" was telling before.
ALTER TABLE public.gurufocus_listing
    ADD COLUMN IF NOT EXISTS has_payments boolean;

NOTIFY pgrst, 'reload schema';
