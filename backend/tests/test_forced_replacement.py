from momentum.forced_replacement import next_same_sector_reserve


def test_uses_next_available_reserve_in_the_same_sector_only():
    got = next_same_sector_reserve(
        {"company_id": 1, "sector": "Technology"},
        [
            {"company_id": 2, "sector": "Technology", "reserve_rank": 1},
            {"company_id": 3, "sector": "Technology", "reserve_rank": 2},
            {"company_id": 4, "sector": "Health Care", "reserve_rank": 1},
        ],
        {1, 2}, set(),
    )
    assert got == {"company_id": 3, "sector": "Technology", "reserve_rank": 2}


def test_leaves_the_sleeve_unfilled_when_no_same_sector_reserve_is_tradable():
    assert next_same_sector_reserve(
        {"company_id": 1, "sector": "Technology"},
        [{"company_id": 2, "sector": "Health Care", "reserve_rank": 1}],
        {1}, set(),
    ) is None
