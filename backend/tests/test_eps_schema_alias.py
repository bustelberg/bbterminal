def test_eps_without_nri_accepts_the_per_share_data_array_schema():
    from routers.earnings import _METRIC_CODES

    assert "annuals__per_share_data_array__EPS without NRI" in _METRIC_CODES["eps_nri"]
