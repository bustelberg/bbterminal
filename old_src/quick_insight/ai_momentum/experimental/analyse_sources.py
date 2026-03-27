# src/quick_insight/ai_momentum/experimental/analyse_sources.py
from __future__ import annotations

from quick_insight.ai_momentum.experimental.utils import (
    plot_gurufocus_indicator_timeseries,
)


if __name__ == "__main__":
    plot_gurufocus_indicator_timeseries(
        primary_ticker="MSFT", # MSFT ASML RMS
        primary_exchange="NASDAQ", # NASDAQ XAMS XPAR
        key="insider_net_volume", # guru_buy guru_sell guru_buy_volume guru_sell_volume  insider_buy
        cumulative=False,
        use_cache=True,
    )



#   {'key': 'price', 'name': 'Current Price'}
#   {'key': 'revenue', 'name': 'Revenue'}
#   {'key': 'revenue_estimate', 'name': 'Revenue with Estimate'}
#   {'key': 'net_income', 'name': 'Net Income'}
#   {'key': 'pettm', 'name': 'PE Ratio (TTM)'}
#   {'key': 'forward_pe_ratio', 'name': 'Forward PE Ratio'}
#   {'key': 'price_to_owner_earnings', 'name': 'Price-to-Owner-Earnings'}
#   {'key': 'gf_value', 'name': 'GF Value'}
#   {'key': 'price_to_gf_value', 'name': 'Price-to-GF-Value'}
#   {'key': 'ps_ratio', 'name': 'PS Ratio'}
#   {'key': 'pb_ratio', 'name': 'PB Ratio'}
#   {'key': 'peg_ratio', 'name': 'PEG Ratio'}
#   {'key': 'yield', 'name': 'Dividend Yield %'}
#   {'key': 'mktcap', 'name': 'Market Cap'}
#   {'key': 'enterprise_value', 'name': 'Enterprise Value'}
#   {'key': 'shares_outstanding', 'name': 'Shares Outstanding (Diluted Average)'}
#   {'key': 'earning_per_share_diluted', 'name': 'Earnings per Share (Diluted)'}
#   {'key': 'per_share_eps_estimate', 'name': 'EPS with Estimate'}
#   {'key': 'eps_without_nri', 'name': 'EPS without NRI'}
#   {'key': 'eps_nri_estimate', 'name': 'EPS without NRI with Estimate'}
#   {'key': 'owner_earnings', 'name': 'Owner Earnings per Share (TTM)'}
#   {'key': 'maxpe', 'name': 'Price at Max PE without NRI'}
#   {'key': 'medpe', 'name': 'Price at Med PE without NRI'}
#   {'key': 'minpe', 'name': 'Price at Min PE without NRI'}
#   {'key': 'custmpe', 'name': 'Price at PE(without NRI)='}
#   {'key': 'custmpeestimate', 'name': 'Price at PE(without NRI with Estimate)='}
#   {'key': 'maxps', 'name': 'Price at Max PS Ratio'}
#   {'key': 'medps', 'name': 'Price at Med PS Ratio'}
#   {'key': 'minps', 'name': 'Price at Min PS Ratio'}
#   {'key': 'custmps', 'name': 'Price at PS Ratio='}
#   {'key': 'maxpb', 'name': 'Price at Max PB Ratio'}
#   {'key': 'medpb', 'name': 'Price at Med PB Ratio'}
#   {'key': 'custmpb', 'name': 'Price at PB Ratio='}
#   {'key': 'minpb', 'name': 'Price at Min PB Ratio'}
#   {'key': 'maxpocf', 'name': 'Price at Max Price-to-Operating-Cash-Flow'}
#   {'key': 'medpocf', 'name': 'Price at Med Price-to-Operating-Cash-Flow'}
#   {'key': 'minpocf', 'name': 'Price at Min Price-to-Operating-Cash-Flow'}
#   {'key': 'custmpocf', 'name': 'Price at Price-to-Operating-Cash-Flow='}
#   {'key': 'insti_owner', 'name': 'Institutional Ownership'}
#   {'key': 'ins_owner', 'name': 'Insider Ownership'}
#   {'key': 'growth_per_share_eps', 'name': 'YoY EPS Growth'}
#   {'key': 'growth_per_share_ebitda', 'name': 'YoY EBITDA Growth (%)'}
#   {'key': 'growth_revenue_per_share', 'name': 'YoY Rev. per Sh. Growth'}
#   {'key': 'ebit_estimate', 'name': 'EBIT with Estimate'}
#   {'key': 'ebitda_estimate', 'name': 'EBITDA with Estimate'}
#   {'key': 'max_enterprise_value_to_ebitda', 'name': 'Price at Max EV-to-EBITDA'}
#   {'key': 'med_enterprise_value_to_ebitda', 'name': 'Price at Med EV-to-EBITDA'}
#   {'key': 'min_enterprise_value_to_ebitda', 'name': 'Price at Min EV-to-EBITDA'}
#   {'key': 'custmenterprise_value_to_ebitda', 'name': 'Price at EV-to-EBITDA='}
#   {'key': 'max_enterprise_value_to_ebit', 'name': 'Price at Max EV-to-EBIT'}
#   {'key': 'med_enterprise_value_to_ebit', 'name': 'Price at Med EV-to-EBIT'}
#   {'key': 'min_enterprise_value_to_ebit', 'name': 'Price at Min EV-to-EBIT'}
#   {'key': 'custmenterprise_value_to_ebit', 'name': 'Price at EV-to-EBIT='}
#   {'key': 'maxdiv', 'name': 'Price at Max Dividend Yield%'}
#   {'key': 'meddiv', 'name': 'Price at Med Dividend Yield%'}
#   {'key': 'mindiv', 'name': 'Price at Min Dividend Yield%'}
#   {'key': 'custmdiv', 'name': 'Price at Dividend Yield%='}
#   {'key': 'ffo_per_share', 'name': 'FFO per Share'}
#   {'key': 'volume', 'name': 'Volume'}
#   {'key': 'medpe_estimate', 'name': 'Price at Med PE without NRI with Estimate'}
#   {'key': 'medps_estimate', 'name': 'Price at Med PS Ratio with Estimate'}
#   {'key': 'medpb_estimate', 'name': 'Price at Med PB Ratio with Estimate'}
#   {'key': 'medpocf_estimate', 'name': 'Price at Med Price-to-Operating-Cash-Flow with Estimate'}
#   {'key': 'volatility_1m', 'name': '1-Month Volatility %'}
#   {'key': 'volatility', 'name': '1-Year Volatility %'}
#   {'key': 'gf_score', 'name': 'GF Score'}
#   {'key': 'rank_balancesheet', 'name': 'Financial Strength'}
#   {'key': 'rank_profitability', 'name': 'Profitability Rank'}
#   {'key': 'rank_gf_value', 'name': 'GF Value Rank'}
#   {'key': 'rank_growth', 'name': 'Growth Rank'}
#   {'key': 'rank_momentum', 'name': 'Momentum Rank'}
#   {'key': 'rank_predictability', 'name': 'Predictability Rank'}
#   {'key': 'price_to_free_cash_flow', 'name': 'Price-to-Free-Cash-Flow'}
#   {'key': 'price_to_operating_cash_flow', 'name': 'Price-to-Operating-Cash-Flow'}
#   {'key': 'enterprise_value_to_revenue', 'name': 'EV-to-Revenue'}
#   {'key': 'enterprise_value_to_ebitda', 'name': 'EV-to-EBITDA'}
#   {'key': 'enterprise_value_to_ebit', 'name': 'EV-to-EBIT'}
#   {'key': 'enterprise_value_to_ocf', 'name': 'EV-to-OCF'}
#   {'key': 'enterprise_value_to_fcf', 'name': 'EV-to-FCF'}
#   {'key': 'earning_yield_greenblatt', 'name': 'Earnings Yield (Joel Greenblatt) %'}
#   {'key': 'rate_of_return_value', 'name': 'Forward Rate of Return (Yacktman) %'}
#   {'key': 'shiller_pe_ratio', 'name': 'Shiller PE Ratio'}
#   {'key': 'cyclically_adjusted_pb_ratio', 'name': 'Cyclically Adjusted PB Ratio'}
#   {'key': 'cyclically_adjusted_ps_ratio', 'name': 'Cyclically Adjusted PS Ratio'}
#   {'key': 'cyclically_adjusted_price_to_fcf', 'name': 'Cyclically Adjusted Price-to-FCF'}
#   {'key': 'price_to_tangible_book', 'name': 'Price-to-Tangible-Book'}
#   {'key': 'revenue_per_share', 'name': 'Revenue per Share'}
#   {'key': 'free_cash_flow_per_share', 'name': 'Free Cash Flow per Share'}
#   {'key': 'cash_flow_from_operations_per_share', 'name': 'Operating Cash Flow per Share'}
#   {'key': 'dividends_per_share', 'name': 'Dividends per Share'}
#   {'key': 'ebitda_per_share', 'name': 'EBITDA per Share'}
#   {'key': 'ebit_per_share', 'name': 'EBIT per Share'}
#   {'key': 'book_value_per_share', 'name': 'Book Value per Share'}
#   {'key': 'tangibles_book_per_share', 'name': 'Tangible Book per Share'}
#   {'key': 'intrinsic_value_dcf_fcf_based', 'name': 'Intrinsic Value: DCF (FCF Based)'}
#   {'key': 'medpsvalue', 'name': 'Median PS Value'}
#   {'key': 'peter_lynch_fair_value', 'name': 'Peter Lynch Fair Value'}
#   {'key': 'epv', 'name': 'Earnings Power Value (EPV)'}
#   {'key': 'graham_number', 'name': 'Graham Number'}
#   {'key': 'net_current_asset_value', 'name': 'Net Current Asset Value'}
#   {'key': 'net_net_working_capital', 'name': 'Net-Net Working Capital'}
#   {'key': 'enterprise_value_per_share', 'name': 'Enterprise Value per Share'}
#   {'key': 'zscore', 'name': 'Altman Z-Score'}
#   {'key': 'fscore', 'name': 'Piotroski F-Score'}
#   {'key': 'mscore', 'name': 'Beneish M-Score'}
#   {'key': 'roe', 'name': 'ROE %'}
#   {'key': 'roce', 'name': 'ROCE %'}
#   {'key': 'roa', 'name': 'ROA %'}
#   {'key': 'roc_joel', 'name': 'ROC (Joel Greenblatt) %'}
#   {'key': 'rore_5y', 'name': '5-Year RORE %'}
#   {'key': 'roiic_1y', 'name': '1-Year ROIIC %'}
#   {'key': 'roiic_3y', 'name': '3-Year ROIIC %'}
#   {'key': 'roiic_5y', 'name': '5-Year ROIIC %'}
#   {'key': 'roiic_10y', 'name': '10-Year ROIIC %'}
#   {'key': 'roic', 'name': 'ROIC %'}
#   {'key': 'wacc', 'name': 'WACC %'}
#   {'key': 'payout', 'name': 'Dividend Payout Ratio'}
#   {'key': 'gross_margin', 'name': 'Gross Margin %'}
#   {'key': 'operating_margin', 'name': 'Operating Margin %'}
#   {'key': 'pretax_margin', 'name': 'Pretax Margin %'}
#   {'key': 'net_margin', 'name': 'Net Margin %'}
#   {'key': 'net_interest_margin', 'name': 'Net Interest Margin (Bank Only) %'}
#   {'key': 'days_sales_outstanding', 'name': 'Days Sales Outstanding'}
#   {'key': 'days_inventory', 'name': 'Days Inventory'}
#   {'key': 'days_payable', 'name': 'Days Payable'}
#   {'key': 'defensive_interval_ratio', 'name': 'Defensive Interval Ratio'}
#   {'key': 'receivables_turnover', 'name': 'Receivables Turnover'}
#   {'key': 'inventory_turnover', 'name': 'Inventory Turnover'}
#   {'key': 'cost_of_goods_sold_to_revenue', 'name': 'COGS-to-Revenue'}
#   {'key': 'rd2rev', 'name': 'RD-to-Revenue'}
#   {'key': 'inventory_to_revenue', 'name': 'Inventory-to-Revenue'}
#   {'key': 'interest_expense_to_revenue', 'name': 'Interest-Expense-to-Revenue'}
#   {'key': 'account_receivable_to_asset', 'name': 'Account-Receivable-to-Asset'}
#   {'key': 'debt_to_equity', 'name': 'Debt-to-Equity'}
#   {'key': 'property_plant_and_equipment_to_asset', 'name': 'Property-Plant-and-Equipment-to-Asset'}
#   {'key': 'equity_to_asset', 'name': 'Equity-to-Asset'}
#   {'key': 'long_term_debt_and_capital_lease_obligation_to_asset', 'name': 'Long-Term-Debt-and-Capital-Lease-Obligation-to-Asset'}
#   {'key': 'turnover', 'name': 'Asset Turnover'}
#   {'key': 'interest_coverage', 'name': 'Interest Coverage'}
#   {'key': 'intrinsic_value_projected_fcf', 'name': 'Intrinsic Value: Projected FCF'}
#   {'key': 'intrinsic_value_dcf_earnings_based', 'name': 'Intrinsic Value: DCF (Earnings Based)'}
#   {'key': 'total_debt_per_share', 'name': 'Total Debt per Share'}
#   {'key': 'cash_per_share', 'name': 'Cash per Share'}
#   {'key': 'price_to_epv', 'name': 'Price-to-Earnings-Power-Value'}
#   {'key': 'cash_conversion_cycle', 'name': 'Cash Conversion Cycle'}
#   {'key': 'current_ratio', 'name': 'Current Ratio'}
#   {'key': 'quick_ratio', 'name': 'Quick Ratio'}
#   {'key': 'cash_ratio', 'name': 'Cash Ratio'}
#   {'key': 'pe_ntm', 'name': 'PE Ratio (Next Twelve Months)'}
#   {'key': 'ps_ntm', 'name': 'PS Ratio (Next Twelve Months)'}
#   {'key': 'enterprise_value_to_ebitda_ntm', 'name': 'EV-to-EBITDA (Next Twelve Months)'}
#   {'key': 'price_to_ffo', 'name': 'Price-to-FFO'}
#   {'key': 'price_to_operating_income', 'name': 'Price-to-Operating-Income'}
#   {'key': 'ebitda_margin', 'name': 'EBITDA Margin %'}
#   {'key': 'fcf_yield', 'name': 'FCF Yield %'}
#   {'key': 'ocf_yield', 'name': 'OCF Yield %'}
#   {'key': 'ebit_margin', 'name': 'EBIT Margin %'}
#   {'key': 'penri', 'name': 'PE Ratio without NRI'}
#   {'key': 'earning_growth_5y_est', 'name': 'Future 3-5Y EPS without NRI Growth Rate Estimate'}
#   {'key': 'cost_of_goods_sold', 'name': 'Cost of Goods Sold'}
#   {'key': 'gross_profit', 'name': 'Gross Profit'}
#   {'key': 'selling_general_admin_expense', 'name': 'Selling, General, & Admin. Expense'}
#   {'key': 'selling_market_expense', 'name': 'Selling and Marketing Expense'}
#   {'key': 'general_admin_expense', 'name': 'General and Admin. Expense'}
#   {'key': 'research_development', 'name': 'Research & Development'}
#   {'key': 'ebitda', 'name': 'EBITDA'}
#   {'key': 'operating_income', 'name': 'Operating Income'}
#   {'key': 'pretax_income', 'name': 'Pretax Income'}
#   {'key': 'interest_income', 'name': 'Interest Income'}
#   {'key': 'interest_expense', 'name': 'Interest Expense'}
#   {'key': 'depreciation_depletion_amortization', 'name': 'Depreciation, Depletion and Amortization'}
#   {'key': 'tax_rate', 'name': 'Tax Rate %'}
#   {'key': 'total_operating_expense', 'name': 'Total Operating Expense'}
#   {'key': 'tax_provision', 'name': 'Tax Provision'}
#   {'key': 'other_net_income_loss', 'name': 'Other Net Income (Loss)'}
#   {'key': 'net_income_including_noncontrolling_interests', 'name': 'Net Income Including Noncontrolling Interests'}
#   {'key': 'ebit', 'name': 'EBIT'}
#   {'key': 'net_income_continuing_operations', 'name': 'Net Income (Continuing Operations)'}
#   {'key': 'net_income_discontinued_operations', 'name': 'Net Income (Discontinued Operations)'}
#   {'key': 'other_income_minority_interest', 'name': 'Other Income (Minority Interest)'}
#   {'key': 'eps_basic', 'name': 'EPS (Basic)'}
#   {'key': 'eps_diluated', 'name': 'EPS (Diluted)'}
#   {'key': 'cash_and_cash_equivalents', 'name': 'Cash and Cash Equivalents'}
#   {'key': 'marke_table_securities', 'name': 'Marketable Securities'}
#   {'key': 'accounts_receivable', 'name': 'Accounts Receivable'}
#   {'key': 'raw_materials', 'name': 'Inventories, Raw Materials & Components'}
#   {'key': 'work_in_process', 'name': 'Inventories, Work In Process'}
#   {'key': 'inventories_adjustments_allowances', 'name': 'Inventories, Inventories Adjustments'}
#   {'key': 'finished_goods', 'name': 'Inventories, Finished Goods'}
#   {'key': 'other_inventories', 'name': 'Inventories, Other'}
#   {'key': 'inventory', 'name': 'Total Inventories'}
#   {'key': 'other_current_assets', 'name': 'Other Current Assets'}
#   {'key': 'total_current_assets', 'name': 'Total Current Assets'}
#   {'key': 'land_and_improvements', 'name': 'Land And Improvements'}
#   {'key': 'buildings_and_improvements', 'name': 'Buildings And Improvements'}
#   {'key': 'machinery_furniture_equipment', 'name': 'Machinery, Furniture, Equipment'}
#   {'key': 'construction_in_progress', 'name': 'Construction In Progress'}
#   {'key': 'gross_ppe', 'name': 'Gross Property, Plant and Equipment'}
#   {'key': 'accumulated_depreciation', 'name': 'Accumulated Depreciation'}
#   {'key': 'net_ppe', 'name': 'Property, Plant and Equipment'}
#   {'key': 'intangibles', 'name': 'Intangible Assets'}
#   {'key': 'good_will', 'name': 'Goodwill'}
#   {'key': 'other_long_term_assets', 'name': 'Other Long Term Assets'}
#   {'key': 'total_assets', 'name': 'Total Assets'}
#   {'key': 'accounts_payable', 'name': 'Accounts Payable'}
#   {'key': 'total_tax_payable', 'name': 'Total Tax Payable'}
#   {'key': 'accounts_payable_accrued_expense', 'name': 'Accounts Payable & Accrued Expense'}
#   {'key': 'short_term_debt_and_capital_lease_obligation', 'name': 'Short-Term Debt & Capital Lease Obligation'}
#   {'key': 'bs_current_deferred_liabilities', 'name': 'Deferred Tax And Revenue'}
#   {'key': 'other_current_liabilities', 'name': 'Other Current Liabilities'}
#   {'key': 'total_current_liabilities', 'name': 'Total Current Liabilities'}
#   {'key': 'long_term_capital_lease_obligation', 'name': 'Long-Term Capital Lease Obligation'}
#   {'key': 'pension_and_retirement_benefit', 'name': 'Pension And Retirement Benefit'}
#   {'key': 'non_current_deferred_liabilities', 'name': 'NonCurrent Deferred Liabilities'}
#   {'key': 'non_current_deferred_income_tax', 'name': 'NonCurrent Deferred Income Tax'}
#   {'key': 'other_long_term_liabilities', 'name': 'Other Long-Term Liabilities'}
#   {'key': 'long_term_debt_and_capital_lease_obligation', 'name': 'Long-Term Debt & Capital Lease Obligation'}
#   {'key': 'total_liabilities', 'name': 'Total Liabilities'}
#   {'key': 'common_stock', 'name': 'Common Stock'}
#   {'key': 'preferred_stock', 'name': 'Preferred Stock'}
#   {'key': 'retained_earnings', 'name': 'Retained Earnings'}
#   {'key': 'accumulated_other_comprehensive_income', 'name': 'Accumulated other comprehensive income (loss)'}
#   {'key': 'additional_paid_in_capital', 'name': 'Additional Paid-In Capital'}
#   {'key': 'treasury_stock', 'name': 'Treasury Stock'}
#   {'key': 'total_stockholders_equity', 'name': 'Total Stockholders Equity'}
#   {'key': 'long_term_debt', 'name': 'Long-Term Debt'}
#   {'key': 'short_term_capital_lease_obligation', 'name': 'Short-Term Capital Lease Obligation'}
#   {'key': 'short_term_debt', 'name': 'Short-Term Debt'}
#   {'key': 'non_current_deferred_revenue', 'name': 'NonCurrent Deferred Revenue'}
#   {'key': 'net_income_from_continuing_operations', 'name': 'Net Income From Continuing Operations'}
#   {'key': 'cash_flow_depreciation_depletion_amortization', 'name': 'Cash Flow Depreciation, Depletion and Amortization'}
#   {'key': 'change_in_working_capital', 'name': 'Change In Working Capital'}
#   {'key': 'cash_flow_deferred_tax', 'name': 'Deferred Tax'}
#   {'key': 'stock_based_compensation', 'name': 'Stock Based Compensation'}
#   {'key': 'asset_impairment_charge', 'name': 'Asset Impairment Charge'}
#   {'key': 'cash_from_discontinued_operating_activities', 'name': 'Cash from Discontinued Operating Activities'}
#   {'key': 'cash_flow_from_others', 'name': 'Cash Flow from Others'}
#   {'key': 'cash_flow_from_operations', 'name': 'Cash Flow from Operations'}
#   {'key': 'purchase_of_ppe', 'name': 'Purchase Of Property, Plant, Equipment'}
#   {'key': 'sale_of_ppe', 'name': 'Sale Of Property, Plant, Equipment'}
#   {'key': 'purchase_of_business', 'name': 'Purchase Of Business'}
#   {'key': 'sale_of_business', 'name': 'Sale Of Business'}
#   {'key': 'purchase_of_investment', 'name': 'Purchase Of Investment'}
#   {'key': 'sale_of_investment', 'name': 'Sale Of Investment'}
#   {'key': 'net_intangibles_purchase_and_sale', 'name': 'Net Intangibles Purchase And Sale'}
#   {'key': 'cash_flow_from_investing', 'name': 'Cash Flow from Investing'}
#   {'key': 'issuance_of_stock', 'name': 'Issuance of Stock'}
#   {'key': 'repurchase_of_stock', 'name': 'Repurchase of Stock'}
#   {'key': 'net_issuance_of_preferred', 'name': 'Net Issuance of Preferred Stock'}
#   {'key': 'net_issuance_of_debt', 'name': 'Net Issuance of Debt'}
#   {'key': 'dividends', 'name': 'Cash Flow for Dividends'}
#   {'key': 'cash_flow_for_lease_financing', 'name': 'Cash Flow for Lease Financing'}
#   {'key': 'cash_from_financing', 'name': 'Cash Flow from Financing'}
#   {'key': 'beginning_cash_position', 'name': 'Beginning Cash Position'}
#   {'key': 'effect_of_exchange_rate_changes', 'name': 'Effect of Exchange Rate Changes'}
#   {'key': 'net_change_in_cash', 'name': 'Net Change in Cash'}
#   {'key': 'ending_cash_position', 'name': 'Ending Cash Position'}
#   {'key': 'cash_flow_capital_expenditure', 'name': 'Capital Expenditure'}
#   {'key': 'total_free_cash_flow', 'name': 'Free Cash Flow'}
#   {'key': 'maintenance_capex', 'name': 'Maintenance Capex'}
#   {'key': 'growth_capex', 'name': 'Growth Capex'}
#   {'key': 'insider_buy', 'name': 'Number of insider Buys'}
#   {'key': 'insider_sell', 'name': 'Number of insider sells'}
#   {'key': 'insider_buy_volume', 'name': 'Insider Buys Volume'}
#   {'key': 'insider_sells_volume', 'name': 'Insider Sells Volume'}
#   {'key': 'institutional_guru_buy_pct', 'name': '% of 13F Buys'}
#   {'key': 'institutional_guru_sell_pct', 'name': '% of 13F Sells'}
#   {'key': 'institutional_guru_hold_pct', 'name': '% of 13F Holds'}
#   {'key': 'etf_guru_buy_pct', 'name': '% of ETF Buys'}
#   {'key': 'etf_guru_sell_pct', 'name': '% of ETF Sells'}
#   {'key': 'etf_guru_hold_pct', 'name': '% of ETF Holds'}
#   {'key': 'mutual_fund_guru_buy_pct', 'name': '% of Mutual Fund Buys'}
#   {'key': 'mutual_fund_guru_sell_pct', 'name': '% of Mutual Fund Sells'}
#   {'key': 'mutual_fund_guru_hold_pct', 'name': '% of Mutual Fund Holds'}
#   {'key': 'premium_guru_buy_pct', 'name': '% of Premium Guru Buys'}
#   {'key': 'premium_guru_sell_pct', 'name': '% of Premium Guru Sells'}
#   {'key': 'premium_guru_hold_pct', 'name': '% of Premium Guru Holds'}
#   {'key': 'plus_guru_buy_pct', 'name': '% of PremiumPlus Guru Buys'}
#   {'key': 'plus_guru_sell_pct', 'name': '% of PremiumPlus Guru Sells'}
#   {'key': 'plus_guru_hold_pct', 'name': '% of PremiumPlus Guru Holds'}
#   {'key': 'guru_sell', 'name': 'Number of Premium Guru Sells'}
#   {'key': 'guru_buy', 'name': 'Number of Premium Guru Buys'}
#   {'key': 'guru_hold', 'name': 'Number of Premium Guru Holds'}
#   {'key': 'plus_guru_buy', 'name': 'Number of PremiumPlus Guru Buys'}
#   {'key': 'plus_guru_sell', 'name': 'Number of PremiumPlus Guru Sells'}
#   {'key': 'plus_guru_hold', 'name': 'Number of PremiumPlus Guru Holds'}
#   {'key': 'guru_buy_volume', 'name': 'Shares of Premium Guru Buys'}
#   {'key': 'guru_sell_volume', 'name': 'Shares of Premium Guru Sells'}
#   {'key': 'guru_hold_volume', 'name': 'Shares of Premium Guru Holds'}
#   {'key': 'plus_guru_buy_volume', 'name': 'Shares of PremiumPlus Guru Buys'}
#   {'key': 'plus_guru_sell_volume', 'name': 'Shares of PremiumPlus Guru Sells'}
#   {'key': 'plus_guru_hold_volume', 'name': 'Shares of PremiumPlus Guru Holds'}
#   {'key': 'ShortInterest', 'name': 'Short Interest'}