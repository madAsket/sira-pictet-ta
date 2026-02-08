from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ValueType = Literal["text", "real", "integer", "date"]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sqlite_type: str
    value_type: ValueType
    aliases: tuple[str, ...]
    required: bool = False


def column(
    name: str,
    sqlite_type: str,
    value_type: ValueType,
    *aliases: str,
    required: bool = False,
) -> ColumnSpec:
    return ColumnSpec(
        name=name,
        sqlite_type=sqlite_type,
        value_type=value_type,
        aliases=tuple(aliases),
        required=required,
    )


COLUMN_SPECS: tuple[ColumnSpec, ...] = (
    column("isin", "TEXT", "text", "ISIN", required=True),
    column("ticker", "TEXT", "text", "Ticker", "Stock Symbol", "Symbol"),
    column(
        "company_name",
        "TEXT",
        "text",
        "Company Name",
        "Company",
        "Issuer Name",
        required=True,
    ),
    column(
        "company_description",
        "TEXT",
        "text",
        "Company Description",
        "Description",
    ),
    column("investment_case_teaser", "TEXT", "text", "Investment Case Teaser"),
    column("investment_case_description", "TEXT", "text", "Investment Case Description"),
    column("swot_analysis", "TEXT", "text", "SWOT Analysis"),
    column("esg_analysis", "TEXT", "text", "ESG Analysis"),
    column("previous_recommendation", "TEXT", "text", "Previous Recommendation"),
    column("recommendation_date", "TEXT", "date", "Recommendation Date"),
    column("recommendation_comment", "TEXT", "text", "Recommendation Comment"),
    column("focus_list_status", "TEXT", "text", "Focus List Status"),
    column("focus_list_status_date", "TEXT", "date", "Focus List Status Date"),
    column("focus_list_status_details", "TEXT", "text", "Focus List Status Details"),
    column("currency", "TEXT", "text", "Currency"),
    column("price", "REAL", "real", "Price", "Stock Price"),
    column("price_date", "TEXT", "date", "Price Date"),
    column("target_price", "REAL", "real", "Target Price"),
    column("target_price_ratio", "REAL", "real", "Target Price Ratio"),
    column("target_price_date", "TEXT", "date", "Target Price Date"),
    column("price_52w_highest", "REAL", "real", "Price 52W Highest"),
    column("price_52w_lowest", "REAL", "real", "Price 52W Lowest"),
    column("dividend_yield", "REAL", "real", "Dividend Yield"),
    column("beta", "REAL", "real", "Beta"),
    column("average_daily_shares_traded", "REAL", "real", "Average Daily Shares Traded"),
    column("market_capitalization", "REAL", "real", "Market Capitalization", "Market Cap"),
    column("free_cash_flow_to_sales", "REAL", "real", "Free Cash Flow to Sales"),
    column("earning_per_share_fy0", "REAL", "real", "Earning Per Share FY0"),
    column("earning_per_share_fy1", "REAL", "real", "Earning Per Share FY1"),
    column("earning_per_share_change_fy1", "REAL", "real", "Earning Per Share Change FY1"),
    column("price_to_earning_historical_10y", "REAL", "real", "Price to Earning Historical 10Y"),
    column("price_to_earning_forward_12m", "REAL", "real", "Price to Earning Forward 12M"),
    column("price_to_earning_fy1", "REAL", "real", "Price to Earning FY1"),
    column("price_to_earning_fy2", "REAL", "real", "Price to Earning FY2"),
    column("return_on_equity_forward_12m", "REAL", "real", "Return on Equity Forward 12M"),
    column("return_on_equity_fy1", "REAL", "real", "Return on Equity FY1"),
    column("return_on_equity_fy2", "REAL", "real", "Return on Equity FY2"),
    column("return_on_capital_employed_fy1", "REAL", "real", "Return on Capital Employed FY1"),
    column("return_on_capital_employed_fy2", "REAL", "real", "Return on Capital Employed FY2"),
    column("net_debt_to_equity_fy1", "REAL", "real", "Net Debt to Equity FY1"),
    column("net_debt_to_equity_fy2", "REAL", "real", "Net Debt to Equity FY2"),
    column("net_debt_to_ebitda_forward_12m", "REAL", "real", "Net Debt to EBITDA Forward 12M"),
    column("net_debt_to_ebitda_fy1", "REAL", "real", "Net Debt to EBITDA FY1"),
    column("net_debt_to_ebitda_fy2", "REAL", "real", "Net Debt to EBITDA FY2"),
    column("price_to_book_value_forward_12m", "REAL", "real", "Price to Book Value Forward 12M"),
    column("price_to_book_value_fy1", "REAL", "real", "Price to Book Value FY1"),
    column("price_to_book_value_fy2", "REAL", "real", "Price to Book Value FY2"),
    column("dividend_yield_current", "REAL", "real", "Dividend Yield Current"),
    column("dividend_yield_forward_12m", "REAL", "real", "Dividend Yield Forward 12M"),
    column("dividend_yield_fy1", "REAL", "real", "Dividend Yield FY1"),
    column("dividend_yield_fy2", "REAL", "real", "Dividend Yield FY2"),
    column(
        "enterprise_value_to_ebitda_forward_12m",
        "REAL",
        "real",
        "Enterprise Value to EBITDA Forward 12M",
    ),
    column("enterprise_value_to_ebitda_fy1", "REAL", "real", "Enterprise Value to EBITDA FY1"),
    column("enterprise_value_to_ebitda_fy2", "REAL", "real", "Enterprise Value to EBITDA FY2"),
    column("ebitda_margin_forward_12m", "REAL", "real", "EBITDA Margin Forward 12M"),
    column("net_profit_margin_forward_12m", "REAL", "real", "Net Profit Margin Forward 12M"),
    column("relative_performance_ytd", "REAL", "real", "Relative Performance YTD"),
    column("relative_performance_one_year", "REAL", "real", "Relative Performance One Year"),
    column("relative_performance_five_year", "REAL", "real", "Relative Performance Five Year"),
    column("relative_performance_to_market", "REAL", "real", "Relative Performance to Market"),
    column("last_update", "TEXT", "date", "Last Update"),
    column("metadata_type", "TEXT", "text", "Metadata Type"),
    column("metadata_provider_code", "TEXT", "text", "Metadata Provider Code"),
    column("metadata_provider_business_line", "TEXT", "text", "Metadata Provider Business Line"),
    column("issuer_country", "TEXT", "text", "Issuer Country"),
    column("blocking_codes", "TEXT", "text", "Blocking Codes"),
    column("tax_rating", "TEXT", "text", "Tax Rating"),
    column("recommendation", "TEXT", "text", "Recommendation"),
    column("sector_level_1", "TEXT", "text", "Sector - Level 1", "Sector"),
    column("industry_group_level_2", "TEXT", "text", "Industry Group - Level 2"),
    column("industry_level_3", "TEXT", "text", "Industry - Level 3"),
    column("sub_industry_level_4", "TEXT", "text", "Sub-Industry - Level 4"),
    column("pwm_universe", "INTEGER", "integer", "PWM Universe"),
    column("region", "TEXT", "text", "Region"),
)

DERIVED_COLUMNS: tuple[ColumnSpec, ...] = (
    column("normalized_company_name", "TEXT", "text", "normalized_company_name", required=True),
)


def all_equities_columns() -> tuple[str, ...]:
    return tuple(spec.name for spec in COLUMN_SPECS) + tuple(spec.name for spec in DERIVED_COLUMNS)


def equities_insert_columns() -> tuple[str, ...]:
    return all_equities_columns()
