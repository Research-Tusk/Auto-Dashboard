"""
AutoQuant ETL — FY Calendar Utilities
=======================================
India fiscal year: April 1 – March 31.
  FY26 = April 1, 2025 – March 31, 2026

FY Quarter mapping:
  Q1FY26 = Apr-Jun 2025
  Q2FY26 = Jul-Sep 2025
  Q3FY26 = Oct-Dec 2025
  Q4FY26 = Jan-Mar 2026
"""

from __future__ import annotations

from datetime import date
from typing import List


def month_to_fy_year(d: date) -> str:
    """
    Return FY year string for a given date.

    Examples:
        2025-04-01 → 'FY26'
        2026-01-15 → 'FY26'
        2025-03-31 → 'FY25'
    """
    if d.month >= 4:
        return f"FY{(d.year + 1) % 100:02d}"
    return f"FY{d.year % 100:02d}"


def month_to_fy_quarter(d: date) -> str:
    """
    Return FY quarter string for a given date.

    Examples:
        2025-04-01 → 'Q1FY26'
        2025-07-15 → 'Q2FY26'
        2025-10-01 → 'Q3FY26'
        2026-01-31 → 'Q4FY26'
    """
    fy = month_to_fy_year(d)
    month = d.month

    if month in (4, 5, 6):
        q = "Q1"
    elif month in (7, 8, 9):
        q = "Q2"
    elif month in (10, 11, 12):
        q = "Q3"
    else:  # 1, 2, 3
        q = "Q4"

    return f"{q}{fy}"


def fy_quarter_months(fy_quarter: str) -> List[date]:
    """
    Return list of first-of-month dates for a given FY quarter string.

    Args:
        fy_quarter: e.g. 'Q3FY26'

    Returns:
        List of 3 date objects (first day of each month in the quarter)

    Example:
        fy_quarter_months('Q3FY26') → [2025-10-01, 2025-11-01, 2025-12-01]
    """
    # Parse quarter number and FY year
    q_num = int(fy_quarter[1])  # 1-4
    fy_year_short = int(fy_quarter[4:])  # e.g. 26

    # Convert FY year to calendar year
    # FY26 starts in April 2025, ends March 2026
    cal_year_start = 2000 + fy_year_short - 1  # e.g. 2025 for FY26

    quarter_start_months = {1: (cal_year_start, 4), 2: (cal_year_start, 7),
                            3: (cal_year_start, 10), 4: (cal_year_start + 1, 1)}
    start_year, start_month = quarter_start_months[q_num]

    months = []
    for i in range(3):
        month = start_month + i
        year = start_year
        if month > 12:
            month -= 12
            year += 1
        months.append(date(year, month, 1))

    return months


def current_fy_quarter() -> str:
    """Return the current FY quarter string based on today's date."""
    return month_to_fy_quarter(date.today())
