"""
AutoQuant ETL — India Financial Year Calendar Utilities
=========================================================
India's financial year runs April 1 → March 31.
FY26 = April 1 2025 → March 31 2026.

Quarter mapping (within an FY):
  Q1: Apr–Jun
  Q2: Jul–Sep
  Q3: Oct–Dec
  Q4: Jan–Mar

Quarter notation: Q{n}FY{yy}  e.g. Q3FY26
"""

from __future__ import annotations

import re
from calendar import monthrange
from datetime import date
from typing import Tuple


def date_to_fy(d: date) -> str:
    """
    Return the Indian Financial Year label for a given date.

    The FY label uses the *end* calendar year (two-digit suffix).
    e.g. April 2025 – March 2026  → "FY26"

    Args:
        d: any calendar date

    Returns:
        FY label string such as "FY26"
    """
    # FY ends in March of the following calendar year
    # April 2025 is start of FY26; January 2026 is still FY26
    if d.month >= 4:
        fy_end_year = d.year + 1
    else:
        fy_end_year = d.year

    return f"FY{fy_end_year % 100:02d}"


def date_to_fy_quarter(d: date) -> str:
    """
    Return the Indian FY quarter label for a given date.

    Quarter mapping:
      Q1: Apr–Jun
      Q2: Jul–Sep
      Q3: Oct–Dec
      Q4: Jan–Mar

    Args:
        d: any calendar date

    Returns:
        Quarter label such as "Q3FY26"

    Example:
        >>> date_to_fy_quarter(date(2026, 1, 15))
        'Q3FY26'
    """
    month = d.month

    if month in (4, 5, 6):
        q = 1
    elif month in (7, 8, 9):
        q = 2
    elif month in (10, 11, 12):
        q = 3
    else:  # 1, 2, 3
        q = 4

    fy = date_to_fy(d)
    return f"Q{q}{fy}"


def fy_quarter_date_range(quarter: str) -> Tuple[date, date]:
    """
    Return the (start_date, end_date) inclusive for an FY quarter string.

    Args:
        quarter: e.g. "Q3FY26"

    Returns:
        Tuple of (start_date, end_date) where both are inclusive.
        e.g. "Q3FY26" → (date(2025, 10, 1), date(2025, 12, 31))

    Raises:
        ValueError: if the quarter string is not recognised
    """
    pattern = re.fullmatch(r"Q([1-4])FY(\d{2})", quarter, re.IGNORECASE)
    if not pattern:
        raise ValueError(
            f"Invalid quarter format: '{quarter}'. Expected Q{{1-4}}FY{{YY}} e.g. Q3FY26"
        )

    q_num = int(pattern.group(1))
    fy_yy = int(pattern.group(2))

    # Resolve two-digit year: 00-49 → 2000-2049, 50-99 → 1950-1999
    if fy_yy < 50:
        fy_end_year = 2000 + fy_yy
    else:
        fy_end_year = 1900 + fy_yy

    # fy_end_year is the calendar year in which Q4 (Jan-Mar) falls
    fy_start_year = fy_end_year - 1

    # Quarter → calendar months
    quarter_months = {
        1: (4, 6, fy_start_year),   # Apr–Jun of FY start year
        2: (7, 9, fy_start_year),   # Jul–Sep of FY start year
        3: (10, 12, fy_start_year), # Oct–Dec of FY start year
        4: (1, 3, fy_end_year),     # Jan–Mar of FY end year
    }

    start_month, end_month, cal_year = quarter_months[q_num]

    start_date = date(cal_year, start_month, 1)
    last_day = monthrange(cal_year, end_month)[1]
    end_date = date(cal_year, end_month, last_day)

    return start_date, end_date


def current_fy_quarter(reference: date | None = None) -> str:
    """
    Return the current (or reference date's) FY quarter label.

    Args:
        reference: optional date override; defaults to today

    Returns:
        FY quarter string e.g. "Q4FY26"
    """
    d = reference or date.today()
    return date_to_fy_quarter(d)
