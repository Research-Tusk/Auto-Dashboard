"""
Tests for FY calendar utilities.
"""

from datetime import date

import pytest

from autoquant_etl.utils.fy_calendar import (
    month_to_fy_year,
    month_to_fy_quarter,
    fy_quarter_months,
    current_fy_quarter,
)


class TestMonthToFyYear:
    def test_april_to_march_next_year(self):
        assert month_to_fy_year(date(2025, 4, 1)) == "FY26"
        assert month_to_fy_year(date(2025, 12, 31)) == "FY26"
        assert month_to_fy_year(date(2026, 3, 31)) == "FY26"

    def test_january_to_march_current_year(self):
        assert month_to_fy_year(date(2025, 1, 1)) == "FY25"
        assert month_to_fy_year(date(2025, 3, 31)) == "FY25"

    def test_century_boundary(self):
        # FY00 (Apr 1999 - Mar 2000) = FY00
        assert month_to_fy_year(date(1999, 4, 1)) == "FY00"


class TestMonthToFyQuarter:
    def test_q1(self):
        assert month_to_fy_quarter(date(2025, 4, 15)) == "Q1FY26"
        assert month_to_fy_quarter(date(2025, 6, 30)) == "Q1FY26"

    def test_q2(self):
        assert month_to_fy_quarter(date(2025, 7, 1)) == "Q2FY26"
        assert month_to_fy_quarter(date(2025, 9, 30)) == "Q2FY26"

    def test_q3(self):
        assert month_to_fy_quarter(date(2025, 10, 1)) == "Q3FY26"
        assert month_to_fy_quarter(date(2025, 12, 31)) == "Q3FY26"

    def test_q4(self):
        assert month_to_fy_quarter(date(2026, 1, 1)) == "Q4FY26"
        assert month_to_fy_quarter(date(2026, 3, 31)) == "Q4FY26"


class TestFyQuarterMonths:
    def test_q3fy26(self):
        months = fy_quarter_months("Q3FY26")
        assert len(months) == 3
        assert months[0] == date(2025, 10, 1)
        assert months[1] == date(2025, 11, 1)
        assert months[2] == date(2025, 12, 1)

    def test_q4fy26(self):
        months = fy_quarter_months("Q4FY26")
        assert months[0] == date(2026, 1, 1)
        assert months[2] == date(2026, 3, 1)

    def test_q1fy26(self):
        months = fy_quarter_months("Q1FY26")
        assert months[0] == date(2025, 4, 1)
        assert months[2] == date(2025, 6, 1)
