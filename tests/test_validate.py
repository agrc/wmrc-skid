import itertools

import numpy as np
import pandas as pd

from wmrc import validate


class TestReportValidations:

    def test_facility_year_over_year(self):
        facilities_df = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023, 2023],
                "id": ["SW01", "SW03", "SW01", "SW03"],
                "name": ["foo", "baz", "foo", "baz"],
                "msw_recycled": [1.0, 20.0, 5.0, 10.0],
                "msw_composted": [0.0, 50.0, 0.0, 60.0],
                "msw_digested": [1.0, 0.0, 0.0, 0.0],
                "msw_landfilled": [80, 30, 90, 90],
            }
        )

        expected_output = pd.DataFrame(
            {
                "msw_recycled_pct_change": [400.0, -50.0],
                "msw_recycled_2023": [5.0, 10.0],
                "msw_recycled_2022": [1.0, 20.0],
                "msw_recycled_diff": [4.0, -10.0],
                "msw_composted_pct_change": [np.nan, 20.0],
                "msw_composted_2023": [0.0, 60.0],
                "msw_composted_2022": [0.0, 50.0],
                "msw_composted_diff": [0.0, 10.0],
                "msw_digested_pct_change": [-100.0, np.nan],
                "msw_digested_2023": [0.0, 0.0],
                "msw_digested_2022": [1.0, 0.0],
                "msw_digested_diff": [-1.0, 0.0],
                "msw_landfilled_pct_change": [12.5, 200.0],
                "msw_landfilled_2023": [90, 90],
                "msw_landfilled_2022": [80, 30],
                "msw_landfilled_diff": [10, 60],
            },
            index=pd.MultiIndex.from_tuples([("SW01", "foo"), ("SW03", "baz")], names=["id", "name"]),
        )

        output = validate.facility_year_over_year(facilities_df, 2023)

        pd.testing.assert_frame_equal(expected_output, output)

    def test_county_year_over_year_happy_path(self):
        counties_df = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023, 2023],
                "county_wide_msw_recycled": [1.0, 20.0, 5.0, 10.0],
                "county_wide_msw_composted": [0.0, 50.0, 0.0, 60.0],
                "county_wide_msw_digested": [1.0, 0.0, 0.0, 0.0],
                "county_wide_msw_landfilled": [80, 30, 90, 90],
                "county_wide_msw_recycling_rate": ["foo", "bar", "baz", "qux"],
            },
            index=pd.Index(["Cache", "Utah", "Cache", "Utah"], name="name"),
        )

        expected_output = pd.DataFrame(
            {
                "county_wide_msw_recycled_pct_change": [400.0, -50.0],
                "county_wide_msw_recycled_2023": [5.0, 10.0],
                "county_wide_msw_recycled_2022": [1.0, 20.0],
                "county_wide_msw_recycled_diff": [4.0, -10.0],
                "county_wide_msw_composted_pct_change": [np.nan, 20.0],
                "county_wide_msw_composted_2023": [0.0, 60.0],
                "county_wide_msw_composted_2022": [0.0, 50.0],
                "county_wide_msw_composted_diff": [0.0, 10.0],
                "county_wide_msw_digested_pct_change": [-100.0, np.nan],
                "county_wide_msw_digested_2023": [0.0, 0.0],
                "county_wide_msw_digested_2022": [1.0, 0.0],
                "county_wide_msw_digested_diff": [-1.0, 0.0],
                "county_wide_msw_landfilled_pct_change": [12.5, 200.0],
                "county_wide_msw_landfilled_2023": [90, 90],
                "county_wide_msw_landfilled_2022": [80, 30],
                "county_wide_msw_landfilled_diff": [10, 60],
            },
            index=pd.Index(["Cache", "Utah"], name="name"),
        )

        output = validate.county_year_over_year(counties_df, 2023)

        pd.testing.assert_frame_equal(expected_output, output)

    def test_state_year_over_year_happy_path(self):
        counties_df = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023, 2023],
                "county_wide_msw_recycled": [1.0, 1.0, 2.0, 2.0],
                "county_wide_msw_composted": [0.0, 50.0, 0.0, 60.0],
                "county_wide_msw_digested": [1.0, 0.0, 0.0, 0.0],
                "county_wide_msw_landfilled": [10, 40, 30, 70],
                "county_wide_msw_recycling_rate": ["foo", "bar", "baz", "qux"],
            },
            index=pd.Index(["Cache", "Utah", "Cache", "Utah"], name="name"),
        )

        expected_output = pd.DataFrame(
            {
                "statewide_msw_recycled_pct_change": [100.0],
                "statewide_msw_recycled_2023": [4.0],
                "statewide_msw_recycled_2022": [2.0],
                "statewide_msw_recycled_diff": [2.0],
                "statewide_msw_composted_pct_change": [20.0],
                "statewide_msw_composted_2023": [60.0],
                "statewide_msw_composted_2022": [50.0],
                "statewide_msw_composted_diff": [10.0],
                "statewide_msw_digested_pct_change": [-100.0],
                "statewide_msw_digested_2023": [0.0],
                "statewide_msw_digested_2022": [1.0],
                "statewide_msw_digested_diff": [-1.0],
                "statewide_msw_landfilled_pct_change": [100.0],
                "statewide_msw_landfilled_2023": [100.0],
                "statewide_msw_landfilled_2022": [50.0],
                "statewide_msw_landfilled_diff": [50.0],
            },
            index=pd.Index(["State"], name="name"),
        )

        output = validate.state_year_over_year(counties_df, 2023)

        pd.testing.assert_frame_equal(expected_output, output)


class TestYearOverYearChanges:

    def test_year_over_year_changes_happy_path(self):
        entries_df = pd.DataFrame(
            {
                "msw_recycled": [1.0, 20.0, 5.0, 10.0],
                "msw_composted": [0.0, 50.0, 0.0, 60.0],
                "msw_digested": [1.0, 0.0, 0.0, 0.0],
                "msw_landfilled": [80, 30, 90, 90],
            },
            index=pd.MultiIndex.from_tuples(itertools.product((2022, 2023), ("SW01", "SW03")), names=["year", "id"]),
        )

        expected_output = pd.DataFrame(
            {
                "msw_recycled_pct_change": [400.0, -50.0],
                "msw_recycled_2023": [5.0, 10.0],
                "msw_recycled_2022": [1.0, 20.0],
                "msw_recycled_diff": [4.0, -10.0],
                "msw_composted_pct_change": [np.nan, 20.0],
                "msw_composted_2023": [0.0, 60.0],
                "msw_composted_2022": [0.0, 50.0],
                "msw_composted_diff": [0.0, 10.0],
                "msw_digested_pct_change": [-100.0, np.nan],
                "msw_digested_2023": [0.0, 0.0],
                "msw_digested_2022": [1.0, 0.0],
                "msw_digested_diff": [-1.0, 0.0],
                "msw_landfilled_pct_change": [12.5, 200.0],
                "msw_landfilled_2023": [90, 90],
                "msw_landfilled_2022": [80, 30],
                "msw_landfilled_diff": [10, 60],
            },
            index=pd.Index(["SW01", "SW03"], name="id"),
        )

        output = validate._year_over_year_changes(entries_df, 2023)

        pd.testing.assert_frame_equal(expected_output, output)
