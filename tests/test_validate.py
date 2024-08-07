import itertools

import numpy as np
import pandas as pd
import pytest
from wmrc import validate


class TestReportValidations:

    @pytest.fixture
    def input_df(self):
        recycling_rate_2023_01 = 5 / 95 * 100
        recycling_rate_2022_01 = 2 / 82 * 100
        recycling_rate_2023_03 = 70 / 150 * 100
        recycling_rate_2022_03 = 70 / 70 * 100

        facilities_df = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023, 2023],
                "msw_recycled": [1.0, 20.0, 5.0, 10.0],
                "msw_composted": [0.0, 50.0, 0.0, 60.0],
                "msw_digested": [1.0, 0.0, 0.0, 0.0],
                "msw_landfilled": [80, 30, 90, 90],
                "msw_recycling_rate": [
                    recycling_rate_2022_01,
                    recycling_rate_2022_03,
                    recycling_rate_2023_01,
                    recycling_rate_2023_03,
                ],
            }
        )

        return facilities_df

    @pytest.fixture
    def expected_output(self):
        recycling_rate_2023_01 = 5 / 95 * 100
        recycling_rate_2022_01 = 2 / 82 * 100
        recycling_rate_diff_01 = recycling_rate_2023_01 - recycling_rate_2022_01
        recycling_rate_2023_03 = 70 / 150 * 100
        recycling_rate_2022_03 = 70 / 70 * 100
        recycling_rate_diff_03 = recycling_rate_2023_03 - recycling_rate_2022_03

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
                "msw_recycling_rate_pct_change": [
                    recycling_rate_diff_01 / recycling_rate_2022_01 * 100,
                    recycling_rate_diff_03 / recycling_rate_2022_03 * 100,
                ],
                "msw_recycling_rate_2023": [recycling_rate_2023_01, recycling_rate_2023_03],
                "msw_recycling_rate_2022": [recycling_rate_2022_01, recycling_rate_2022_03],
                "msw_recycling_rate_diff": [recycling_rate_diff_01, recycling_rate_diff_03],
            },
            index=pd.MultiIndex.from_tuples([("SW01", "foo"), ("SW03", "baz")], names=["id", "name"]),
        )

        return expected_output

    def test_facility_year_over_year(self, input_df, expected_output):
        new_columns = pd.DataFrame(
            {
                "id": ["SW01", "SW03", "SW01", "SW03"],
                "name": ["foo", "baz", "foo", "baz"],
            }
        )
        input_df = pd.concat([new_columns, input_df], axis=1)

        output = validate.facility_year_over_year(input_df, 2023)

        pd.testing.assert_frame_equal(expected_output, output)

    def test_county_year_over_year_happy_path(self, input_df, expected_output):

        counties_df = input_df
        counties_df.index = pd.Index(["Cache", "Utah", "Cache", "Utah"], name="name")
        counties_df.columns = [f"county_wide_{col}" if "msw_" in col else col for col in counties_df.columns]

        output = validate.county_year_over_year(counties_df, 2023)

        expected_output.index = pd.Index(["Cache", "Utah"], name="name")
        expected_output.columns = [f"county_wide_{col}" for col in expected_output.columns]

        pd.testing.assert_frame_equal(expected_output, output)

    def test_state_year_over_year_happy_path(self):

        recycling_rate_2022 = (1 + 1 + 50 + 1) / (1 + 1 + 50 + 1 + 10 + 40) * 100
        recycling_rate_2023 = (2 + 2 + 60) / (2 + 2 + 60 + 30 + 70) * 100
        recycling_rate_diff = recycling_rate_2023 - recycling_rate_2022

        counties_df = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023, 2023],
                "county_wide_msw_recycled": [1.0, 1.0, 2.0, 2.0],
                "county_wide_msw_composted": [0.0, 50.0, 0.0, 60.0],
                "county_wide_msw_digested": [1.0, 0.0, 0.0, 0.0],
                "county_wide_msw_landfilled": [10, 40, 30, 70],
                "county_wide_msw_recycling_rate": [
                    "foo",
                    "bar",
                    "baz",
                    "qux",
                ],  #: yearly.statewide_metrics calcs its own recycling rate based on the summed county values
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
                "statewide_msw_recycling_rate_pct_change": [recycling_rate_diff / recycling_rate_2022 * 100],
                "statewide_msw_recycling_rate_2023": [recycling_rate_2023],
                "statewide_msw_recycling_rate_2022": [recycling_rate_2022],
                "statewide_msw_recycling_rate_diff": [recycling_rate_diff],
            },
            index=pd.Index(["Statewide"], name="name"),
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
