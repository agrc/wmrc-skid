import pandas as pd

from wmrc import helpers


class TestYearlyMetrics:

    def test_statewide_yearly_metrics_happy_path(self):
        input_df = pd.DataFrame(
            {
                "county_wide_msw_recycled": [10, 10, 20],
                "county_wide_msw_composted": [0, 0, 50],
                "county_wide_msw_digested": [10, 0, 0],
                "county_wide_msw_landfilled": [80, 90, 30],
            },
            index=["foo", "bar", "baz"],
        )

        expected_output = pd.Series(
            {
                "statewide_msw_recycled": 40,
                "statewide_msw_composted": 50,
                "statewide_msw_digested": 10,
                "statewide_msw_landfilled": 200,
                "statewide_msw_recycling_rate": 100 / 300 * 100,
            }
        )

        output = helpers.YearlyAnalysis.statewide_yearly_metrics(input_df)

        pd.testing.assert_series_equal(output, expected_output)

    def test_statewide_yearly_metrics_removes_out_of_state_values(self):
        input_df = pd.DataFrame(
            {
                "county_wide_msw_recycled": [10, 10, 20, 100],
                "county_wide_msw_composted": [0, 0, 50, 100],
                "county_wide_msw_digested": [10, 0, 0, 100],
                "county_wide_msw_landfilled": [80, 90, 30, 700],
            },
            index=["foo", "bar", "baz", "Out of State"],
        )

        expected_output = pd.Series(
            {
                "statewide_msw_recycled": 40,
                "statewide_msw_composted": 50,
                "statewide_msw_digested": 10,
                "statewide_msw_landfilled": 200,
                "statewide_msw_recycling_rate": 100 / 300 * 100,
            }
        )

        output = helpers.YearlyAnalysis.statewide_yearly_metrics(input_df)

        pd.testing.assert_series_equal(output, expected_output)


class TestSmallMethods:

    def test_add_bogus_geometries_happy_path(self):
        input_df = pd.DataFrame(
            {
                "a": [1, 2],
                "b": [3, 4],
            }
        )

        result_df = helpers.add_bogus_geometries(input_df)

        assert result_df.spatial.validate()
