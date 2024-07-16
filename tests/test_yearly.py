import pandas as pd
from wmrc import yearly


class TestYearlyMetrics:

    def test_statewide_metrics_happy_path(self):
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

        output = yearly.statewide_metrics(input_df)

        pd.testing.assert_series_equal(output, expected_output)

    def test_statewide_metrics_removes_out_of_state_values(self):
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

        output = yearly.statewide_metrics(input_df)

        pd.testing.assert_series_equal(output, expected_output)

    def test_facility_combined_metrics_happy_path(self):
        input_df = pd.DataFrame(
            {
                "Facility_Name__c": ["foo", "bar", "baz"],
                "facility_id": ["SW01", "SW02", "SW03"],
                "Municipal_Solid_Waste__c": [10, 50, 100],
                "Combined_Total_of_Material_Recycled__c": [10, 10, 20],
                "Total_Materials_sent_to_composting__c": [0, 0, 50],
                "Total_Material_managed_by_ADC__c": [10, 0, 0],
                "Municipal_Waste_In_State_in_Tons__c": [80, 90, 30],
            }
        )

        expected_output = pd.DataFrame(
            {
                "id": ["SW01", "SW02", "SW03"],
                "name": ["foo", "bar", "baz"],
                "msw_recycled": [1.0, 5.0, 20.0],
                "msw_composted": [0.0, 0.0, 50.0],
                "msw_digested": [1.0, 0.0, 0.0],
                "msw_landfilled": [80, 90, 30],
            }
        )

        output = yearly.facility_combined_metrics(input_df)

        pd.testing.assert_frame_equal(output, expected_output)
