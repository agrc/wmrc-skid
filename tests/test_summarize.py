import numpy as np
import pandas as pd
from wmrc import summarize


class TestSummaryMethods:

    def test_counties_happy_path(self, mocker):
        records_mock = mocker.Mock()
        summaries_df = pd.DataFrame(
            {
                "recycled": [1, 2, 3, 4],
                "landfilled": [5, 6, 7, 8],
                "total": [9, 10, 11, 12],
            },
            index=pd.MultiIndex.from_tuples(
                [
                    ("2022", "Box Elder County"),
                    ("2022", "Out of State"),
                    ("2023", "Box Elder County"),
                    ("2023", "Out of State"),
                ],
                names=["year", "county"],
            ),
        )
        records_mock.df.groupby.return_value.apply.return_value = summaries_df

        result_df = summarize.counties(records_mock)

        test_df = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023, 2023],
                "recycled": [1, 2, 3, 4],
                "landfilled": [5, 6, 7, 8],
                "total": [9, 10, 11, 12],
            },
            index=["Box Elder County", "Out of State", "Box Elder County", "Out of State"],
        )
        test_df.index.name = "name"

        pd.testing.assert_frame_equal(result_df, test_df)

    def test_counties_replace_nan_with_0(self, mocker):
        records_mock = mocker.Mock()
        summaries_df = pd.DataFrame(
            {
                "recycled": [1, 2, 3, 4],
                "landfilled": [5, 6, np.nan, 8],
                "total": [9, 10, np.nan, 12],
            },
            index=pd.MultiIndex.from_tuples(
                [
                    ("2022", "Box Elder County"),
                    ("2022", "Out of State"),
                    ("2023", "Box Elder County"),
                    ("2023", "Out of State"),
                ],
                names=["year", "county"],
            ),
        )
        records_mock.df.groupby.return_value.apply.return_value = summaries_df

        result_df = summarize.counties(records_mock)

        test_df = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023, 2023],
                "recycled": [1, 2, 3, 4],
                "landfilled": [5, 6, 0.0, 8],
                "total": [9, 10, 0.0, 12],
            },
            index=["Box Elder County", "Out of State", "Box Elder County", "Out of State"],
        )
        test_df.index.name = "name"

        pd.testing.assert_frame_equal(result_df, test_df)

    def test_recovery_rates_by_tonnage_happy_path(self, mocker):
        records = mocker.Mock()
        records.df = pd.DataFrame(
            {
                "Calendar_Year__c": [2022, 2022, 2023, 2023],
                "Out_of_State__c": [0, 0, 0, 0],
                "Annual_Recycling_Contamination_Rate__c": [10, 0, 10, 20],
                "Combined_Total_of_Material_Recycled__c": [100, 100, 100, 100],
            }
        )

        output_series = summarize.recovery_rates_by_tonnage(records)

        test_df = pd.Series(
            {
                2022: 95.0,
                2023: 85.0,
            },
            name="annual_recycling_uncontaminated_rate",
        )
        test_df.index.name = "data_year"

        pd.testing.assert_series_equal(output_series, test_df)

    def test_recovery_rates_by_tonnage_uses_out_of_state_modifier(self, mocker):
        records = mocker.Mock()
        records.df = pd.DataFrame(
            {
                "facility_name": ["foo", "bar", "foo", "bar"],
                "Calendar_Year__c": [2022, 2022, 2023, 2023],
                "Out_of_State__c": [0, 100, 0, 100],
                "Annual_Recycling_Contamination_Rate__c": [10, 0, 10, 20],
                "Combined_Total_of_Material_Recycled__c": [100, 100, 100, 100],
            }
        )

        output_series = summarize.recovery_rates_by_tonnage(records)

        test_df = pd.Series(
            {
                2022: 90.0,
                2023: 90.0,
            },
            name="annual_recycling_uncontaminated_rate",
        )
        test_df.index.name = "data_year"

        pd.testing.assert_series_equal(output_series, test_df)
