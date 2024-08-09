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


class TestAddFacilityInfo:

    def test_add_facility_info_simple_no_duplicates(self, mocker):
        records_mock = mocker.Mock()
        records_mock.df = pd.DataFrame(
            {
                "LastModifiedDate": [
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2022-01-01"),
                ],
                "Facility_Name__c": ["foo", "bar", "baz"],
                "facility_id": ["SW0123", "SW0124", "SW0125"],
                "Are_materials_accepted_for_drop_off__c": ["Yes", "No", "Yes"],
                "Facility_Phone_Number__c": ["123-456-7890", "234-567-8901", "345-678-9012"],
                "Facility_Website__c": ["http://foo.com", "http://bar.com", "http://baz.com"],
            }
        )
        records_mock.df["LastModifiedDate"] = pd.to_datetime(records_mock.df["LastModifiedDate"])

        facility_df = pd.DataFrame(
            {
                "id_": ["123", "124", "125"],
                "Facility_Name__c": ["foo", "bar", "baz"],
                "tons_of_material_diverted_from_": [1, 2, 3],
            }
        )

        output_df = summarize._add_facility_info(facility_df, records_mock)

        expected_df = pd.DataFrame(
            {
                "id_": ["123", "124", "125"],
                "tons_of_material_diverted_from_": [1, 2, 3],
                "accept_material_dropped_off_by_": ["Yes", "No", "Yes"],
                "phone_no_": ["123-456-7890", "234-567-8901", "345-678-9012"],
                "website": ["http://foo.com", "http://bar.com", "http://baz.com"],
                "facility_name": ["foo", "bar", "baz"],
            }
        )

        pd.testing.assert_frame_equal(output_df, expected_df)

    def test_add_facility_info_uses_latest_info(self, mocker):
        records_mock = mocker.Mock()
        records_mock.df = pd.DataFrame(
            {
                "LastModifiedDate": [
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2022-10-01"),
                    pd.Timestamp("2022-01-01"),
                ],
                "Facility_Name__c": ["foo", "Foo", "baz"],
                "facility_id": ["SW0123", "SW0123", "SW0124"],
                "Are_materials_accepted_for_drop_off__c": ["Yes", "No", "Yes"],
                "Facility_Phone_Number__c": ["123-456-7890", "123-456-7890", "345-678-9012"],
                "Facility_Website__c": ["http://foo.com", "http://foo.com", "http://baz.com"],
            }
        )
        records_mock.df["LastModifiedDate"] = pd.to_datetime(records_mock.df["LastModifiedDate"])

        facility_df = pd.DataFrame(
            {
                "id_": ["123", "124"],
                "Facility_Name__c": ["foo", "baz"],
                "tons_of_material_diverted_from_": [1, 2],
            }
        )

        output_df = summarize._add_facility_info(facility_df, records_mock)

        expected_df = pd.DataFrame(
            {
                "id_": ["123", "124"],
                "tons_of_material_diverted_from_": [1, 2],
                "accept_material_dropped_off_by_": ["No", "Yes"],
                "phone_no_": ["123-456-7890", "345-678-9012"],
                "website": ["http://foo.com", "http://baz.com"],
                "facility_name": ["Foo", "baz"],
            }
        )

        pd.testing.assert_frame_equal(output_df, expected_df)

    def test_add_facility_info_uses_out_of_order_latest_info(self, mocker):
        records_mock = mocker.Mock()
        records_mock.df = pd.DataFrame(
            {
                "LastModifiedDate": [
                    pd.Timestamp("2022-10-01"),
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2022-01-01"),
                ],
                "Facility_Name__c": ["foo", "Foo", "baz"],
                "facility_id": ["SW0123", "SW0123", "SW0124"],
                "Are_materials_accepted_for_drop_off__c": ["Yes", "No", "Yes"],
                "Facility_Phone_Number__c": ["123-456-7890", "123-456-7890", "345-678-9012"],
                "Facility_Website__c": ["http://foo.com", "http://foo.com", "http://baz.com"],
            }
        )
        records_mock.df["LastModifiedDate"] = pd.to_datetime(records_mock.df["LastModifiedDate"])

        facility_df = pd.DataFrame(
            {
                "id_": ["123", "124"],
                "Facility_Name__c": ["Foo", "bar"],
                "tons_of_material_diverted_from_": [1, 2],
            }
        )

        output_df = summarize._add_facility_info(facility_df, records_mock)

        expected_df = pd.DataFrame(
            {
                "id_": ["123", "124"],
                "tons_of_material_diverted_from_": [1, 2],
                "accept_material_dropped_off_by_": ["Yes", "Yes"],
                "phone_no_": ["123-456-7890", "345-678-9012"],
                "website": ["http://foo.com", "http://baz.com"],
                "facility_name": ["foo", "baz"],
            }
        )

        pd.testing.assert_frame_equal(output_df, expected_df)


class TestFacilityMetrics:

    def test_facility_metrics_happy_path(self, mocker):
        input_df = pd.DataFrame(
            {
                "Calendar_Year__c": [2022, 2023, 2022],
                "Facility_Name__c": ["foo", "foo", "baz"],
                "facility_id": ["SW01", "SW01", "SW03"],
                "Municipal_Solid_Waste__c": [10, 50, 100],
                "Combined_Total_of_Material_Recycled__c": [10, 10, 20],
                "Total_Materials_sent_to_composting__c": [0, 0, 50],
                "Total_Material_managed_by_ADC__c": [10, 0, 0],
                "Municipal_Waste_In_State_in_Tons__c": [80, 90, 30],
            }
        )
        sf_records = mocker.Mock()
        sf_records.df = input_df

        expected_output = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023],
                "id": ["SW01", "SW03", "SW01"],
                "name": ["foo", "baz", "foo"],
                "msw_recycled": [1.0, 20.0, 5.0],
                "msw_composted": [0.0, 50.0, 0.0],
                "msw_digested": [1.0, 0.0, 0.0],
                "msw_landfilled": [80, 30, 90],
                "msw_recycling_rate": [2 / 82 * 100, 70 / 100 * 100, 5 / 95 * 100],
            }
        )

        output = summarize.facility_metrics(sf_records)

        pd.testing.assert_frame_equal(expected_output, output)
