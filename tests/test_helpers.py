import pandas as pd

from wmrc import helpers


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


class TestSalesForceRecords:

    def test_build_columns_string_happy_path(self, mocker):
        salesforce_records = mocker.Mock()

        salesforce_records.field_mapping = {
            "a": "b",
            "c": "d",
        }
        salesforce_records.county_fields = ["foo", "bar"]

        result = helpers.SalesForceRecords._build_columns_string(salesforce_records)

        assert (
            result
            == "b,d,RecordTypeId,Classifications__c,RecordType.Name,Facility__r.Solid_Waste_Facility_ID_Number__c,LastModifiedDate,foo,bar"
        )

    def test_deduplicate_records_on_facility_id_single_year(self, mocker):
        salesforce_records = mocker.Mock()
        salesforce_records.df = pd.DataFrame(
            {
                "facility_id": ["1", "2", "1"],
                "LastModifiedDate": ["2022-01-01", "2022-01-02", "2022-01-03"],
                "a": [1, 2, 3],
                "Calendar_Year__c": "2022",
            }
        )

        duplicate_ids = helpers.SalesForceRecords.deduplicate_records_on_facility_id(salesforce_records)

        expected_df = pd.DataFrame(
            {
                "facility_id": ["2", "1"],
                "LastModifiedDate": ["2022-01-02", "2022-01-03"],
                "a": [2, 3],
                "Calendar_Year__c": "2022",
            },
            index=[1, 2],
        )
        expected_df["LastModifiedDate"] = pd.to_datetime(expected_df["LastModifiedDate"])

        pd.testing.assert_frame_equal(salesforce_records.df, expected_df)
        assert duplicate_ids == {"1": "2022"}

    def test_deduplicate_records_on_facility_id_keeps_multiple_years(self, mocker):
        salesforce_records = mocker.Mock()
        salesforce_records.df = pd.DataFrame(
            {
                "facility_id": ["1", "2", "1", "2"],
                "LastModifiedDate": ["2022-01-01", "2022-01-02", "2023-01-03", "2023-12-02"],
                "a": [1, 2, 3, 4],
                "Calendar_Year__c": ["2022", "2022", "2022", "2023"],
            }
        )

        duplicate_ids = helpers.SalesForceRecords.deduplicate_records_on_facility_id(salesforce_records)

        expected_df = pd.DataFrame(
            {
                "facility_id": ["2", "1", "2"],
                "LastModifiedDate": ["2022-01-02", "2023-01-03", "2023-12-02"],
                "a": [2, 3, 4],
                "Calendar_Year__c": ["2022", "2022", "2023"],
            },
            index=[1, 2, 3],
        )
        expected_df["LastModifiedDate"] = pd.to_datetime(expected_df["LastModifiedDate"])

        pd.testing.assert_frame_equal(salesforce_records.df, expected_df)
        assert duplicate_ids == {"1": "2022"}

    def test_deduplicate_records_on_facility_id_keeps_modified_date_later_than_calendar_year(self, mocker):
        salesforce_records = mocker.Mock()
        salesforce_records.df = pd.DataFrame(
            {
                "facility_id": ["1", "1", "1"],
                "LastModifiedDate": ["2022-01-01", "2023-01-02", "2024-01-03"],
                "a": [1, 2, 3],
                "Calendar_Year__c": ["2022", "2023", "2023"],
            }
        )

        duplicate_ids = helpers.SalesForceRecords.deduplicate_records_on_facility_id(salesforce_records)

        expected_df = pd.DataFrame(
            {
                "facility_id": [
                    "1",
                    "1",
                ],
                "LastModifiedDate": ["2022-01-01", "2024-01-03"],
                "a": [1, 3],
                "Calendar_Year__c": ["2022", "2023"],
            },
            index=[0, 2],
        )
        expected_df["LastModifiedDate"] = pd.to_datetime(expected_df["LastModifiedDate"])

        pd.testing.assert_frame_equal(salesforce_records.df, expected_df)
        assert duplicate_ids == {"1": "2023"}
