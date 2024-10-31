import pandas as pd

from wmrc import yearly


class TestYearlyMetrics:

    def test_county_wide_metrics_happy_path(self):
        facility_year_df = pd.DataFrame(
            {
                "Municipal_Solid_Waste__c": [50, 50],
                "Combined_Total_of_Material_Recycled__c": [10, 20],
                "Total_Materials_sent_to_composting__c": [0, 50],
                "Total_Material_managed_by_ADC__c": [10, 0],
                "Municipal_Waste_In_State_in_Tons__c": [80, 30],
                "Cache_County__c": [50, 50],
                "Utah_County__c": [50, 50],
            }
        )

        expected_output = pd.DataFrame(
            {
                "county_wide_msw_recycled": [7.5, 7.5, 15.0],
                "county_wide_msw_composted": [12.5, 12.5, 25.0],
                "county_wide_msw_digested": [2.5, 2.5, 5.0],
                "county_wide_msw_landfilled": [55.0, 55.0, 110.0],
                "county_wide_msw_diverted_total": [22.5, 22.5, 45.0],
                "county_wide_msw_recycling_rate": [
                    22.5 / (22.5 + 55.0) * 100,
                    22.5 / (22.5 + 55.0) * 100,
                    45.0 / (45.0 + 110.0) * 100,
                ],
            },
            index=["Cache_County__c", "Utah_County__c", "Statewide"],
        )

        output = yearly.county_summaries(facility_year_df, ["Cache_County__c", "Utah_County__c"])

        pd.testing.assert_frame_equal(output, expected_output)

    def test_statewide_metrics_happy_path(self):
        input_df = pd.DataFrame(
            {
                "county_wide_msw_recycled": [10, 10, 20],
                "county_wide_msw_composted": [0, 0, 50],
                "county_wide_msw_digested": [10, 0, 0],
                "county_wide_msw_landfilled": [80, 90, 30],
                "county_wide_msw_diverted_total": [20, 10, 70],
            },
            index=["foo", "bar", "baz"],
        )

        expected_output = pd.Series(
            {
                "statewide_msw_recycled": 40,
                "statewide_msw_composted": 50,
                "statewide_msw_digested": 10,
                "statewide_msw_landfilled": 200,
                "statewide_msw_diverted_total": 100,
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
                "statewide_msw_diverted_total": 100,
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
                "msw_recycling_rate": [2 / 82 * 100, 5 / 95 * 100, 70 / 100 * 100],
            }
        )

        output = yearly.facility_combined_metrics(input_df)

        pd.testing.assert_frame_equal(output, expected_output)


class TestRatesPerMaterial:

    def test_update_classification_sets_recycling_classifications(self):
        output = yearly._update_classification("Recycling")

        assert output == ["Recycling", "Recycling Facility Non-Permitted"]

    def test_update_classification_sets_composting_classifications(self):
        output = yearly._update_classification("Composts")

        assert output == ["Composts"]

    def test_update_fields_moves_msw_to_end(self):
        output = yearly._update_fields(["foo", "Municipal_Solid_Waste__c", "bar", "Out_of_State__c"])

        assert output == ["foo", "bar", "Out_of_State__c", "Municipal_Solid_Waste__c"]

    def test_update_fields_adds_msw(self):
        output = yearly._update_fields(["foo", "bar", "Out_of_State__c"])

        assert output == ["foo", "bar", "Out_of_State__c", "Municipal_Solid_Waste__c"]

    def test_update_fields_adds_out_of_state(self):
        output = yearly._update_fields(["foo", "bar", "Municipal_Solid_Waste__c"])

        assert output == ["foo", "bar", "Out_of_State__c", "Municipal_Solid_Waste__c"]

    def test_update_fields_adds_both(self):
        output = yearly._update_fields(["foo", "bar"])

        assert output == ["foo", "bar", "Out_of_State__c", "Municipal_Solid_Waste__c"]

    def test_update_fields_reorders_both(self):
        output = yearly._update_fields(["foo", "Municipal_Solid_Waste__c", "Out_of_State__c", "bar"])

        assert output == ["foo", "bar", "Out_of_State__c", "Municipal_Solid_Waste__c"]

    def test_rates_per_material_sums_properly_no_modifiers(self):
        year_df = pd.DataFrame(
            {
                "Classifications__c": ["Recycling", "Recycling"],
                "Total_Corrugated_Boxes_received__c": [10, 10],
                "Municipal_Solid_Waste__c": [100, 100],
                "Out_of_State__c": [0, 0],
                "Combined_Total_of_Material_Received__c": [10, 10],
            }
        )

        expected_output = pd.DataFrame(
            {
                "material": ["Combined Total of Material Received", "Corrugated Boxes"],
                "amount": [20.0, 20.0],
                "percent": [1.0, 1.0],
            },
        )

        output = yearly.rates_per_material(
            year_df,
            classification="Recycling",
            fields=["Combined_Total_of_Material_Received__c", "Total_Corrugated_Boxes_received__c"],
            total_field="Combined_Total_of_Material_Received__c",
        )

        pd.testing.assert_frame_equal(output, expected_output)

    def test_rates_per_material_sums_properly_with_msw_modifier(self):
        year_df = pd.DataFrame(
            {
                "Classifications__c": ["Recycling", "Recycling"],
                "Total_Corrugated_Boxes_received__c": [10, 10],
                "Municipal_Solid_Waste__c": [50, 50],
                "Out_of_State__c": [0, 0],
                "Combined_Total_of_Material_Received__c": [10, 10],
            }
        )

        expected_output = pd.DataFrame(
            {
                "material": ["Combined Total of Material Received", "Corrugated Boxes"],
                "amount": [10.0, 10.0],
                "percent": [1.0, 1.0],
            },
        )

        output = yearly.rates_per_material(
            year_df,
            classification="Recycling",
            fields=["Combined_Total_of_Material_Received__c", "Total_Corrugated_Boxes_received__c"],
            total_field="Combined_Total_of_Material_Received__c",
        )

        pd.testing.assert_frame_equal(output, expected_output)

    def test_rates_per_material_sums_properly_with_out_of_state_modifier(self):
        year_df = pd.DataFrame(
            {
                "Classifications__c": ["Recycling", "Recycling"],
                "Total_Corrugated_Boxes_received__c": [10, 10],
                "Municipal_Solid_Waste__c": [100, 100],
                "Out_of_State__c": [50, 50],
                "Combined_Total_of_Material_Received__c": [10, 10],
            }
        )

        expected_output = pd.DataFrame(
            {
                "material": ["Combined Total of Material Received", "Corrugated Boxes"],
                "amount": [10.0, 10.0],
                "percent": [1.0, 1.0],
            },
        )

        output = yearly.rates_per_material(
            year_df,
            classification="Recycling",
            fields=["Combined_Total_of_Material_Received__c", "Total_Corrugated_Boxes_received__c"],
            total_field="Combined_Total_of_Material_Received__c",
        )

        pd.testing.assert_frame_equal(output, expected_output)

    def test_rates_per_material_sums_properly_with_both_modifiers(self):
        year_df = pd.DataFrame(
            {
                "Classifications__c": ["Recycling", "Recycling"],
                "Total_Corrugated_Boxes_received__c": [10, 10],
                "Municipal_Solid_Waste__c": [50, 50],
                "Out_of_State__c": [50, 50],
                "Combined_Total_of_Material_Received__c": [10, 10],
            }
        )

        expected_output = pd.DataFrame(
            {
                "material": ["Combined Total of Material Received", "Corrugated Boxes"],
                "amount": [5.0, 5.0],
                "percent": [1.0, 1.0],
            },
        )

        output = yearly.rates_per_material(
            year_df,
            classification="Recycling",
            fields=["Combined_Total_of_Material_Received__c", "Total_Corrugated_Boxes_received__c"],
            total_field="Combined_Total_of_Material_Received__c",
        )

        pd.testing.assert_frame_equal(output, expected_output)

    def test_rates_per_material_sums_properly_with_both_modifiers_composting(self):
        year_df = pd.DataFrame(
            {
                "Classifications__c": ["Composts", "Composts"],
                "Total_Agricultural_Organics_received__c": [10, 10],
                "Municipal_Solid_Waste__c": [50, 50],
                "Out_of_State__c": [50, 50],
                "Total_Material_Received_Compost__c": [10, 10],
            }
        )

        expected_output = pd.DataFrame(
            {
                "material": ["Total Material Received Compost", "Agricultural Organics"],
                "amount": [5.0, 5.0],
                "percent": [1.0, 1.0],
            },
        )

        output = yearly.rates_per_material(
            year_df,
            classification="Composts",
            fields=["Total_Material_Received_Compost__c", "Total_Agricultural_Organics_received__c"],
            total_field="Total_Material_Received_Compost__c",
        )

        pd.testing.assert_frame_equal(output, expected_output)
