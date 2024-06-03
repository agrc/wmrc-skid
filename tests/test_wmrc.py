import pandas as pd

from wmrc import main


def test_get_secrets_from_gcp_location(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')
    mocker.patch("google.auth.default", return_value=("sa", 42))

    secrets = main.Skid._get_secrets()

    assert secrets == {"foo": "bar", "SERVICE_ACCOUNT_JSON": "sa"}


def test_get_secrets_from_local_location(mocker):
    exists_mock = mocker.Mock(side_effect=[False, True])
    mocker.patch("pathlib.Path.exists", new=exists_mock)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main.Skid._get_secrets()

    assert secrets == {"foo": "bar"}
    assert exists_mock.call_count == 2


class TestUpdateMethods:

    def test_update_counties_merges_data_and_shapes(self, mocker):
        existing_data = pd.DataFrame(
            {
                "name": ["Box Elder County", "Out of State"],
                "col_a": [1, 2],
                "col_b": [3, 4],
                "SHAPE": ["shape1", "shape2"],
            }
        )
        mocker.patch("wmrc.main.transform.FeatureServiceMerging.get_live_dataframe", return_value=existing_data)
        updater_mock = mocker.patch("wmrc.main.load.FeatureServiceUpdater").return_value

        county_summaries = pd.DataFrame(
            {
                "data_year": [2022, 2022, 2023, 2023],
                "recycled": [1, 2, 3, 4],
                "landfilled": [5, 6, 7, 8],
                "total": [9, 10, 11, 12],
            },
            index=["Box Elder County", "Out of State", "Box Elder County", "Out of State"],
        )
        county_summaries.index.name = "name"
        mocker.patch("wmrc.main.pd.DataFrame.spatial")

        main.Skid._update_counties(mocker.Mock(), mocker.Mock(), county_summaries)

        test_df = pd.DataFrame(
            {
                "name": ["Box Elder County", "Box Elder County", "Out of State", "Out of State"],
                "data_year": [2022, 2023, 2022, 2023],
                "recycled": [1, 3, 2, 4],
                "landfilled": [5, 7, 6, 8],
                "total": [9, 11, 10, 12],
                "SHAPE": ["shape1", "shape1", "shape2", "shape2"],
            }
        )

        pd.testing.assert_frame_equal(updater_mock.truncate_and_load_features.call_args[0][0], test_df)


class TestCountyNamesMethod:

    def test_get_county_names_handles_empty_str_lat_long(self, mocker):
        spatial_mock = mocker.patch("wmrc.main.pd.DataFrame.spatial")
        mocker.patch("wmrc.main.arcgis")

        input_df = pd.DataFrame(
            {
                "foo": [1, 2, 3],
                "latitude": ["", -111.1, -111.2],
                "longitude": ["", 41.1, 41.2],
            }
        )

        main.Skid._get_county_names(input_df, mocker.Mock())

        df_without_empty = pd.DataFrame(
            {
                "foo": [2, 3],
                "latitude": [-111.1, -111.2],
                "longitude": [41.1, 41.2],
            },
            index=[1, 2],
        )
        df_without_empty["latitude"] = df_without_empty["latitude"].astype(object)
        df_without_empty["longitude"] = df_without_empty["longitude"].astype(object)

        # spatial_mock.from_xy.assert_called_once_with(df_without_empty, "longitude", "latitude")
        assert spatial_mock.from_xy.call_count == 1
        pd.testing.assert_frame_equal(spatial_mock.from_xy.call_args[0][0], df_without_empty)
