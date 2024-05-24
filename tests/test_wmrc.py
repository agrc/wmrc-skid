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

class TestSummaryMethods:

    def test_county_summaries_happy_path(self, mocker):
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

        result_df = main.Skid._county_summaries(records_mock)

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
