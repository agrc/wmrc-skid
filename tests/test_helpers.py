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
