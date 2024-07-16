import pandas as pd
from toolz import interleave


def county_year_over_year(county_df: pd.DataFrame, data_year: int) -> pd.DataFrame:

    return county_df


def facility_year_over_year(facility_df: pd.DataFrame, current_year: int) -> pd.DataFrame:

    facility_summary_by_year = facility_df.set_index(["data_year", "id", "name"])

    return _year_over_year_changes(facility_summary_by_year, current_year)


def _year_over_year_changes(rcdl_df, current_year):

    #: rcdl_df should be a multi-index dataframe with year and at least one other level identifying the entity (facility, county, etc)

    previous_year = current_year - 1
    if current_year not in rcdl_df.index:
        raise ValueError(f"Current year {current_year} not found in index")
    if previous_year not in rcdl_df.index:
        raise ValueError(f"Previous year {previous_year} not found in index")

    diffs = rcdl_df.loc[current_year] - rcdl_df.loc[previous_year]
    pct_change = diffs / rcdl_df.loc[previous_year] * 100

    #: have to rename columns after pct_change so that the indices line up properly for the calculation
    diffs.columns = [f"{col}_diff" for col in diffs.columns]
    pct_change.columns = [f"{col}_pct_change" for col in pct_change.columns]

    values_current_year = rcdl_df.loc[current_year].copy().reindex(index=diffs.index)
    values_current_year.columns = [f"{col}_{current_year}" for col in values_current_year.columns]

    values_previous_year = rcdl_df.loc[previous_year].copy().reindex(index=diffs.index)
    values_previous_year.columns = [f"{col}_{previous_year}" for col in values_previous_year.columns]

    everything = pd.concat([pct_change, values_current_year, values_previous_year, diffs], axis=1)

    return everything[
        list(interleave([pct_change.columns, values_current_year.columns, values_previous_year.columns, diffs.columns]))
    ]
