import pandas as pd
from toolz import interleave

from wmrc import summarize, yearly
from wmrc.main import Skid


def state_year_over_year(county_df: pd.DataFrame, current_year: int) -> pd.DataFrame:
    """Calculates year-over-year comparison for the state as a whole using the county data as starting point.

    Args:
        county_df (pd.DataFrame): Dataframe with county data, indexed by year and county name. Must have columns for
            each metric to compare. Expects a "countywide_msw_recycling_rate" column (which will be dropped after
            computing statewide metrics).
        current_year (int): The year to compare changes from the previous year.

    Returns:
        pd.DataFrame: A single-row dataframe of each column's pct change, current year value, previous year value, and
            difference between current and previous year values.
    """

    state_metrics = county_df.groupby("data_year").apply(yearly.statewide_metrics).reset_index()
    state_metrics["name"] = "Statewide"
    state_metrics = state_metrics.set_index(["data_year", "name"])
    # state_metrics = state_metrics.drop(columns="statewide_msw_recycling_rate", axis=1)

    return _year_over_year_changes(state_metrics, current_year)


def county_year_over_year(county_df: pd.DataFrame, current_year: int) -> pd.DataFrame:
    """Calculate year-over-year comparison for each county.

    Args:
        county_df (pd.DataFrame): Dataframe with "data_year" and "name" columns. Must have columns for
            each metric to compare. Expects a "county_wide_msw_recycling_rate" column (which will be dropped).
        current_year (int): The year to compare changes from the previous year.

    Returns:
        pd.DataFrame: Each column's pct change, current year value, previous year value, and difference between current
            and previous year values, indexed by the county name.
    """

    county_summary_by_year = county_df.reset_index().set_index(["data_year", "name"])
    # county_summary_by_year = county_summary_by_year.drop(columns="county_wide_msw_recycling_rate", axis=1)

    return _year_over_year_changes(county_summary_by_year, current_year)


def facility_year_over_year(facility_df: pd.DataFrame, current_year: int) -> pd.DataFrame:
    """Calculate year-over-year comparison for for each facility (based on Facility ID).

    Args:
        facility_df (pd.DataFrame): Dataframe with facility data, indexed by year and facility ID/Name. Must have
            columns for each metric to compare.
        current_year (int): The year to compare changes from the previous year.

    Returns:
        pd.DataFrame: Each column's pct change, current year value, previous year value, and difference between current
            and previous year values, indexed by the facility ID.
    """

    facility_summary_by_year = facility_df.set_index(["data_year", "id", "name"])

    return _year_over_year_changes(facility_summary_by_year, current_year)


def _year_over_year_changes(metrics_df: pd.DataFrame, current_year: int) -> pd.DataFrame:
    """Calculate year-over-year changes for columns in a dataframe indexed by both year and another level for entity.

    Args:
        metrics_df (pd.DataFrame): Data to calculate year-over-year changes for. Must be indexed by year and at least
            one other level identifying the entity (facility, county, etc). Columns must be numeric metrics we want to
            compare year over year.
        current_year (int): The year to compare changes from the previous year.

    Raises:
        ValueError: If the current year or previous year are in the dataframe's index

    Returns:
        pd.DataFrame: Each column's pct change, current year value, previous year value, and difference between current and previous year values, indexed by the entity (or whatever was the other level in the input dataframe).
    """

    previous_year = current_year - 1
    if current_year not in metrics_df.index:
        raise ValueError(f"Current year {current_year} not found in index")
    if previous_year not in metrics_df.index:
        raise ValueError(f"Previous year {previous_year} not found in index")

    diffs = metrics_df.loc[current_year] - metrics_df.loc[previous_year]
    pct_change = diffs / metrics_df.loc[previous_year] * 100

    #: have to rename columns after pct_change so that the indices line up properly for the calculation
    diffs.columns = [f"{col}_diff" for col in diffs.columns]
    pct_change.columns = [f"{col}_pct_change" for col in pct_change.columns]

    values_current_year = metrics_df.loc[current_year].copy().reindex(index=diffs.index)
    values_current_year.columns = [f"{col}_{current_year}" for col in values_current_year.columns]

    values_previous_year = metrics_df.loc[previous_year].copy().reindex(index=diffs.index)
    values_previous_year.columns = [f"{col}_{previous_year}" for col in values_previous_year.columns]

    everything = pd.concat([pct_change, values_current_year, values_previous_year, diffs], axis=1)

    return everything[
        list(interleave([pct_change.columns, values_current_year.columns, values_previous_year.columns, diffs.columns]))
    ]


def run_validations():

    wmrc_skid = Skid()
    records = wmrc_skid._load_salesforce_data()
    _ = records.deduplicate_records_on_facility_id()
    facility_summary_df = summarize.facility_metrics(records)
    county_summary_df = summarize.counties(records)

    facility_changes = facility_year_over_year(facility_summary_df, 2023)
    county_changes = county_year_over_year(county_summary_df, 2023)
    state_changes = state_year_over_year(county_summary_df, 2023)

    county_changes.rename(
        columns={col: col.replace("county_wide_", "") for col in county_changes.columns}, inplace=True
    )
    state_changes.rename(columns={col: col.replace("statewide_", "") for col in state_changes.columns}, inplace=True)

    all_changes = pd.concat([facility_changes, county_changes, state_changes], axis=0)
    all_changes.to_csv(r"c:\gis\projects\wmrc\data\from_sf\validation_1.csv")


if __name__ == "__main__":
    run_validations()
