import pandas as pd
from toolz import interleave

try:
    from wmrc import summarize, yearly
    from wmrc.main import Skid
except ImportError:
    import summarize
    import yearly
    from main import Skid


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

    return _year_over_year_changes(county_summary_by_year, current_year)


def facility_year_over_year(
    facility_summary_df: pd.DataFrame, all_facility_records: pd.DataFrame, current_year: int
) -> pd.DataFrame:
    """Calculate year-over-year comparison for for each facility (based on Facility ID).

    Uses the facility summary data calculated from summarize & yearly modules to calculate year-over-year changes for
    the Recycling, Composting, Digestion, and landfill rates. The all facility records provided data for calculating
    year-over-year changes for things that don't need pre-analysis (percent msw and county shares).

    Args:
        facility_summary_df (pd.DataFrame): Dataframe with facility data, indexed by year and facility ID/Name. Must
            have columns for each metric to compare.
        all_facility_records (pd.DataFrame): Dataframe with all facility records from salesforce
        current_year (int): The year to compare changes from the previous year.

    Returns:
        pd.DataFrame: Each column's pct change, current year value, previous year value, and difference between current
            and previous year values, indexed by the facility ID.
    """

    #: all_facility_records still has salesforce-style column names, so we need to rename
    column_renaming = {
        "facility_id": "id",
        "Calendar_Year__c": "data_year",
        "Municipal_Solid_Waste__c": "percent_msw",
    }
    column_renaming.update({col: col.rstrip("__c") for col in all_facility_records.columns if "_County__c" in col})
    all_facility_records_renamed = all_facility_records.rename(columns=column_renaming)
    all_facility_records_renamed["data_year"] = all_facility_records_renamed["data_year"].astype(int)

    #: Subset to desired columns and merge with facility summary data
    all_facility_records_renamed = all_facility_records_renamed[column_renaming.values()]
    facility_summary_by_year = (
        facility_summary_df.set_index(["id", "data_year"])
        .merge(
            all_facility_records_renamed.set_index(["id", "data_year"]),
            left_index=True,
            right_index=True,
        )
        .reset_index()
        .set_index(["data_year", "id", "name"])
    )

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


# def run_validations():

#     base_year = 2023
#     report_path = r"c:\gis\projects\wmrc\data\from_sf\validation_2.csv"

#     #: Get records from salesforce, run summary methods
#     wmrc_skid = Skid()
#     records = wmrc_skid._load_salesforce_data()
#     _ = records.deduplicate_records_on_facility_id()
#     facility_summary_df = summarize.facility_metrics(records)
#     county_summary_df = summarize.counties(records)

#     #: Calc year-over-year changes
#     facility_changes = facility_year_over_year(facility_summary_df, records.df, base_year)
#     county_changes = county_year_over_year(county_summary_df, base_year)
#     state_changes = state_year_over_year(county_summary_df, base_year)

#     #: Remove county-wide and statewide prefixes so we can concat the different change dfs by row
#     county_changes.rename(
#         columns={col: col.replace("county_wide_", "") for col in county_changes.columns}, inplace=True
#     )
#     state_changes.rename(columns={col: col.replace("statewide_", "") for col in state_changes.columns}, inplace=True)

#     all_changes = pd.concat([facility_changes, county_changes, state_changes], axis=0)

#     #: Move the msw_recycling_rate columns to the front, write to csv
#     index_a = all_changes.columns.get_loc("msw_recycling_rate_pct_change")
#     slice_b = all_changes.columns.slice_indexer("msw_recycling_rate_pct_change", "msw_recycling_rate_diff")
#     index_c = all_changes.columns.get_loc("msw_recycling_rate_diff") + 1
#     new_index = all_changes.columns[slice_b].append([all_changes.columns[:index_a], all_changes.columns[index_c:]])

#     all_changes.reindex(columns=new_index).to_csv(report_path)


# if __name__ == "__main__":
#     run_validations()
