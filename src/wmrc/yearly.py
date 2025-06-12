"""These functions calculate metrics for a given year, usually applied to a groupby(year) object"""

import re

import numpy as np
import pandas as pd


def county_summaries(year_df: pd.DataFrame, county_fields: list[str]) -> pd.DataFrame:
    """Calculate the county-wide summaries for Municipal Solid Waste (MSW) over time.

    Designed to be run on a yearly groupby object. Calculates the totals based on the following formulas:
        - recycling tons: county % * MSW/100 * Combined Total of Material Recycled
        - composted tons: county % * MSW/100 * Total Materials sent to composting
        - digested tons: county % * MSW/100 * Total Material managed by AD/C
        - landfilled tons: county % * Municipal Waste In-State (in Tons)
        - recycling rate: (recycling + composted + digested) / (recycling + composted + digested + landfilled) * 100

    County % is the amount of a given record's totals that apply to the given county. MSW/100 is a modifier to
    isolate the materials reported by the facility that are MSW instead of construction debris, etc.

    Args:
        year_df (pd.DataFrame): A dataframe of facility records for a single year (can be .applied to a groupby
            (year) object). Columns include percentages for each county and the fields needed for the calculations
        county_fields (list[str]): List county field names

    Returns:
        pd.DataFrame: A dataframe of tons recycled, composted, digested, and landfilled for each county along with
            overall recycling rate
    """

    #: Create new dataframes that have a column for each county, one dataframe per category
    recycling_df = pd.DataFrame()
    composted_df = pd.DataFrame()
    digested_df = pd.DataFrame()
    landfilled_df = pd.DataFrame()

    #: MSW modifier is the percentage of the facility's materials that are MSW instead of construction debris, etc.
    year_df["msw_modifier"] = year_df["Municipal_Solid_Waste__c"] / 100

    #: Calculate the tons per county for each category
    for county in county_fields:
        recycling_df[county] = (
            year_df[county] / 100 * year_df["msw_modifier"] * year_df["Combined_Total_of_Material_Recycled__c"]
        )
        composted_df[county] = (
            year_df[county] / 100 * year_df["msw_modifier"] * year_df["Total_Materials_sent_to_composting__c"]
        )
        digested_df[county] = (
            year_df[county] / 100 * year_df["msw_modifier"] * year_df["Total_Material_managed_by_ADC__c"]
        )
        landfilled_df[county] = year_df[county] / 100 * year_df["Municipal_Waste_In_State_in_Tons__c"]

    #: Now sum all the counties to get a single value per county per category
    counties_df = pd.DataFrame()
    counties_df["county_wide_msw_recycled"] = recycling_df.sum()
    counties_df["county_wide_msw_composted"] = composted_df.sum()
    counties_df["county_wide_msw_digested"] = digested_df.sum()
    counties_df["county_wide_msw_landfilled"] = landfilled_df.sum()
    statewide = counties_df.sum()
    statewide.name = "Statewide"
    counties_df = pd.concat([counties_df, pd.DataFrame(statewide).T], axis=0)

    counties_df["county_wide_msw_diverted_total"] = (
        counties_df["county_wide_msw_recycled"]
        + counties_df["county_wide_msw_composted"]
        + counties_df["county_wide_msw_digested"]
    )
    counties_df["county_wide_msw_recycling_rate"] = (
        counties_df["county_wide_msw_diverted_total"]
        / (counties_df["county_wide_msw_diverted_total"] + counties_df["county_wide_msw_landfilled"])
        * 100
    )

    return counties_df


def facility_tons_diverted_from_landfills(year_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the total tonnage of material diverted from landfills for each facility.

    Tons diverted = Combined Total of Material Recycled + Total Materials recycled + Total Materials sent to
    composting + Combined Total Material for Composting +Total Material managed by AD/C + Combined Total Material
    for Combustion + Total Materials combusted + Total waste tires recycled (in Tons) + Total WT for combustion (in
    Tons)

    Args:
        year_df (pd.DataFrame): Dataframe of facility records for a single year (can be .applied to a groupby
            year)).

    Returns:
        pd.DataFrame: Facility name, id, and total tons diverted from landfills
    """

    fields = [
        "Facility_Name__c",
        "facility_id",
        "Combined_Total_of_Material_Recycled__c",
        "Total_Materials_recycled__c",
        "Total_Materials_sent_to_composting__c",
        "Combined_Total_Material_for_Compostion__c",
        "Total_Material_managed_by_ADC__c",
        "Combined_Total_Material_for_Combustion__c",
        "Total_Materials_combusted__c",
        "Total_waste_tires_recycled_in_Tons__c",
        "Total_WT_for_combustion_in_Tons__c",
    ]
    subset_df = year_df[fields].copy()

    #: Sum any duplicate records for a single facility
    #: NOTE: May be necessary now that records are deduplicated, leave for now
    sum_df = subset_df.groupby(["Facility_Name__c", "facility_id"]).sum().reset_index()

    sum_df["tons_of_material_diverted_from_"] = (
        sum_df["Combined_Total_of_Material_Recycled__c"]
        + sum_df["Total_Materials_recycled__c"]
        + sum_df["Total_Materials_sent_to_composting__c"]
        + sum_df["Combined_Total_Material_for_Compostion__c"]
        + sum_df["Total_Material_managed_by_ADC__c"]
        + sum_df["Combined_Total_Material_for_Combustion__c"]
        + sum_df["Total_Materials_combusted__c"]
        + sum_df["Total_waste_tires_recycled_in_Tons__c"]
        + sum_df["Total_WT_for_combustion_in_Tons__c"]
    )

    #: Include recycling facility recycling totals
    sum_df["tons_recycled_at_recycle_fac"] = sum_df["Combined_Total_of_Material_Recycled__c"]

    #: Extract just the number part of the facility id, strip leading zeros
    sum_df["id_"] = sum_df["facility_id"].astype(str).str[3:].str.lstrip("0")

    #: Replace 0s with NaN for AGOL/Arcade logic (want to identify missing data as such, not as 0s)
    sum_df["tons_of_material_diverted_from_"] = sum_df["tons_of_material_diverted_from_"].replace(0, np.nan)
    sum_df["tons_recycled_at_recycle_fac"] = sum_df["tons_recycled_at_recycle_fac"].replace(0, np.nan)

    return sum_df[["Facility_Name__c", "id_", "tons_of_material_diverted_from_", "tons_recycled_at_recycle_fac"]]


def rates_per_material(year_df: pd.DataFrame, classification: str, fields: list[str], total_field: str) -> pd.DataFrame:
    """Calculate recycling/composting rates for each material type for a given year.

    Args:
        year_df (pd.DataFrame): Dataframe of facility records for a single year (can be .applied to a groupby(year)
            object).
        classification (str): Report Classification, either "Recycling" or "Composts"
        fields (list[str]): List of the fields containing the material totals.
        total_field (str): The field containing the total material received for the percentage calculation.

    Returns:
        pd.DataFrame: Renamed material types, total tonnage processed, and percent processed
    """

    #: Make sure the out-of-state and MSW modifier fields are the last two fields
    needed_fields = _update_fields(fields)

    #: Update: Recycling should also include "Recycling Facility Non-Permitted"
    classification = _update_classification(classification)

    subset_df = year_df[year_df["Classifications__c"].isin(classification)][needed_fields]


    #: Sum totals across all records taking into account MSW and out-of-state modifiers, calculate total percentage

    #: NOTE: if either out of state or MSW is null, that row comes out to 0
    #: All year 2023 data have an out-of-state value, but only one 2024 does
    #: Fixing by filling NaNs with 0s
    subset_df.fillna(0, inplace=True)

    sum_series = pd.Series()
    for col in needed_fields[:-2]:  #: We don't want sum up the raw MSW or out-of-state modifier values
        sum_series[col] = (
            (100 - subset_df["Out_of_State__c"]) / 100 * subset_df["Municipal_Solid_Waste__c"] / 100 * subset_df[col]
        ).sum()
    sum_df = pd.DataFrame(sum_series, columns=["amount"])
    sum_df["percent"] = sum_df["amount"] / sum_df.loc[total_field, "amount"]

    #: Rename columns for existing AGOL layer
    regex = re.compile(r"(?<=Total_)(.+)((?=_recei)|((?=_recycled)|(?=Materials_recycled)))")
    sum_df.reset_index(names="material", inplace=True)
    sum_df["material"] = (
        sum_df["material"]
        .apply(lambda x: re.search(regex, x)[0] if re.search(regex, x) else x)
        .str.removesuffix("_Materials")
        .str.replace("__c", "")
        .str.replace("_", " ")
        .str.replace(" CM", " Compostable Material")
        .str.replace("SW Stream", "Other Solid Waste Stream Materials")
        .str.replace("Paper", "Paper and Paperboard")
        .str.replace("ICD", "Industrial, Commercial, and Demolition Materials")
    )

    return sum_df


def _update_fields(fields: list[str]) -> list[str]:
    """Ensures that the Out of state percentage and Municipal Solid Waste fields are the last two fields in the list."""
    try:
        fields.remove("Out_of_State__c")
    except ValueError:
        pass
    fields.append("Out_of_State__c")

    try:
        fields.remove("Municipal_Solid_Waste__c")
    except ValueError:
        pass
    fields.append("Municipal_Solid_Waste__c")

    return fields


def _update_classification(classification: str) -> list[str]:
    """Make classification a list, ensure recycling includes non-permitted facilities."""
    if classification == "Recycling":
        classification = ["Recycling", "Recycling Facility Non-Permitted"]
    if classification == "Composts":
        classification = ["Composts"]
    return classification


def statewide_metrics(county_year_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate statewide yearly metrics for recycling, composting, digestion, and landfilling (RCDL), filtering
    out out of state totals.

    Args:
        county_year_df (pd.DataFrame): Dataframe of county summaries for a given year with the RCDL metrics (can be
            applied to a groupby (year) object).

    Returns:
        pd.DataFrame: Statewide yearly metrics.
    """

    in_state_only = county_year_df.drop(index=["Out of State", "Statewide"], errors="ignore")

    statewide_series = pd.Series()
    statewide_series["statewide_msw_recycled"] = in_state_only["county_wide_msw_recycled"].sum()
    statewide_series["statewide_msw_composted"] = in_state_only["county_wide_msw_composted"].sum()
    statewide_series["statewide_msw_digested"] = in_state_only["county_wide_msw_digested"].sum()
    statewide_series["statewide_msw_landfilled"] = in_state_only["county_wide_msw_landfilled"].sum()
    statewide_series["statewide_msw_diverted_total"] = (
        statewide_series["statewide_msw_recycled"]
        + statewide_series["statewide_msw_composted"]
        + statewide_series["statewide_msw_digested"]
    )
    statewide_series["statewide_msw_recycling_rate"] = (
        statewide_series["statewide_msw_diverted_total"]
        / (statewide_series["statewide_msw_diverted_total"] + statewide_series["statewide_msw_landfilled"])
        * 100
    )

    return statewide_series


def facility_combined_metrics(year_df: pd.DataFrame) -> pd.DataFrame:
    """Get the recycled, composting, digested, and landfilled (RCDL) tons for each facility.

    Args:
        year_df (pd.DataFrame): Dataframe of facility records for a single year (can be applied to a groupby(year)
            object).

    Returns:
        pd.DataFrame: Facility id, name, and tons of material recycled, composted, digested, and landfilled.
    """

    msw_modifier = year_df["Municipal_Solid_Waste__c"] / 100

    stats_df = pd.DataFrame()
    stats_df["id"] = year_df["facility_id"]
    stats_df["name"] = year_df["Facility_Name__c"]
    stats_df["msw_recycled"] = msw_modifier * year_df["Combined_Total_of_Material_Recycled__c"]
    stats_df["msw_composted"] = msw_modifier * year_df["Total_Materials_sent_to_composting__c"]
    stats_df["msw_digested"] = msw_modifier * year_df["Total_Material_managed_by_ADC__c"]
    stats_df["msw_landfilled"] = year_df["Municipal_Waste_In_State_in_Tons__c"]
    stats_df["msw_recycling_rate"] = (
        (stats_df["msw_recycled"] + stats_df["msw_composted"] + stats_df["msw_digested"])
        / (stats_df["msw_recycled"] + stats_df["msw_composted"] + stats_df["msw_digested"] + stats_df["msw_landfilled"])
        * 100
    )
    return stats_df
