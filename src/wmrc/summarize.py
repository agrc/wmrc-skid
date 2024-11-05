"""These functions generally apply functions from the yearly module to the records grouped by
Calender_Year__c to create dataframes of the reports that will be used to update the AGOL feature services.
"""

import numpy as np
import pandas as pd

try:
    from wmrc import helpers, yearly
except ImportError:
    import helpers
    import yearly


def counties(records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Perform the county summary per year analysis on the Salesforce records.

    Args:
        records (helpers.SalesForceRecords): Salesforce records loaded into a helper object

    Returns:
        pd.DataFrame: County summary report indexed by county name with data_year column as integer
    """

    county_df = records.df.groupby("Calendar_Year__c").apply(
        yearly.county_summaries, county_fields=records.county_fields
    )
    county_df.index.names = ["data_year", "name"]
    county_df.reset_index(level="data_year", inplace=True)
    county_df.rename(index={name: name.replace("__c", "").replace("_", " ") for name in county_df.index}, inplace=True)
    county_df["data_year"] = county_df["data_year"].apply(helpers.convert_to_int)
    county_df.fillna(0, inplace=True)

    return county_df


def facilities(records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Perform the facility summary per year analysis on the Salesforce records.

    Args:
        records (helpers.SalesForceRecords): Salesforce records loaded into a helper object

    Returns:
        pd.DataFrame: Facilities summary report with default index and data_year column as integer
    """

    facility_summaries = (
        records.df.groupby("Calendar_Year__c")
        .apply(
            yearly.facility_tons_diverted_from_landfills,
        )
        .droplevel(1)
    )
    facility_summaries.index.name = "data_year"
    facility_summaries.reset_index(inplace=True)
    facility_summaries["data_year"] = facility_summaries["data_year"].apply(helpers.convert_to_int)
    facility_summaries = _add_facility_info(facility_summaries, records)

    return facility_summaries


def _add_facility_info(facility_df: pd.DataFrame, records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Add facility information to the facility summary based on the record with the most recent LastModifiedDate.

    Args:
        facility_df (pd.DataFrame): Facility summary report with LastModifiedDate as datetime
        records (helpers.SalesForceRecords): Salesforce records loaded into a helper object

    Returns:
        pd.DataFrame: Facility summary report with facility information added
    """

    latest_records = records.df.loc[records.df.groupby("facility_id")["LastModifiedDate"].idxmax()].reset_index()[
        [
            "facility_id",
            "Are_materials_accepted_for_drop_off__c",
            "Facility_Phone_Number__c",
            "Facility_Website__c",
            "Facility_Name__c",
        ]
    ]

    latest_records["id_"] = latest_records["facility_id"].astype(str).str[3:].str.lstrip("0")
    latest_records.drop(columns="facility_id", inplace=True)
    latest_records.rename(
        columns={
            "Are_materials_accepted_for_drop_off__c": "accept_material_dropped_off_by_",
            "Facility_Phone_Number__c": "phone_no_",
            "Facility_Website__c": "website",
            "Facility_Name__c": "facility_name",
        },
        inplace=True,
    )

    #: Drop Facility_Name__c because it gets carried over in the summary report from groupby'ing on both it and id
    facility_df = facility_df.drop(columns=["Facility_Name__c"]).merge(
        latest_records,
        left_on="id_",
        right_on="id_",
        how="left",
    )

    return facility_df


def materials_recycled(records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Perform the materials recycled analysis per year on the Salesforce records.

    Args:
        records (helpers.SalesForceRecords): Salesforce records loaded into a helper object. Relies on both df and
            field_mapping attributes.

    Returns:
        pd.DataFrame: Materials recycled report with default index and year_ column as integer
    """

    recycling_fields = [
        "Combined Total of Material Recycled",
        "Total Paper Materials recycled",
        "Total Plastic Materials recycled",
        "Total Glass Materials recycled",
        "Total Metal Materials recycled",
        "Total Rubber Materials recycled",
        "Total Leather Materials recycled",
        "Total Textile Materials recycled",
        "Total Wood Materials recycled",
        "Total Tires recycled",
        "Total Electronics recycled",
        "Total ICD recycled",
        "Total SW Stream Materials recycled",
    ]
    renamed_fields = [records.field_mapping[field] for field in recycling_fields if field in records.field_mapping]
    materials_recycled = (
        records.df.groupby("Calendar_Year__c")
        .apply(
            yearly.rates_per_material,
            classification="Recycling",
            fields=renamed_fields,
            total_field="Combined_Total_of_Material_Recycled__c",
        )
        .droplevel(1)
        .reset_index()
        .rename(columns={"Calendar_Year__c": "year_"})
    )
    materials_recycled["year_"] = materials_recycled["year_"].apply(helpers.convert_to_int)

    return materials_recycled


def materials_composted(records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Perform the materials composted per year analysis on the Salesforce records.

    Args:
        records (helpers.SalesForceRecords): Helper object containing the Salesforce records. Relies on both df and
            field_mapping attributes.

    Returns:
        pd.DataFrame: Materials composted report with default index and year_ column as integer
    """

    composting_fields = [
        "Municipal Solid Waste",
        "Total Material Received Compost",
        "Total Paper and Paperboard receiced (C)",
        "Total Plastic Materials received (C)",
        "Total Rubber Materials received (C)",
        "Total Leather Materials received (C)",
        "Total Textile Materials received (C)",
        "Total Wood Materials received (C)",
        "Total Yard Trimmings received (C)",
        "Total Food received (C)",
        "Total Agricultural Organics received",
        "Total BFS received",
        "Total Drywall received",
        "Total Other CM received",
    ]
    renamed_fields = [records.field_mapping[field] for field in composting_fields if field in records.field_mapping]
    materials_composted = (
        records.df.groupby("Calendar_Year__c")
        .apply(
            yearly.rates_per_material,
            classification="Composts",
            fields=renamed_fields,
            total_field="Total_Material_Received_Compost__c",
        )
        .droplevel(1)
        .reset_index()
        .rename(columns={"Calendar_Year__c": "year_"})
    )
    materials_composted["year_"] = materials_composted["year_"].apply(helpers.convert_to_int)
    materials_composted["material"] = materials_composted["material"].replace(
        {"BFS": "Biosolids, Food Processing Residuals, and Sewage Sludge"}
    )

    return materials_composted


def recovery_rates_by_tonnage(records: helpers.SalesForceRecords) -> pd.Series:
    """Calculates a yearly recovery rate based on the Salesforce records.

    Recovery rate is opposite of contaminated rate (5% contamination = 95% uncontaminated). Rate is
    calculated by calculating the total in-state MSW recycled per facility and the total received, which comes from
    dividing that amount by the recovery rate per facility, and then dividing the sums of those two values across all
    facilities.

    Args:
        records (helpers.SalesForceRecords): Helper object containing the Salesforce records

    Returns:
        pd.Series: recovery rates per year with index name data_year and series name
            "annual_recycling_uncontaminated_rate"
    """
    #: Create our various modifiers
    records.df["in_state_modifier"] = (100 - records.df["Out_of_State__c"]) / 100
    records.df["msw_modifier"] = records.df["Municipal_Solid_Waste__c"] / 100
    records.df["recovery_rate"] = (100 - records.df["Annual_Recycling_Contamination_Rate__c"]) / 100

    #: Amount of material recycled
    records.df["in_state_msw_recycled"] = (
        records.df["Combined_Total_of_Material_Recycled__c"]
        * records.df["in_state_modifier"]
        * records.df["msw_modifier"]
    )

    #: Amount of material received derived from recovery rate
    records.df["in_state_msw_received_for_recycling"] = (
        records.df["in_state_msw_recycled"] / records.df["recovery_rate"]
    )

    #: Uncontaminated rates by year
    clean_rates = records.df.groupby("Calendar_Year__c").apply(
        lambda year_df: (
            year_df["in_state_msw_recycled"].sum() / year_df["in_state_msw_received_for_recycling"].sum() * 100
        )
    )

    clean_rates.replace([np.inf, -np.inf], np.nan, inplace=True)  #: Can arise from division by np.nan
    clean_rates.name = "annual_recycling_uncontaminated_rate"
    clean_rates.index.name = "data_year"
    clean_rates.index = clean_rates.index.map(helpers.convert_to_int)

    return clean_rates


def facility_metrics(records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Get the recycled, composting, digested, and landfilled (RCDL) numbers for each facility grouped by year.

    Args:
        records (helpers.SalesForceRecords): Salesforce records loaded into a helper object

    Returns:
        pd.DataFrame: RCDL metrics for each facility with data_year column as integer
    """

    facility_metrics = (
        records.df.groupby("Calendar_Year__c")
        .apply(
            yearly.facility_combined_metrics,
        )
        .droplevel(1)
    )
    facility_metrics.index.name = "data_year"
    facility_metrics.reset_index(inplace=True)
    facility_metrics["data_year"] = facility_metrics["data_year"].apply(helpers.convert_to_int)

    return facility_metrics
