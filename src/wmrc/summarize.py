"""These functions generally apply functions from the helpers module to the records grouped by
Calender_Year__c to create dataframes of the reports that will be used to update the AGOL feature services.
"""

import pandas as pd


from . import helpers


def county_summaries(records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Perform the county summary per year analysis on the Salesforce records.

    Args:
        records (helpers.SalesForceRecords): Salesforce records loaded into a helper object

    Returns:
        pd.DataFrame: County summary report indexed by county name with data_year column as integer
    """

    county_df = records.df.groupby("Calendar_Year__c").apply(
        helpers.YearlyAnalysis.county_summaries, county_fields=records.county_fields
    )
    county_df.index.names = ["data_year", "name"]
    county_df.reset_index(level="data_year", inplace=True)
    county_df.rename(index={name: name.replace("__c", "").replace("_", " ") for name in county_df.index}, inplace=True)
    county_df["data_year"] = county_df["data_year"].apply(helpers.convert_to_int)
    county_df.fillna(0, inplace=True)

    return county_df


def facility_summaries(records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Perform the facility summary per year analysis on the Salesforce records.

    Args:
        records (helpers.SalesForceRecords): Salesforce records loaded into a helper object

    Returns:
        pd.DataFrame: Facilities summary report with default index and data_year column as integer
    """

    facility_summaries = (
        records.df.groupby("Calendar_Year__c")
        .apply(
            helpers.YearlyAnalysis.facility_tons_diverted_from_landfills,
        )
        .droplevel(1)
    )
    facility_summaries.index.name = "data_year"
    facility_summaries.reset_index(inplace=True)
    facility_summaries["data_year"] = facility_summaries["data_year"].apply(helpers.convert_to_int)

    return facility_summaries


def materials_recycled(records: helpers.SalesForceRecords) -> pd.DataFrame:
    """Perform the materials recycled analysis per year on the Salesforce records.

    Args:
        records (helpers.SalesForceRecords): Salesforce records loaded into a helper object. Relies on both df and
            field_mapping attributes.

    Returns:
        pd.DataFrame: Materials recycled report with default index and year_ column as integer
    """

    recycling_fields = [
        "Combined Total of Material Received",
        "Total Corrugated Boxes received",
        "Total Paper and Paperboard received",
        "Total Plastic Materials received",
        "Total Glass Materials received",
        "Total Ferrous Metal Materials received",
        "Total Aluminum Metal Materials received",
        "Total Nonferrous Metal received",
        "Total Rubber Materials received",
        "Total Leather Materials received",
        "Total Textile Materials received",
        "Total Wood Materials received",
        "Total Yard Trimmings received",
        "Total Food received",
        "Total Tires received",
        "Total Lead-Acid Batteries received",
        "Total Lithium-Ion Batteries received",
        "Total Other Electronics received",
        "Total ICD received",
        "Total SW Stream Materials received",
    ]
    renamed_fields = [records.field_mapping[field] for field in recycling_fields if field in records.field_mapping]
    materials_recycled = (
        records.df.groupby("Calendar_Year__c")
        .apply(
            helpers.YearlyAnalysis.rates_per_material,
            classification="Recycling",
            fields=renamed_fields,
            total_field="Combined_Total_of_Material_Received__c",
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
            helpers.YearlyAnalysis.rates_per_material,
            classification="Composts",
            fields=renamed_fields,
            total_field="Total_Material_Received_Compost__c",
        )
        .droplevel(1)
        .reset_index()
        .rename(columns={"Calendar_Year__c": "year_"})
    )
    materials_composted["year_"] = materials_composted["year_"].apply(helpers.convert_to_int)

    return materials_composted


def recovery_rates_by_tonnage(records: helpers.SalesForceRecords) -> pd.Series:
    """Calculates a yearly recovery rate based on the Salesforce records.

    recovery rate is opposite of contaminated rate (5% contamination = 95% uncontaminated). Rate is
    calculated by using the contamination rate to determine contaminated tonnage and comparing that to the total
    tonnage handled by facilities reporting a contamination rate.

    Args:
        records (helpers.SalesForceRecords): Helper object containing the Salesforce records

    Returns:
        pd.Series: recovery rates per year with index name data_year and series name
            "annual_recycling_uncontaminated_rate"
    """
    #: First, create a modifier to account for material from out-of-state
    records.df["in_state_modifier"] = (100 - records.df["Out_of_State__c"]) / 100

    #: Calculate contaminated tonnage
    records.df["recycling_tons_contaminated"] = (
        records.df["Annual_Recycling_Contamination_Rate__c"]
        / 100
        * records.df["Combined_Total_of_Material_Recycled__c"]
        * records.df["in_state_modifier"]
    )

    #: Calculate total tonnage from facilities reporting a contamination rate
    records.df["recycling_tons_report_contamination_total"] = pd.NA
    records.df.loc[~records.df["recycling_tons_contaminated"].isnull(), "recycling_tons_report_contamination_total"] = (
        records.df["Combined_Total_of_Material_Recycled__c"] * records.df["in_state_modifier"]
    )

    #: Invert to get uncontaminated rate
    clean_rates = records.df.groupby("Calendar_Year__c").apply(
        lambda year_df: (
            1
            - (
                year_df["recycling_tons_contaminated"].sum()
                / year_df["recycling_tons_report_contamination_total"].sum()
            )
        )
        * 100
    )
    clean_rates.name = "annual_recycling_uncontaminated_rate"
    clean_rates.index.name = "data_year"
    clean_rates.index = clean_rates.index.map(helpers.convert_to_int)

    return clean_rates
