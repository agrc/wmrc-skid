import re
from typing import Mapping

import pandas as pd
from palletjack import extract


def extract_data_from_salesforce(client_secret, client_id, org):
    """Load data from Salesforce into a dataframe

    Builds a string of needed column names for our specific needs and uses that in the REST query. Only loads reports
    that have a Status of 'Submitted' and a RecordType of 'Annual Report.'

    Args:
        client_secret (str): Client secret for Salesforce API
        client_id (str): Client ID for Salesforce API
        org (str): The Salesforce org to connect to

    Returns:
        pd.DataFrame: The data from Salesforce in a dataframe with just our needed fields
    """

    salesforce_credentials = extract.SalesforceApiUserCredentials(client_secret, client_id)
    salesforce_extractor = extract.SalesforceRestLoader(org, salesforce_credentials)

    #: Get a dataframe of the first 200 rows with all the columns so we can extract the needed columns
    df_for_columns = salesforce_extractor.get_records(
        "services/data/v60.0/query/", "SELECT FIELDS(ALL) from Application_Report__c LIMIT 200"
    )
    county_fields = [col for col in df_for_columns.columns if "_County" in col]
    fields_string = _build_columns_string(df_for_columns, county_fields)

    #: Main query with just our desired fields
    request_df = salesforce_extractor.get_records(
        "services/data/v60.0/query/",
        f"SELECT {fields_string} from Application_Report__c WHERE Status__c = 'Submitted' AND RecordType.Name = 'Annual Report'",
    )

    #: Extract the facility id from the nested salesforce object
    request_df["facility_id"] = request_df["Facility__r"].apply(lambda x: x["Solid_Waste_Facility_ID_Number__c"])

    return request_df


def _build_columns_string(field_mapping: Mapping[str, str], county_fields) -> str:
    """Build a string of needed columns for the SOQL query based on field mapping and some custom fields

    Args:
        field_mapping (Mapping[str, str]): Dictionary of manual report names to Salesforce field names

    Returns:
        str: A comma-delimited string of needed columns for the SOQL query
    """

    #: Fix typo in fields
    field_mapping["Combined Total Material for Composting"] = "Combined_Total_Material_for_Compostion__c"

    fields_string = ",".join(field_mapping.values())
    fields_string += ",RecordTypeId,Classifications__c,RecordType.Name,Facility__r.Solid_Waste_Facility_ID_Number__c"
    fields_string += "," + ",".join(county_fields)

    return fields_string


def _build_field_mapping(all_columns_df: pd.DataFrame) -> Mapping[str, str]:
    """Map names from manual reports to Salesforce field names

    Args:
        all_columns_df (pd.DataFrame): A dataframe from Salesforce that has all the available columns

    Raises:
        ValueError: If a field from the manual reports is not found in the Salesforce columns

    Returns:
        Mapping[str, str]: Dictionary of manual report names to Salesforce field names
    """

    aliases = [
        "Combined Total of Material Recycled",
        "Municipal Solid Waste",
        "Total Materials sent to composting",
        "Total Material managed by AD/C",
        "Municipal Waste In-State (in Tons)",
        "Facility Name",
        "Solid Waste Facility ID Number",
        "Combined Total of Material Recycled",
        "Total Materials recycled",
        "Total Materials sent to composting",
        "Combined Total Material for Composting",
        "Total Material managed by AD/C",
        "Combined Total Material for Combustion",
        "Total Materials combusted",
        "Total waste tires recycled (in Tons)",
        "Total WT for combustion (in Tons)",
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
        "Calendar Year",
    ]
    field_mapping = {}
    missing_fields = []
    for alias in aliases:
        if alias in field_mapping:
            continue
        field_name = (
            f'{alias.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace("/", "")}__c'
        )
        if field_name not in all_columns_df.columns:
            missing_fields.append(alias)
            continue
        field_mapping[alias] = field_name

    if missing_fields:
        raise ValueError(f"Missing fields: {missing_fields}")
    return field_mapping


def county_summaries(year_df, county_fields):
    """Calculate the county-wide summaries for Municipal Solid Waste (MSW) over time.

    Designed to be run on a yearly groupby object. Calculates the totals based on the following formulas:
        - recycling tons: county % * MSW/100 * Combined Total of Material Recycled
        - composted tons: county % * MSW/100 * Total Materials sent to composting
        - digested tons: county % * MSW/100 * Total Material managed by AD/C
        - landfilled tons: county % * Municipal Waste In-State (in Tons)
        - recycling rate: (recycling + composted + digested) / (recycling + composted + digested + landfilled) * 100

    County % is the amount of a given record's totals that apply to the given county. MSW/100 is a modifier to isolate the materials reported by the facility that are MSW instead of construction debris, etc.

    Args:
        year_df (pd.DataFrame): A dataframe of facility records for a single year (can be .applied to a groupby(year)
            object). Columns include percentages for each county and the fields needed for the calculations
        county_fields (List[str]): List county field names

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
    counties_df["county_wide_msw_recycling"] = recycling_df.sum()
    counties_df["county_wide_msw_composted"] = composted_df.sum()
    counties_df["county_wide_msw_digested"] = digested_df.sum()
    counties_df["county_wide_msw_landfilled"] = landfilled_df.sum()
    counties_df["county_wide_msw_recycling_rate"] = (
        (
            counties_df["county_wide_msw_recycling"]
            + counties_df["county_wide_msw_composted"]
            + counties_df["county_wide_msw_digested"]
        )
        / (
            counties_df["county_wide_msw_recycling"]
            + counties_df["county_wide_msw_composted"]
            + counties_df["county_wide_msw_digested"]
            + counties_df["county_wide_msw_landfilled"]
        )
        * 100
    )

    return counties_df


def facility_tons_diverted_from_landfills(year_df):
    """Calculate the total tonnage of material diverted from landfills for each facility.

    Tons diverted = Combined Total of Material Recycled + Total Materials recycled + Total Materials sent to composting
    + Combined Total Material for Composting +Total Material managed by AD/C + Combined Total Material for Combustion +
    Total Materials combusted + Total waste tires recycled (in Tons) + Total WT for combustion (in Tons)

    Args:
        year_df (pd.DataFrame): Dataframe of facility records for a single year (can be .applied to a groupby(year)).

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

    #: Extract just the number part of the facility id, strip leading zeros
    sum_df["id_"] = sum_df["facility_id"].astype(str).str[3:].str.lstrip("0")

    #: Replace 0s with None for AGOL/Arcade logic
    sum_df["tons_of_material_diverted_from_"] = sum_df["tons_of_material_diverted_from_"].replace(0, None)

    return sum_df[["Facility_Name__c", "id_", "tons_of_material_diverted_from_"]]


def recycling_rates_per_material(year_df):
    """Calculate the recycling rates for each material type for a given year.

    Args:
        year_df (pd.DataFrame): Dataframe of facility records for a single year (can be .applied to a groupby(year)).

    Returns:
        pd.DataFrame: Renamed material types, total tonnage recycled, and percent recycled
    """

    #: TODO: multiply by msw/100 per notes in notebook

    #: Field names from existing manual reports
    desired_fields = [
        "Municipal Solid Waste",
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

    fields = [field_mapping[alias] for alias in desired_fields]

    #: Just looking at recycling records
    recycling_df = year_df[year_df["Classifications__c"] == "Recycling"][fields]

    #: Sum totals across all records, calculate total percentage
    sum_series = pd.Series()
    for col in fields[1:]:  #: We don't want to total Municipal Solid Waste, we just need for the computation
        sum_series[col] = (recycling_df["Municipal_Solid_Waste__c"] / 100 * recycling_df[col]).sum()
    materials_sums_df = pd.DataFrame(sum_series, columns=["amount"])
    materials_sums_df["percent"] = (
        materials_sums_df["amount"] / materials_sums_df.loc["Total_Material_Received_Compost__c", "amount"]
    )

    #: Rename columns for existing AGOL layer
    regex = re.compile(r"(?<=Total_)(.+)(?=_Materials_received__c)|(?<=Total_)(.+)(?=_received__c)")
    materials_sums_df.reset_index(names="material", inplace=True)
    materials_sums_df["material"] = (
        materials_sums_df["material"]
        .apply(lambda x: re.search(regex, x)[0] if re.search(regex, x) else x)
        .str.replace("__c", "")
        .str.replace("_", " ")
    )

    return materials_sums_df


def composting_rates_per_material(year_df):

    desired_fields = [
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

    fields = [field_mapping[alias] for alias in desired_fields]

    #: Just looking at composting records
    composting_df = year_df[year_df["Classifications__c"] == "Composts"][fields]

    #: Sum totals across all records taking into account MSW modifier, calculate total percentage
    sum_series = pd.Series()
    for col in fields[1:]:  #: We don't want to total Municipal Solid Waste, we just need for the computation
        sum_series[col] = (composting_df["Municipal_Solid_Waste__c"] / 100 * composting_df[col]).sum()
    sum_df = pd.DataFrame(sum_series, columns=["amount"])
    sum_df["percent"] = sum_df["amount"] / sum_df.loc["Total_Material_Received_Compost__c", "amount"]

    #: Rename columns for existing AGOL layer
    regex = re.compile(r"(?<=Total_)(.+)(?=_Materials_recei)|(?<=Total_)(.+)(?=_recei)")
    sum_df.reset_index(names="material", inplace=True)
    sum_df["material"] = (
        sum_df["material"]
        .apply(lambda x: re.search(regex, x)[0] if re.search(regex, x) else x)
        .str.replace("__c", "")
        .str.replace("_", " ")
        .str.replace(" CM", " Compostable Material")
    )

    return sum_df


def rates_per_material(year_df: pd.DataFrame, classification: str, fields: list[str]) -> pd.DataFrame:

    #: Make sure the MSW percentage field is last
    try:
        fields.remove("Municipal_Solid_Waste__c")
    except ValueError:
        pass
    fields.append("Municipal_Solid_Waste__c")

    subset_df = year_df[year_df["Classifications__c"] == classification][fields]

    #: Sum totals across all records taking into account MSW modifier, calculate total percentage
    sum_series = pd.Series()
    for col in fields[:-1]:  #: We don't want to total Municipal Solid Waste, we just need for the computation
        sum_series[col] = (subset_df["Municipal_Solid_Waste__c"] / 100 * subset_df[col]).sum()
    sum_df = pd.DataFrame(sum_series, columns=["amount"])
    sum_df["percent"] = sum_df["amount"] / sum_df.loc["Total_Material_Received_Compost__c", "amount"]

    #: Rename columns for existing AGOL layer
    regex = re.compile(r"(?<=Total_)(.+)(?=_Materials_recei)|(?<=Total_)(.+)(?=_recei)")
    sum_df.reset_index(names="material", inplace=True)
    sum_df["material"] = (
        sum_df["material"]
        .apply(lambda x: re.search(regex, x)[0] if re.search(regex, x) else x)
        .str.replace("__c", "")
        .str.replace("_", " ")
        .str.replace(" CM", " Compostable Material")
    )

    return sum_df
