import re
from typing import Mapping

import palletjack
import pandas as pd


def convert_to_int(s):
    """Convert a string to an integer. If the string cannot be converted, return -1."""
    try:
        return int(s)
    except ValueError:
        return -1


class SalesForceRecords:

    def __init__(self, salesforce_extractor: palletjack.extract.SalesforceRestLoader):
        self.salesforce_extractor = salesforce_extractor
        self.field_mapping: Mapping[str, str] = {}
        self.county_fields: list[str] = []

        self._build_field_mapping()

    def extract_data_from_salesforce(self):
        """Load data from Salesforce into self.df dataframe

        Builds a string of needed column names for our specific needs and uses that in the REST query. Only loads
        reports that have a Status of 'Submitted' and a RecordType of 'Annual Report.'
        """

        fields_string = self._build_columns_string()

        #: Main query with just our desired fields
        self.df = self.salesforce_extractor.get_records(
            "services/data/v60.0/query/",
            f"SELECT {fields_string} from Application_Report__c WHERE Status__c = 'Submitted' AND RecordType.Name = 'Annual Report'",
        )

        #: Extract the facility id from the nested salesforce object
        self.df["facility_id"] = self.df["Facility__r"].apply(lambda x: x["Solid_Waste_Facility_ID_Number__c"])

    def _build_columns_string(self) -> str:
        """Build a string of needed columns for the SOQL query based on field mapping and some custom fields

        Returns:
            str: A comma-delimited string of needed columns for the SOQL query
        """

        fields_string = ",".join(self.field_mapping.values())
        fields_string += (
            ",RecordTypeId,Classifications__c,RecordType.Name,Facility__r.Solid_Waste_Facility_ID_Number__c"
        )
        fields_string += "," + ",".join(self.county_fields)

        return fields_string

    def _build_field_mapping(self):
        """Map names from manual reports to Salesforce field names.

        Queries Salesforce for the first 200 records to get all the column names. Then maps field names from manual
        report runs to the Salesforce column names.

        Raises:
            ValueError: If a field from the manual reports is not found in the Salesforce columns
        """

        #: Get a dataframe of the first 200 rows with all the columns so we can extract the needed columns
        df_for_columns = self.salesforce_extractor.get_records(
            "services/data/v60.0/query/", "SELECT FIELDS(ALL) from Application_Report__c LIMIT 200"
        )
        self.county_fields = [col for col in df_for_columns.columns if "_County" in col]

        aliases = [
            "Combined Total of Material Recycled",
            "Municipal Solid Waste",
            "Total Materials sent to composting",
            "Total Material managed by AD/C",
            "Municipal Waste In-State (in Tons)",
            "Facility Name",
            # "Solid Waste Facility ID Number",
            "Combined Total of Material Recycled",
            "Total Materials recycled",
            "Total Materials sent to composting",
            # "Combined Total Material for Composting",
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
        missing_fields = []
        for alias in aliases:
            if alias in self.field_mapping:
                continue
            field_name = (
                f'{alias.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace("/", "")}__c'
            )
            if field_name not in df_for_columns.columns:
                missing_fields.append(alias)
                continue
            self.field_mapping[alias] = field_name

        #: Fix typo in fields
        self.field_mapping["Combined Total Material for Composting"] = "Combined_Total_Material_for_Compostion__c"

        if missing_fields:
            raise ValueError(f"Missing fields: {missing_fields}")


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


def rates_per_material(year_df: pd.DataFrame, classification: str, fields: list[str], total_field: str) -> pd.DataFrame:
    """Calculate recycling/composting rates for each material type for a given year.

    Args:
        year_df (pd.DataFrame): Dataframe of facility records for a single year (can be .applied to a groupby(year) object).
        classification (str): Report Classification, either "Recycling" or "Composts"
        fields (list[str]): List of the fields containing the material totals.
        total_field (str): The field containing the total material received for the percentage calculation.

    Returns:
        pd.DataFrame: Renamed material types, total tonnage processed, and percent processed
    """

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
    sum_df["percent"] = sum_df["amount"] / sum_df.loc[total_field, "amount"]
    #: FIXME: ^ parameterize the totals field

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
