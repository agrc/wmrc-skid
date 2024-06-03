from typing import Mapping

import palletjack
import pandas as pd


def convert_to_int(s):
    """Convert a string to an integer. If the string cannot be converted, return -1."""
    try:
        return int(s)
    except ValueError:
        return -1


def add_bogus_geometries(input_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Add a bogus geometry (point in downtown Malad City, ID) to a dataframe in WKID 4326.

    Args:
        input_dataframe (pd.DataFrame): Non-spatial dataframe to add geometry to

    Returns:
        pd.DataFrame: Spatially-enabled dataframe version of input input_dataframe with geometry added to every row
    """

    input_dataframe["x"] = 12_495_000
    input_dataframe["y"] = 5_188_000

    spatial_dataframe = pd.DataFrame.spatial.from_xy(input_dataframe, "x", "y", sr=4326)

    spatial_dataframe.drop(columns=["x", "y"], inplace=True)

    return spatial_dataframe


class SalesForceRecords:
    """A helper class that extracts data from Salesforce based on fields from WMRC's manual reports. Provides access to
    the data through the .df attribute along with the field mapping and list of counties.
    """

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
        additional_fields = [
            "RecordTypeId",
            "Classifications__c",
            "RecordType.Name",
            "Facility__r.Solid_Waste_Facility_ID_Number__c",
            "LastModifiedDate",
        ]

        fields_string = ",".join(list(self.field_mapping.values()) + additional_fields + self.county_fields)

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
            "services/data/v60.0/query/", "SELECT FIELDS(ALL) from Application_Report__c LIMIT 1"
        )
        self.county_fields = [col for col in df_for_columns.columns if "_County" in col]
        self.county_fields.append("Out_of_State__c")

        aliases = [
            "Combined Total of Material Recycled",
            "Municipal Solid Waste",
            "Total Materials sent to composting",
            "Total Material managed by AD/C",
            "Municipal Waste In-State (in Tons)",
            "Facility Name",
            # "Solid Waste Facility ID Number",  #: From nested Facility object
            "Combined Total of Material Recycled",
            "Total Materials recycled",
            "Total Materials sent to composting",
            # "Combined Total Material for Composting",  #: Typo in field name
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
            "Annual Recycling Contamination Rate",
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

    def deduplicate_records_on_facility_id(self) -> Mapping[str, str]:
        """Deduplicate all facilities' records, dropping all but the latest modified record per Calendar_Year__c.

        Returns:
            Mapping[str, str]: Dictionary of facility ids: calendar years that had duplicate records - {"SW0123":
                "2022, 2023", etc}
        """

        #: {"SW0123": "2022, 2023", etc}
        duplicated_facility_ids = {
            facility_id: ", ".join(years)
            for facility_id, years in self.df[
                self.df.duplicated(subset=["facility_id", "Calendar_Year__c"], keep=False)
            ]
            .groupby("facility_id")["Calendar_Year__c"]
            .unique()
            .items()
        }

        #: Sort by last updated time and keep the most recent record
        self.df["LastModifiedDate"] = pd.to_datetime(self.df["LastModifiedDate"])
        self.df = self.df.sort_values("LastModifiedDate").drop_duplicates(
            subset=["facility_id", "Calendar_Year__c"], keep="last"
        )

        return duplicated_facility_ids
