#!/usr/bin/env python
# * coding: utf8 *
"""
Run the wmrc script as a cloud function.
"""
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import arcgis
import google.auth
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor  # noqa: F401
from palletjack import extract, load, transform
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, helpers, version
except ImportError:
    import config
    import helpers
    import version


class Skid:
    def __init__(self):
        self.secrets = SimpleNamespace(**self._get_secrets())
        self.tempdir = TemporaryDirectory(ignore_cleanup_errors=True)
        self.tempdir_path = Path(self.tempdir.name)
        self.log_name = f'{config.LOG_FILE_NAME}_{datetime.now().strftime("%Y%m%d-%H%M%S")}.txt'
        self.log_path = self.tempdir_path / self.log_name
        self._initialize_supervisor()
        self.skid_logger = logging.getLogger(config.SKID_NAME)

    def __del__(self):
        self.tempdir.cleanup()

    @staticmethod
    def _get_secrets():
        """A helper method for loading secrets from either a GCF mount point or the local src/wmrc/secrets/secrets.json file

        Raises:
            FileNotFoundError: If the secrets file can't be found.

        Returns:
            dict: The secrets .json loaded as a dictionary
        """

        secret_folder = Path("/secrets")

        #: Try to get the secrets from the Cloud Function mount point
        if secret_folder.exists():
            secrets_dict = json.loads(Path("/secrets/app/secrets.json").read_text(encoding="utf-8"))
            credentials, _ = google.auth.default()
            secrets_dict["SERVICE_ACCOUNT_JSON"] = credentials
            return secrets_dict

        #: Otherwise, try to load a local copy for local development
        #: This file path might not work if extracted to its own module
        secret_folder = Path(__file__).parent / "secrets"
        if secret_folder.exists():
            return json.loads((secret_folder / "secrets.json").read_text(encoding="utf-8"))

        raise FileNotFoundError("Secrets folder not found; secrets not loaded.")

    def _initialize_supervisor(self):
        """A helper method to set up logging and supervisor

        Args:
            log_path (Path): File path for the logfile to be written
            sendgrid_api_key (str): The API key for sendgrid for this particular application

        Returns:
            Supervisor: The supervisor object used for sending messages
        """

        skid_logger = logging.getLogger(config.SKID_NAME)
        skid_logger.setLevel(config.LOG_LEVEL)
        palletjack_logger = logging.getLogger("palletjack")
        palletjack_logger.setLevel(config.LOG_LEVEL)

        cli_handler = logging.StreamHandler(sys.stdout)
        cli_handler.setLevel(config.LOG_LEVEL)
        formatter = logging.Formatter(
            fmt="%(levelname)-7s %(asctime)s %(name)15s:%(lineno)5s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        cli_handler.setFormatter(formatter)

        log_handler = logging.FileHandler(self.log_path, mode="w")
        log_handler.setLevel(config.LOG_LEVEL)
        log_handler.setFormatter(formatter)

        skid_logger.addHandler(cli_handler)
        skid_logger.addHandler(log_handler)
        palletjack_logger.addHandler(cli_handler)
        palletjack_logger.addHandler(log_handler)

        #: Log any warnings at logging.WARNING
        #: Put after everything else to prevent creating a duplicate, default formatter
        #: (all log messages were duplicated if put at beginning)
        logging.captureWarnings(True)

        skid_logger.debug("Creating Supervisor object")
        self.supervisor = Supervisor(handle_errors=False)
        sendgrid_settings = config.SENDGRID_SETTINGS
        sendgrid_settings["api_key"] = self.secrets.SENDGRID_API_KEY
        self.supervisor.add_message_handler(
            SendGridHandler(
                sendgrid_settings=sendgrid_settings, client_name=config.SKID_NAME, client_version=version.__version__
            )
        )

    def _remove_log_file_handlers(self, loggers):
        """A helper function to remove the file handlers so the tempdir will close correctly

        Args:
            log_name (str): The logfiles filename
            loggers (List<str>): The loggers that are writing to log_name
        """

        for logger in loggers:
            for handler in logger.handlers:
                try:
                    if self.log_name in handler.stream.name:
                        logger.removeHandler(handler)
                        handler.close()
                except Exception:
                    pass

    def process(self):
        """The main function that does all the work."""

        #: Set up secrets, tempdir, supervisor, and logging
        start = datetime.now()

        #: Get our GIS object via the ArcGIS API for Python
        gis = arcgis.gis.GIS(config.AGOL_ORG, self.secrets.AGOL_USER, self.secrets.AGOL_PASSWORD)

        #: Do the work

        #: Load data from Salesforce and generate analyses
        self.skid_logger.info("Loading records from Salesforce...")
        records = self._load_salesforce_data()
        facility_summary_df = self._facility_summaries(records).query("data_year == @config.YEAR")
        county_summary_df = self._county_summaries(records)  # .query("data_year == @config.YEAR")
        materials_recycled_df = self._materials_recycled(records)
        materials_composted_df = self._materials_composted(records)

        #: Facilities on map
        self.skid_logger.info("Updating facility info...")
        facilities_load_count = self._update_facilities(gis, facility_summary_df)

        #: county summaries on map, dashboard:
        self.skid_logger.info("Updating county info...")
        counties_update_count = self._update_counties(gis, county_summary_df)

        #: Materials recycled on dashboard:
        self.skid_logger.info("Updating materials recycled...")
        materials_spatial = self._add_bogus_geometries(materials_recycled_df)
        materials_spatial.rename(columns={"percent": "percent_"}, inplace=True)
        materials_loader = load.FeatureServiceUpdater(gis, config.MATERIALS_LAYER_ITEMID, self.tempdir_path)
        materials_count = materials_loader.truncate_and_load_features(materials_spatial)

        #:  Materials composted on dashboard:
        self.skid_logger.info("Updating materials composted...")
        composting_spatial = self._add_bogus_geometries(materials_composted_df)
        composting_spatial.rename(columns={"percent": "percent_"}, inplace=True)
        composting_loader = load.FeatureServiceUpdater(gis, config.COMPOSTING_LAYER_ITEMID, self.tempdir_path)
        composting_count = composting_loader.truncate_and_load_features(composting_spatial)

        #: Statewide metrics
        self.skid_logger.info("Updating statewide metrics...")
        statewide_totals_df = county_summary_df.groupby("data_year").apply(helpers.statewide_yearly_metrics)
        contamination_rates_df = self._contamination_rates_by_tonnage(records)
        # contamination_rates_df = self._contamination_rates_by_facility(records)
        statewide_metrics = pd.concat([statewide_totals_df, contamination_rates_df], axis=1)
        statewide_spatial = self._add_bogus_geometries(statewide_metrics)
        statewide_loader = load.FeatureServiceUpdater(gis, config.STATEWIDE_LAYER_ITEMID, self.tempdir_path)
        statewide_count = statewide_loader.truncate_and_load_features(statewide_spatial)

        end = datetime.now()

        summary_message = MessageDetails()
        summary_message.subject = f"{config.SKID_NAME} Update Summary"
        summary_rows = [
            f'{config.SKID_NAME} update {start.strftime("%Y-%m-%d")}',
            "=" * 20,
            "",
            f'Start time: {start.strftime("%H:%M:%S")}',
            f'End time: {end.strftime("%H:%M:%S")}',
            f"Duration: {str(end-start)}",
            "",
            f"Facility rows loaded: {facilities_load_count}",
            f"County rows loaded: {counties_update_count}",
            f"Materials recycled rows loaded: {materials_count}",
            f"Materials composted rows loaded: {composting_count}",
            f"Statewide metrics rows loaded: {statewide_count}",
        ]

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = self.tempdir_path / self.log_name

        self.supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        self._remove_log_file_handlers(loggers)

    def _update_counties(self, gis, county_summary_df):

        county_geoms = transform.FeatureServiceMerging.get_live_dataframe(gis, config.COUNTY_BOUNDARIES_ITEMID)[
            ["name", "SHAPE"]
        ].set_index("name")
        new_data = county_summary_df.merge(county_geoms, left_index=True, right_index=True, how="left")

        new_data.reset_index(inplace=True)
        new_data.spatial.project(4326)
        new_data.spatial.sr = {"wkid": 4326}

        updater = load.FeatureServiceUpdater(gis, config.COUNTY_LAYER_ITEMID, self.tempdir_path)
        update_count = updater.truncate_and_load_features(new_data)
        return update_count

    def _update_facilities(self, gis, facility_summary_df):
        self.skid_logger.info("Loading data from Google Sheets...")
        combined_df = self._parse_from_google_sheets()
        self.skid_logger.info("Adding county names from SGID county boundaries...")
        with_counties_df = self._get_county_names(combined_df, gis)

        #:  Merge facility summaries and google sheet on id_
        google_and_sf_data = with_counties_df.merge(
            facility_summary_df[["id_", "tons_of_material_diverted_from_"]],
            on="id_",
        )

        #:  Merge live data with sheet/sf data
        live_facility_data = transform.FeatureServiceMerging.get_live_dataframe(gis, config.FACILITIES_LAYER_ITEMID)
        updated_facility_data = live_facility_data.set_index("id_")
        updated_facility_data.update(google_and_sf_data.set_index("id_"))
        updated_facility_data.reset_index(inplace=True)

        #:  Truncate and load to AGOL
        self.skid_logger.info("Preparing data for truncate and load...")
        # new_facility_data = with_counties_df.copy()
        updated_facility_data.spatial.project(4326)
        updated_facility_data.spatial.set_geometry("SHAPE")
        updated_facility_data.spatial.sr = {"wkid": 4326}
        updated_facility_data["last_updated"] = date.today()
        updated_facility_data = transform.DataCleaning.switch_to_datetime(updated_facility_data, ["last_updated"])
        updated_facility_data = transform.DataCleaning.switch_to_float(
            updated_facility_data,
            [
                "latitude",
                "longitude",
                "tons_of_material_diverted_from_",
                "gallons_of_used_oil_collected_for_recycling_last_year",
            ],
        )

        # #: Fields from sheet that aren't in AGOL
        # updated_facility_data.drop(columns=["local_health_department", "uocc_email_address"], inplace=True)

        self.skid_logger.info("Truncating and loading...")
        updater = load.FeatureServiceUpdater(gis, config.FACILITIES_LAYER_ITEMID, self.tempdir_path)
        load_count = updater.truncate_and_load_features(updated_facility_data)
        return load_count

    def _parse_from_google_sheets(self):
        #: Get individual sheets
        gsheet_extractor = extract.GSheetLoader(self.secrets.SERVICE_ACCOUNT_JSON)
        sw_df = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self.secrets.SHEET_ID, "SW Facilities", by_title=True
        )
        uocc_df = gsheet_extractor.load_specific_worksheet_into_dataframe(self.secrets.SHEET_ID, "UOCCs", by_title=True)

        #: Fix columns
        try:
            sw_df.drop(columns=[""], inplace=True)  #: Drop empty columns that don't have a name
        except KeyError:
            pass

        sw_df.rename(
            columns={"Accept Material\n Dropped \n Off by the Public": "Accept Material Dropped Off by the Public"},
            inplace=True,
        )
        uocc_df.rename(
            columns={
                "Type": "Class",
                "Accept Material\n Dropped \n Off by the Public": "Accept Material Dropped Off by the Public",
            },
            inplace=True,
        )
        combined_df = pd.concat([sw_df, uocc_df]).query('Status in ["Open", "OPEN"]')

        renamed_df = (
            transform.DataCleaning.rename_dataframe_columns_for_agol(combined_df)
            .rename(columns=str.lower)
            .rename(
                columns={
                    "longitude_": "longitude",
                    "accept_material_dropped_off_by_the_public": "accept_material_dropped_off_by_",
                    "tons_of_material_diverted_from_landfills_last_year": "tons_of_material_diverted_from_",
                }
            )
        )
        renamed_df["id_"] = renamed_df["id_"].astype(str)

        return renamed_df

    @staticmethod
    def _get_county_names(input_df, gis):
        #: Load counties from open data feature service
        counties_df = pd.DataFrame.spatial.from_layer(
            arcgis.features.FeatureLayer.fromitem(gis.content.get(config.COUNTIES_ITEMID))
        )
        counties_df.spatial.project(26912)
        counties_df.reset_index(inplace=True)
        counties_df = counties_df.reindex(columns=["SHAPE", "NAME"])  #: We only care about the county name
        counties_df.spatial.set_geometry("SHAPE")
        counties_df.spatial.sr = {"wkid": 26912}

        #: Convert dataframe to spatial
        spatial_df = pd.DataFrame.spatial.from_xy(input_df, x_column="longitude", y_column="latitude")
        spatial_df.reset_index(drop=True, inplace=True)
        spatial_df.spatial.project(26912)
        spatial_df.spatial.set_geometry("SHAPE")
        spatial_df.spatial.sr = {"wkid": 26912}

        #: Perform the join, clean up the output
        joined_points_df = spatial_df.spatial.join(counties_df, "left", "within")
        joined_points_df.drop(columns=["index_right"], inplace=True)
        joined_points_df.rename(columns={"NAME": "county_name"}, inplace=True)
        joined_points_df["county_name"] = joined_points_df["county_name"].str.title()

        return joined_points_df

    #: The following methods operate on all the salesforce data, while the SalesForceRecords class operates on subsets
    #: of the data, usually a year at a time. Thus, these methods get all the records and then groupby them by year,
    #: applying the SalesForceRecords methods.
    def _load_salesforce_data(self) -> helpers.SalesForceRecords:

        salesforce_credentials = extract.SalesforceApiUserCredentials(
            self.secrets.SF_CLIENT_SECRET, self.secrets.SF_CLIENT_ID
        )
        salesforce_extractor = extract.SalesforceRestLoader(self.secrets.SF_ORG, salesforce_credentials)

        salesforce_records = helpers.SalesForceRecords(salesforce_extractor)
        salesforce_records.extract_data_from_salesforce()

        return salesforce_records

    @staticmethod
    def _county_summaries(records: helpers.SalesForceRecords) -> pd.DataFrame:

        county_df = records.df.groupby("Calendar_Year__c").apply(
            helpers.county_summaries, county_fields=records.county_fields
        )
        county_df.index.names = ["data_year", "name"]
        county_df.reset_index(level="data_year", inplace=True)
        county_df.rename(
            index={name: name.replace("__c", "").replace("_", " ") for name in county_df.index}, inplace=True
        )
        county_df["data_year"] = county_df["data_year"].apply(helpers.convert_to_int)

        return county_df

    @staticmethod
    def _facility_summaries(records: helpers.SalesForceRecords) -> pd.DataFrame:
        facility_summaries = (
            records.df.groupby("Calendar_Year__c")
            .apply(
                helpers.facility_tons_diverted_from_landfills,
            )
            .droplevel(1)
        )
        facility_summaries.index.name = "data_year"
        facility_summaries.reset_index(inplace=True)
        facility_summaries["data_year"] = facility_summaries["data_year"].apply(helpers.convert_to_int)

        return facility_summaries

    @staticmethod
    def _materials_recycled(records: helpers.SalesForceRecords) -> pd.DataFrame:
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
                helpers.rates_per_material,
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

    @staticmethod
    def _materials_composted(records: helpers.SalesForceRecords) -> pd.DataFrame:
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
                helpers.rates_per_material,
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

    @staticmethod
    def _contamination_rates_by_tonnage(records: helpers.SalesForceRecords) -> pd.DataFrame:
        records.df["in_state_modifier"] = (100 - records.df["Out_of_State__c"]) / 100
        records.df["recycling_tons_contaminated"] = (
            records.df["Annual_Recycling_Contamination_Rate__c"]
            / 100
            * records.df["Combined_Total_of_Material_Recycled__c"]
            * records.df["in_state_modifier"]
        )
        records.df["recycling_tons_report_contamination_total"] = pd.NA
        records.df.loc[
            ~records.df["recycling_tons_contaminated"].isnull(), "recycling_tons_report_contamination_total"
        ] = (records.df["Combined_Total_of_Material_Recycled__c"] * records.df["in_state_modifier"])

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

    def _contamination_rates_by_facility(records: helpers.SalesForceRecords) -> pd.DataFrame:

        records.df["annual_recycling_uncontaminated_rate"] = 100 - records.df["Annual_Recycling_Contamination_Rate__c"]
        yearly_stats = records.df.groupby("Calendar_Year__c").describe()
        yearly_stats.index = yearly_stats.index.map(helpers.convert_to_int)
        yearly_stats.index.name = "data_year"
        return yearly_stats[["count", "mean", "std"]]

    @staticmethod
    def _add_bogus_geometries(input_dataframe: pd.DataFrame) -> pd.DataFrame:
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


def main(event, context):  # pylint: disable=unused-argument
    """Entry point for Google Cloud Function triggered by pub/sub event

    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to Cloud Logging.
    """

    #: This function must be called 'main' to act as the Google Cloud Function entry point. It must accept the two
    #: arguments listed, but doesn't have to do anything with them (I haven't used them in anything yet).

    #: Call process() and any other functions you want to be run as part of the skid here.
    wmrc_skid = Skid()
    wmrc_skid.process()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    main(1, 2)  #: Just some junk args to satisfy the signature needed for Cloud Functions
