#!/usr/bin/env python
# * coding: utf8 *
"""
Run the wmrc script as a cloud function.
"""
import base64
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import arcgis
import functions_framework
import google.auth
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor  # noqa: F401
from cloudevents.http import CloudEvent
from palletjack import extract, load, transform
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, helpers, summarize, validate, version, yearly
except ImportError:
    import config
    import helpers
    import summarize
    import validate
    import version
    import yearly


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

    def _remove_log_file_handlers(self, loggers: list[str]):
        """A helper function to remove the file handlers so the tempdir will close correctly

        Args:
            loggers (list[str]): The loggers that are writing to log_name
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
        """The main method that does all the work."""

        start = datetime.now()

        #: Get our GIS object via the ArcGIS API for Python
        gis = arcgis.gis.GIS(config.AGOL_ORG, self.secrets.AGOL_USER, self.secrets.AGOL_PASSWORD)

        #: Load data from Salesforce and generate analyses using Summarize methods
        self.skid_logger.info("Loading records from Salesforce...")
        records = self._load_salesforce_data()
        duplicate_facility_ids = records.deduplicate_records_on_facility_id()
        facility_summary_df = summarize.facilities(records).query("data_year == @config.YEAR")
        county_summary_df = summarize.counties(records)
        materials_recycled_df = summarize.materials_recycled(records)
        materials_composted_df = summarize.materials_composted(records)

        #: Facilities on map
        self.skid_logger.info("Updating facility info...")
        facilities_load_count = self._update_facilities(gis, facility_summary_df)

        #: county summaries on map, dashboard:
        self.skid_logger.info("Updating county info...")
        counties_update_count = self._update_counties(gis, county_summary_df)

        #: Materials recycled on dashboard:
        self.skid_logger.info("Updating materials recycled...")
        materials_spatial = helpers.add_bogus_geometries(materials_recycled_df)
        materials_spatial.rename(columns={"percent": "percent_"}, inplace=True)
        materials_loader = load.FeatureServiceUpdater(gis, config.MATERIALS_LAYER_ITEMID, self.tempdir_path)
        materials_count = materials_loader.truncate_and_load_features(materials_spatial)

        #:  Materials composted on dashboard:
        self.skid_logger.info("Updating materials composted...")
        composting_spatial = helpers.add_bogus_geometries(materials_composted_df)
        composting_spatial.rename(columns={"percent": "percent_"}, inplace=True)
        composting_loader = load.FeatureServiceUpdater(gis, config.COMPOSTING_LAYER_ITEMID, self.tempdir_path)
        composting_count = composting_loader.truncate_and_load_features(composting_spatial)

        #: Statewide metrics
        self.skid_logger.info("Updating statewide metrics...")
        statewide_totals_df = county_summary_df.groupby("data_year").apply(yearly.statewide_metrics)
        contamination_rates_df = summarize.recovery_rates_by_tonnage(records)
        statewide_metrics = pd.concat([statewide_totals_df, contamination_rates_df], axis=1)
        statewide_spatial = helpers.add_bogus_geometries(statewide_metrics)
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
        if duplicate_facility_ids:
            summary_rows.insert(7, "Duplicate facility IDs per calendar year:")
            summary_rows.insert(8, "\t" + "\n\t".join(f"{k}: {v}" for k, v in duplicate_facility_ids.items()))

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = self.tempdir_path / self.log_name

        self.supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        self._remove_log_file_handlers(loggers)

    def _update_counties(self, gis: arcgis.gis.GIS, county_summary_df: pd.DataFrame) -> int:
        """Updates the live county summary data on AGOL with data from salesforce using another feature service as a geometry source.

        Truncates and loads after merging the updated data with the geometries. Relies on
        config.COUNTY_BOUNDARIES_ITEMID and config.COUNTY_LAYER_ITEMID to access these layers.

        The geometry source layer needs to have an extra geometry placed somewhere out of the normal extent named "Out
        of State" for capturing info about material from out of the state.

        Args:
            gis (arcgis.gis.GIS): AGOL org with both the live layer and the geometry source layer
            county_summary_df (pd.DataFrame): The county summary report generated from Salesforce records

        Returns:
            int: Number of records updated.
        """

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

    def _update_facilities(self, gis: arcgis.gis.GIS, facility_summary_df: pd.DataFrame) -> int:
        """Updates the live facility data on AGOL with data from the Google sheets and Salesforce.

        Truncates and loads after merging the live data with the updated data. Does not (currently) add new features.
        Relies on config.FACILITIES_LAYER_ITEMID to access the live data. MSW facility info comes from the Google
        sheet, while their total diverted comes from Salesforce. All UOCC data comes from the sheet.

        Args:
            gis (arcgis.gis.GIS): AGOL org with the live layer
            facility_summary_df (pd.DataFrame): The facility summary report generated from Salesforce records

        Returns:
            int: Number of facilities loaded.
        """
        self.skid_logger.info("Loading data from Google Sheets...")
        combined_df = self._parse_from_google_sheets()
        self.skid_logger.info("Adding county names from SGID county boundaries...")
        with_counties_df = self._get_county_names(combined_df, gis)

        #:  Merge facility summaries and google sheet on id_
        google_and_sf_data = with_counties_df.merge(
            facility_summary_df[["id_", "tons_of_material_diverted_from_"]],
            on="id_",
            how="left",
        )
        google_and_sf_data["tons_of_material_diverted_from_"] = google_and_sf_data[
            "tons_of_material_diverted_from_"
        ].astype(str)

        #: Update to overwrite the name, website, phone, and accept values on the sheet from Salesforce instead
        google_and_sf_data.set_index("id_", inplace=True)
        google_and_sf_data.update(
            facility_summary_df[
                ["id_", "website", "phone_no_", "accept_material_dropped_off_by_", "facility_name"]
            ].set_index("id_")
        )
        google_and_sf_data.reset_index(inplace=True)

        #: Subset down the columns to only the ones that are in the live data
        live_facility_data = transform.FeatureServiceMerging.get_live_dataframe(gis, config.FACILITIES_LAYER_ITEMID)
        live_fields = live_facility_data.columns
        common_fields = set(google_and_sf_data.columns).intersection(live_fields)
        google_and_sf_data = google_and_sf_data[list(common_fields)]

        #: Calculate the filter field so that MRFs are under Recycling Facilities
        google_and_sf_data["type_filter"] = google_and_sf_data["facility_type"].apply(
            lambda x: "Recycling Facility" if x == "Recycling Facility - MRF" else x
        )

        #:  Truncate and load to AGOL
        self.skid_logger.info("Preparing data for truncate and load...")
        google_and_sf_data.spatial.project(4326)
        google_and_sf_data.spatial.set_geometry("SHAPE")
        google_and_sf_data.spatial.sr = {"wkid": 4326}
        google_and_sf_data["last_updated"] = date.today()
        google_and_sf_data = transform.DataCleaning.switch_to_datetime(google_and_sf_data, ["last_updated"])
        google_and_sf_data = transform.DataCleaning.switch_to_float(
            google_and_sf_data,
            [
                "latitude",
                "longitude",
                "tons_of_material_diverted_from_",
                "gallons_of_used_oil_collected_f",
            ],
        )

        self.skid_logger.info("Truncating and loading...")
        updater = load.FeatureServiceUpdater(gis, config.FACILITIES_LAYER_ITEMID, self.tempdir_path)
        load_count = updater.truncate_and_load_features(google_and_sf_data)
        return load_count

    def _parse_from_google_sheets(self) -> pd.DataFrame:
        """Load MSW and UOCC data from Google Sheets and combine them into a single dataframe.

        Does some field cleaning and aligns the UOCC columns with the MSW columns for consistency.

        Returns:
            pd.DataFrame: Single dataframe with the unified data from the two sheets
        """
        #: Get individual sheets
        gsheet_extractor = extract.GSheetLoader(self.secrets.SERVICE_ACCOUNT_JSON)
        sw_df = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self.secrets.SHEET_ID, "SW Facilities", by_title=True
        )
        uocc_df = gsheet_extractor.load_specific_worksheet_into_dataframe(self.secrets.SHEET_ID, "UOCCs", by_title=True)

        #: Fix columns
        sw_df.drop(columns=[""], inplace=True, errors="ignore")  #: Drop empty columns that don't have a name
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
                    "gallons_of_used_oil_collected_for_recycling_last_year": "gallons_of_used_oil_collected_f",
                }
            )
        )
        renamed_df["id_"] = renamed_df["id_"].astype(str)

        return renamed_df

    @staticmethod
    def _get_county_names(input_df: pd.DataFrame, gis: arcgis.gis.GIS) -> pd.DataFrame:
        """Assigns a county name to each facility based on the SGID county boundaries hosted in AGOL.

        Args:
            input_df (pd.DataFrame): Facility dataframe with "latitude" and "longitude" columns
            gis (arcgis.gis.GIS): AGOL org containing the county boundaries

        Returns:
            pd.DataFrame: A spatially-enabled dataframe of the facilities in WKID 26912 with county names added.
        """

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
        input_df = input_df[
            input_df["latitude"].astype(bool) & input_df["longitude"].astype(bool)
        ]  #: Drop empty lat/long
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

    def _load_salesforce_data(self) -> helpers.SalesForceRecords:
        """Helper method to connect to and load data from Salesforce.

        Returns:
            helpers.SalesForceRecords: An object containing the records from Salesforce along with other derived data.
        """

        salesforce_credentials = extract.SalesforceApiUserCredentials(
            self.secrets.SF_CLIENT_SECRET, self.secrets.SF_CLIENT_ID
        )
        salesforce_extractor = extract.SalesforceRestLoader(self.secrets.SF_ORG, salesforce_credentials)

        salesforce_records = helpers.SalesForceRecords(salesforce_extractor)
        salesforce_records.extract_data_from_salesforce()

        return salesforce_records


# def main(event, context):  # pylint: disable=unused-argument
#     """Entry point for Google Cloud Function triggered by pub/sub event

#     Args:
#          event (dict):  The dictionary with data specific to this type of
#                         event. The `@type` field maps to
#                          `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
#                         The `data` field maps to the PubsubMessage data
#                         in a base64-encoded string. The `attributes` field maps
#                         to the PubsubMessage attributes if any is present.
#          context (google.cloud.functions.Context): Metadata of triggering event
#                         including `event_id` which maps to the PubsubMessage
#                         messageId, `timestamp` which maps to the PubsubMessage
#                         publishTime, `event_type` which maps to
#                         `google.pubsub.topic.publish`, and `resource` which is
#                         a dictionary that describes the service API endpoint
#                         pubsub.googleapis.com, the triggering topic's name, and
#                         the triggering event type
#                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
#     Returns:
#         None. The output is written to Cloud Logging.
#     """

#     #: This function must be called 'main' to act as the Google Cloud Function entry point. It must accept the two
#     #: arguments listed, but doesn't have to do anything with them (I haven't used them in anything yet).

#     #: Call process() and any other functions you want to be run as part of the skid here.
#     wmrc_skid = Skid()
#     wmrc_skid.process()


@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    """Entry point for Google Cloud Function triggered by pub/sub event

    Args:
         cloud_event (CloudEvent):  The CloudEvent object with data specific to this type of
                        event. The `type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
    Returns:
        None. The output is written to Cloud Logging.
    """

    #: This function must be called 'subscribe' to act as the Google Cloud Function entry point. It must accept the
    #: CloudEvent object as the only argument.

    if base64.b64decode(cloud_event.data["message"]["data"]).decode() == "facility updates":
        wmrc_skid = Skid()
        wmrc_skid.process()
    if base64.b64decode(cloud_event.data["message"]["data"]).decode() == "validate":
        validate.run_validations()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    wmrc_skid = Skid()
    wmrc_skid.process()
