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
    from . import config, version
except ImportError:
    import config
    import version


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
    secret_folder = Path(__file__).parent / "secrets"
    if secret_folder.exists():
        return json.loads((secret_folder / "secrets.json").read_text(encoding="utf-8"))

    raise FileNotFoundError("Secrets folder not found; secrets not loaded.")


def _initialize(log_path, sendgrid_api_key):
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

    log_handler = logging.FileHandler(log_path, mode="w")
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
    skid_supervisor = Supervisor(handle_errors=False)
    sendgrid_settings = config.SENDGRID_SETTINGS
    sendgrid_settings["api_key"] = sendgrid_api_key
    skid_supervisor.add_message_handler(
        SendGridHandler(
            sendgrid_settings=sendgrid_settings, client_name=config.SKID_NAME, client_version=version.__version__
        )
    )

    return skid_supervisor


def _remove_log_file_handlers(log_name, loggers):
    """A helper function to remove the file handlers so the tempdir will close correctly

    Args:
        log_name (str): The logfiles filename
        loggers (List<str>): The loggers that are writing to log_name
    """

    for logger in loggers:
        for handler in logger.handlers:
            try:
                if log_name in handler.stream.name:
                    logger.removeHandler(handler)
                    handler.close()
            except Exception:
                pass


def process():
    """The main function that does all the work."""

    #: Set up secrets, tempdir, supervisor, and logging
    start = datetime.now()

    secrets = SimpleNamespace(**_get_secrets())

    with TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        log_name = f'{config.LOG_FILE_NAME}_{start.strftime("%Y%m%d-%H%M%S")}.txt'
        log_path = tempdir_path / log_name

        skid_supervisor = _initialize(log_path, secrets.SENDGRID_API_KEY)
        module_logger = logging.getLogger(config.SKID_NAME)

        # module_logger.info(pprint.pformat(dict(os.environ)))
        # module_logger.info(locale.getdefaultlocale())
        # module_logger.info(locale.getlocale(locale.LC_NUMERIC))

        #: Get our GIS object via the ArcGIS API for Python
        gis = arcgis.gis.GIS(config.AGOL_ORG, secrets.AGOL_USER, secrets.AGOL_PASSWORD)

        #: Do the work
        module_logger.info("Loading data from Google Sheets...")
        combined_df = _parse_from_google_sheets(secrets)
        module_logger.info("Adding county names from SGID county boundaries...")
        with_counties_df = _get_county_names(combined_df, gis)

        module_logger.info("Preparing data for truncate and load...")
        proj_df = with_counties_df.copy()
        proj_df.spatial.project(4326)
        proj_df.spatial.set_geometry("SHAPE")
        proj_df.spatial.sr = {"wkid": 4326}
        proj_df["last_updated"] = date.today()
        proj_df = transform.DataCleaning.switch_to_datetime(proj_df, ["last_updated"])
        proj_df = transform.DataCleaning.switch_to_float(
            proj_df,
            [
                "latitude",
                "longitude",
                "tons_of_material_diverted_from_",
                "gallons_of_used_oil_collected_for_recycling_last_year",
            ],
        )

        module_logger.info("Truncating and loading...")
        updater = load.FeatureServiceUpdater(gis, config.FEATURE_LAYER_ITEMID, tempdir)
        load_count = updater.truncate_and_load_features(proj_df)

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
            f"Rows loaded: {load_count}",
        ]

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = tempdir_path / log_name

        skid_supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        _remove_log_file_handlers(log_name, loggers)


def _parse_from_google_sheets(secrets):
    #: Get individual sheets
    gsheet_extractor = extract.GSheetLoader(secrets.SERVICE_ACCOUNT_JSON)
    sw_df = gsheet_extractor.load_specific_worksheet_into_dataframe(secrets.SHEET_ID, "SW Facilities", by_title=True)
    uocc_df = gsheet_extractor.load_specific_worksheet_into_dataframe(secrets.SHEET_ID, "UOCCs", by_title=True)

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

    return renamed_df


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
    process()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    process()
