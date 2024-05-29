"""
config.py: Configuration values. Secrets to be handled with Secrets Manager
"""

import logging
import socket
import urllib.request

SKID_NAME = "wmrc"
#: Try to get project id from GCP metadata server for hostname. If it's empty or errors out, revert to local hostname
try:
    url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    req = urllib.request.Request(url)
    req.add_header("Metadata-Flavor", "Google")
    project_id = urllib.request.urlopen(req).read().decode()
    if not project_id:
        raise ValueError
    HOST_NAME = project_id
except Exception:
    HOST_NAME = socket.gethostname()

AGOL_ORG = "https://utahdeq.maps.arcgis.com"
SENDGRID_SETTINGS = {  #: Settings for SendGridHandler
    "from_address": "noreply@utah.gov",
    "to_addresses": ["jdadams@utah.gov"],
    # "to_addresses": ["jdadams@utah.gov", "stevienorcross@utah.gov", "gerardorodriguez@utah.gov"],
    "prefix": f"{SKID_NAME} on {HOST_NAME}: ",
}
LOG_LEVEL = logging.DEBUG
LOG_FILE_NAME = "log"

COUNTIES_ITEMID = "90431cac2f9f49f4bcf1505419583753"

FACILITIES_LAYER_ITEMID = "4df06137fb0a45459e49107a5f47a326"  #: Beta version
# FACILITIES_LAYER_ITEMID = "056bbc52ff3240f6b69666750a61aeff"  #: Live version
JOIN_COLUMN = "id_"

COUNTY_LAYER_ITEMID = "ea80c3c34bb147fba462ea100179bf09"
COUNTY_BOUNDARIES_ITEMID = "4d0671aac09347d3a03415632f19ddb3"  #: Must include a feature with the name "Out of State"

MATERIALS_LAYER_ITEMID = "7e13c71b32284915bad47983aca3602b"
COMPOSTING_LAYER_ITEMID = "9b543c20de704839b90502a49ce1b8f3"
STATEWIDE_LAYER_ITEMID = "a5abaa46850e46ff88ee6024ce21f52e"

YEAR = 2023
