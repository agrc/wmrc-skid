"""
config.py: Configuration values. Secrets to be handled with Secrets Manager
"""

import logging
import socket

SKID_NAME = 'wmrc'

AGOL_ORG = 'https://utahdeq.maps.arcgis.com'
SENDGRID_SETTINGS = {  #: Settings for SendGridHandler
    'from_address': 'noreply@utah.gov',
    'to_addresses': 'jdadams@utah.gov',
    'prefix': f'{SKID_NAME} on {socket.gethostname()}: ',
}
LOG_LEVEL = logging.DEBUG
LOG_FILE_NAME = 'log'

FEATURE_LAYER_ITEMID = '4df06137fb0a45459e49107a5f47a326'  #: Beta version
# FEATURE_LAYER_ITEMID = '056bbc52ff3240f6b69666750a61aeff'  #: Live version
JOIN_COLUMN = 'id_'
ATTACHMENT_LINK_COLUMN = ''
ATTACHMENT_PATH_COLUMN = ''
FIELDS = {}
