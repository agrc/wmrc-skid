import logging
import os
import pprint

logging.basicConfig(level=logging.INFO)
logging.info(pprint.pformat(dict(os.environ)))
