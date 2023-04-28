# __init__.py

from .settings import settings
from .database import Database

from .sites.spiegel import DeSpiegel
from .sites.bild import DeBild
from .sites.welt import DeWelt
from .sites.zeit import DeZeit
from .sites.handelsblatt import DeHandelsblatt

# Version of the newspaper_scraper package
__version__ = "0.2.0"

Spiegel = DeSpiegel
Bild = DeBild
Welt = DeWelt
Zeit = DeZeit
Handelsblatt = DeHandelsblatt
