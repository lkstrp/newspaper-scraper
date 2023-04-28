# __init__.py

from .settings import settings
from .database import Database

from .sites.spiegel import DeSpiegel
from .sites.bild import DeBild
from .sites.welt import DeWelt
from .sites.zeit import DeZeit

# Version of the newspaper_scraper package
__version__ = "0.1.3"

Spiegel = DeSpiegel
Bild = DeBild
Welt = DeWelt
Zeit = DeZeit
