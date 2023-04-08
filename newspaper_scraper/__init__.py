# __init__.py

from .settings import settings

from .sites.spiegel import DeSpiegel
from .sites.bild import DeBild
from .sites.welt import DeWelt

# Version of the newspaper_scraper package
__version__ = "0.1.3"

Spiegel = DeSpiegel
Bild = DeBild
Welt = DeWelt
