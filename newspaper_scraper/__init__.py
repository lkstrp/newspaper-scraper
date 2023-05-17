# __init__.py

from .settings import settings
from .database import Database

from .sites.spiegel import DeSpiegel
from .sites.bild import DeBild
from .sites.welt import DeWelt
from .sites.zeit import DeZeit
from .sites.handelsblatt import DeHandelsblatt
from .sites.tagesspiegel import DeTagesspiegel
from .sites.sueddeutsche import DeSueddeutsche

# Version of the newspaper_scraper package
__version__ = "0.2.1"

Spiegel = DeSpiegel
Bild = DeBild
Welt = DeWelt
Zeit = DeZeit
Handelsblatt = DeHandelsblatt
Tagesspiegel = DeTagesspiegel
Sueddeutsche = DeSueddeutsche
