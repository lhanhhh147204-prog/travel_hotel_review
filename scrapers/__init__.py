# scrapers/__init__.py
from scrapers.booking     import scrape_booking
from scrapers.agoda       import scrape_agoda
from scrapers.tripadvisor import scrape_tripadvisor
from scrapers.google_maps import scrape_google_maps
from scrapers.ivivu       import scrape_ivivu
from scrapers.mytour      import scrape_mytour
from scrapers.traveloka   import scrape_traveloka
from scrapers.vntrip      import scrape_vntrip
from scrapers.airbnb      import scrape_airbnb

__all__ = [
    "scrape_booking",
    "scrape_agoda",
    "scrape_tripadvisor",
    "scrape_google_maps",
    "scrape_ivivu",
    "scrape_mytour",
    "scrape_traveloka",
    "scrape_vntrip",
    "scrape_airbnb",
]