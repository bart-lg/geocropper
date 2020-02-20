import sys
import config
from geojson import Point

# from sentinelsat.sentinel import SentinelAPI, geojson_to_wkt, read_geojson
from sentinelsat import SentinelAPI, geojson_to_wkt

class sentinelWrapper:

    def __init__(self):
        self.api = SentinelAPI(config.copernicusUser, config.copernicusPW, config.copernicusURL)

    def getSentinelProducts(self, lat, lon, fromDate, toDate, platform, **kwargs):
        
        footprint = geojson_to_wkt(Point((lon, lat)))
        
        if "cloudcoverpercentage" in kwargs:
            kwargs["cloudcoverpercentage"] = (0, kwargs["cloudcoverpercentage"])

        return self.api.query(footprint,
                date=(fromDate, toDate),
                platformname=platform,
                **kwargs)

    def downloadSentinelProducts(self, products):
        self.api.download_all(products, config.bigTilesDir)

    def downloadSentinelProduct(self, productID):
        self.api.download(productID, config.bigTilesDir)

