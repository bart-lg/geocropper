import sys
import config
from geojson import Point

import logging

logger = logging.getLogger('root')

# from sentinelsat.sentinel import SentinelAPI, geojson_to_wkt, read_geojson
from sentinelsat import SentinelAPI, geojson_to_wkt

class sentinelWrapper:

    def __init__(self):
        logger.info("connect to sentinel API")
        self.api = SentinelAPI(config.copernicusUser, config.copernicusPW, config.copernicusURL)
        logger.info("sentinel API connected")

    def getSentinelProducts(self, lat, lon, fromDate, toDate, platform, **kwargs):
        
        logger.info("start sentinel query")

        footprint = geojson_to_wkt(Point((lon, lat)))
        
        if "cloudcoverpercentage" in kwargs:
            kwargs["cloudcoverpercentage"] = (0, kwargs["cloudcoverpercentage"])

        result = self.api.query(footprint, date=(fromDate, toDate), platformname=platform, **kwargs)

        logger.info("sentinel query complete")

        return result

    def downloadSentinelProducts(self, products):
        logger.info("start downloading sentinel product list")
        self.api.download_all(products, config.bigTilesDir)
        logger.info("download complete")

    def downloadSentinelProduct(self, productID):
        logger.info("start downloading sentinel product")
        self.api.download(productID, config.bigTilesDir)
        logger.info("download complete")

