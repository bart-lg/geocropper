import config
import logging
from geojson import Point
# from sentinelsat.sentinel import SentinelAPI, geojson_to_wkt, read_geojson
from sentinelsat import SentinelAPI, geojson_to_wkt


# get logger object
logger = logging.getLogger('root')


# sentinelWrapper serves as interface between this module and sentinel-api-module
class sentinelWrapper:

    def __init__(self):

        logger.info("connect to sentinel API")
        
        # connection to API for search queries and download requests
        self.api = SentinelAPI(config.copernicusUser, config.copernicusPW, config.copernicusURL)

        logger.info("sentinel API connected")


    def getSentinelProducts(self, lat, lon, dateFrom, dateTo, platform, **kwargs):
        
        logger.info("start sentinel query")

        # convert geolocation coordinates to wkt format
        footprint = geojson_to_wkt(Point((lon, lat)))
        
        # prepare parameter for cloud coverage
        if "cloudcoverpercentage" in kwargs:
            kwargs["cloudcoverpercentage"] = (0, kwargs["cloudcoverpercentage"])

        # search query
        result = self.api.query(footprint, date=(dateFrom, dateTo), platformname=platform, **kwargs)

        logger.info("sentinel query complete")

        return result


    # download multiple sentinel products (list of product IDs)
    def downloadSentinelProducts(self, products):
        logger.info("start downloading sentinel product list")
        self.api.download_all(products, config.bigTilesDir)
        logger.info("download complete")


    # download sentinel product with certain product ID
    def downloadSentinelProduct(self, productID):
        logger.info("start downloading sentinel product")
        self.api.download(productID, config.bigTilesDir)
        logger.info("download complete")
