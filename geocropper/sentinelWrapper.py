import geocropper.config as config
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
        product_info = self.api.download(productID, config.bigTilesDir)
        if not product_info["Online"]:
            logger.info("archived download triggered")
            return False
        else:
            # TODO: Download should be checked
            logger.info("download complete")
            return True

    def getProductData(self, productID):
        return self.api.get_product_odata(productID)

    def readyForDownload(self, productID):
        return self.api.is_online(productID)

    def requestOfflineTile(self, productID):
        # HTTP-Code 202: Accepted for retrieval
        product_info = self.api.get_product_odata(productID)
        if self.api._trigger_offline_retrieval(product_info["url"]) == 202:
            return True
        else:
            return False
