import geocropper.config as config
import geocropper.utils as utils
import logging
from geojson import Point
from datetime import datetime
# from sentinelsat.sentinel import SentinelAPI, geojson_to_wkt, read_geojson
from sentinelsat import SentinelAPI, geojson_to_wkt


# get logger object
logger = logging.getLogger('root')


# SentinelWrapper serves as interface between this module and sentinel-api-module
class SentinelWrapper:

    def __init__(self):

        logger.info("connect to sentinel API")
        
        # connection to API for search queries and download requests
        self.api = SentinelAPI(config.copernicusUser, config.copernicusPW, config.copernicusURL)

        logger.info("sentinel API connected")


    def get_sentinel_products(self, lat, lon, date_from, date_to, platform, **kwargs):
        
        logger.info("start sentinel query")

        # convert geolocation coordinates to wkt format
        footprint = geojson_to_wkt(Point((lon, lat)))
        
        # prepare parameter for cloud coverage
        if "cloudcoverpercentage" in kwargs:
            kwargs["cloudcoverpercentage"] = (0, kwargs["cloudcoverpercentage"])

        # search query
        result = self.api.query(footprint, date=(date_from, date_to), platformname=platform, **kwargs)

        logger.info("sentinel query complete")

        return result


    # download multiple sentinel products (list of product IDs)
    def download_sentinel_products(self, products):
        logger.info("start downloading sentinel product list")
        self.api.download_all(products, config.bigTilesDir)
        logger.info("download complete")


    # download sentinel product with certain product ID
    def download_sentinel_product(self, product_id):
        logger.info("start downloading sentinel product")
        product_info = self.api.download(product_id, config.bigTilesDir)
        if not product_info["Online"]:
            logger.info("archived download triggered")
            return False
        else:
            # TODO: Download should be checked
            logger.info("download complete")
            return True

    def get_product_data(self, product_id):
        return self.api.get_product_odata(product_id)

    def ready_for_download(self, product_id):
        return self.api.is_online(product_id)

    def request_offline_tile(self, last_tile_download_request, product_id):

        # check if last request not within request delay
        last_request = utils.minutes_since_last_download_request()
        if last_request == None or last_request > config.copernicusRequestDelay:

            if last_tile_download_request == None or \
                    utils.minutes_since_timestamp(last_tile_download_request) > config.copernicusRepeatRequestAfterMin:

                # HTTP-Code 202: Accepted for retrieval
                # TODO: handle other HTTP-Codes as well...
                product_info = self.api.get_product_odata(product_id)
                if self.api._trigger_offline_retrieval(product_info["url"]) == 202:
                    return True
                else:
                    return False               

        else:

            return False



