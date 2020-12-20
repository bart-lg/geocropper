import geocropper.config as config
import logging
import landsatxplore.api
from landsatxplore.earthexplorer import EarthExplorer


# get logger object
logger = logging.getLogger('root')


# landsatWrapper serves as interface between this module and landsat-api-module
class LandsatWrapper:

    def __init__(self):
        
        logger.info("connect to landsat API")

        # connection to API for search queries
        self.api = landsatxplore.api.API(config.usgsUser, config.usgsPW)

        # connection to EarthExplorer for download requests
        self.earth_explorer = EarthExplorer(config.usgsUser, config.usgsPW)

        logger.info("landsat API connected")


    def __del__(self):
        # logout from API and EarthExplorer
        self.api.logout()
        self.earth_explorer.logout()

    
    def get_landsat_products(self, lat, lon, date_from, date_to, platform, max_cloud_coverage = 100, limit = 0):
        # datasets: [LANDSAT_TM_C1|LANDSAT_ETM_C1|LANDSAT_8_C1]
        # format for dates: 'YYYY-MM-DD'
    
        logger.info("start landsat query")
        
        if int(limit) > 0:
            scenes = self.api.search(
                dataset=platform,
                latitude=lat,
                longitude=lon,
                start_date=date_from,
                end_date=date_to,
                max_cloud_cover=max_cloud_coverage,
                max_results=limit)
        else:
            scenes = self.api.search(
                dataset=platform,
                latitude=lat,
                longitude=lon,
                start_date=date_from,
                end_date=date_to,
                max_cloud_cover=maxCloudCoverage)        
        
        return scenes


    def download_landsat_product(self, scene_id):
        self.earth_explorer.download(scene_id=scene_id, output_dir=config.bigTilesDir)
        