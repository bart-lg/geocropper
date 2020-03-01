import config
import logging
import landsatxplore.api
from landsatxplore.earthexplorer import EarthExplorer


# get logger object
logger = logging.getLogger('root')


# landsatWrapper serves as interface between this module and landsat-api-module
class landsatWrapper:

    def __init__(self):
        
        logger.info("connect to landsat API")

        # connection to API for search queries
        self.api = landsatxplore.api.API(config.usgsUser, config.usgsPW)

        # connection to EarthExplorer for download requests
        self.earthExplorer = EarthExplorer(config.usgsUser, config.usgsPW)

        logger.info("landsat API connected")


    def __del__(self):
        # logout from API and EarthExplorer
        self.api.logout()
        self.earthExplorer.logout()

    
    def getLandsatProducts(self, lat, lon, dateFrom, dateTo, platform, maxCloudCoverage = 100, limit = 0):
        # datasets: [LANDSAT_TM_C1|LANDSAT_ETM_C1|LANDSAT_8_C1]
        # format for dates: 'YYYY-MM-DD'
    
        logger.info("start landsat query")
        
        if int(limit) > 0:
            scenes = self.api.search(
                dataset=platform,
                latitude=lat,
                longitude=lon,
                start_date=dateFrom,
                end_date=dateTo,
                max_cloud_cover=maxCloudCoverage,
                max_results=limit)
        else:
            scenes = self.api.search(
                dataset=platform,
                latitude=lat,
                longitude=lon,
                start_date=dateFrom,
                end_date=dateTo,
                max_cloud_cover=maxCloudCoverage)        
        
        return scenes


    def downloadLandsatProduct(self, sceneId):
        self.earthExplorer.download(scene_id=sceneId, output_dir=config.bigTilesDir)
        