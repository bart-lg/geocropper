#import sys
import config
#from geojson import Point

import logging

logger = logging.getLogger('root')

import landsatxplore.api
from landsatxplore.earthexplorer import EarthExplorer

class landsatWrapper:

    def __init__(self):
        logger.info("connect to landsat API")
        self.api = landsatxplore.api.API(config.usgsUser, config.usgsPW)
        self.earthExplorer = EarthExplorer(config.usgsUser, config.usgsPW)
        logger.info("landsat API connected")
        
    def __del__(self):        
        self.api.logout()
        self.earthExplorer.logout()
    
    def getLandsatProducts(self, lat, lon, fromDate, toDate, platform, maxCloudCoverage = 100, limit = 0):
        # datasets: [LANDSAT_TM_C1|LANDSAT_ETM_C1|LANDSAT_8_C1]
        # format for dates: 'YYYY-MM-DD'
    
        logger.info("start landsat query")
        
        if int(limit) > 0:
            scenes = self.api.search(
                dataset=platform,
                latitude=lat,
                longitude=lon,
                start_date=fromDate,
                end_date=toDate,
                max_cloud_cover=maxCloudCoverage,
                max_results=limit)
        else:
            scenes = self.api.search(
                dataset=platform,
                latitude=lat,
                longitude=lon,
                start_date=fromDate,
                end_date=toDate,
                max_cloud_cover=maxCloudCoverage)        
        
        return scenes

    def downloadLandsatProduct(self, sceneId):
        self.earthExplorer.download(scene_id=sceneId, output_dir=config.bigTilesDir)
        