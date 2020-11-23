import geocropper.log as log
from tqdm import tqdm
import os
import subprocess
import pathlib
import shutil
import pyproj
from pprint import pprint
import rasterio
import math 
from shapely.geometry import Point
from shapely.geometry import Polygon
from shapely.ops import transform
from PIL import Image

from geocropper.database import database
import geocropper.config as config
import geocropper.sentinelWrapper as sentinelWrapper
import geocropper.landsatWrapper as landsatWrapper
import geocropper.csvImport as csvImport
import geocropper.utils as utils

from osgeo import gdal
# gdal library distributed by conda destroys PATH environment variable
# see -> https://github.com/OSGeo/gdal/issues/1231
# workaround: remove first entry...
# os.environ["PATH"] = os.environ["PATH"].split(';')[1]

logger = log.setupCustomLogger('main')
db = database()


def importAllCSVs(delimiter=',', quotechar='"'):
    """Import of all CSVs

    Place your CSV files in the inputCSV directory defined in the config file.
    With this function all CSVs get imported and loaded.
    This means that for all geolocations the appropriate tiles get downloaded and cropped according to the request.

    Parameters
    ----------
    delimiter : str, optional
        Used delimiter in CSV file. Default is ','
    quotechar : str, optional
        Used quote character in CSV file. Default is '"'

    """
    csvImport.importAllCSVs(delimiter, quotechar)


def init(lat, lon):
    """Initialization of a Geocropper instance.

    Parameters
    ----------
    lat : float
        Latitude of the geolocation (WGS84 decimal)
    lon : float
        Longitude of the geolocation (WGS84 decimal)

    Returns
    -------
    Geocropper
        New instance of Geocropper class with attributes lat and lon

    """
    return Geocropper(lat, lon)


class Geocropper:


    def __init__(self, lat , lon):
        self.lat = lat
        self.lon = lon
        # print("\nGeocropper initialized.")
        # print("=========================\n")
        logger.info("new geocropper instance initialized") 


    def printPosition(self):
        """Prints current location attributes of Geocropper object to console."""
        print("lat: " + str(self.lat))
        print("lon: " + str(self.lon))


    def downloadSentinelData(self, dateFrom, dateTo, platform, poiId = 0, tileLimit = 0, **kwargs):
        """Download Sentinel tiles to directory specified in the config file.

        Parameters
        ----------
        dateFrom : str
            Start date for search request in a chosen format.
            The format must be recognizable by the dateutil lib.
            In case of doubt use the format 'YYYY-MM-DD'.
        dateTo : str
            End date for search request in a chosen format.
            The format must be recognizable by the dateutil lib.
            In case of doubt use the format 'YYYY-MM-DD'.
        platform : str
            Choose between 'Sentinel-1' and 'Sentinel-2'
        poiId : int, optional
            ID of PointOfInterest record in sqlite database.
            This is primarly used by other functions to create a connection between the database records.
        tileLimit : int, optional
            Maximum number of tiles to be downloaded.
        cloudcoverpercentage : int, optional
            Value between 0 and 100 for maximum cloud cover percentage.
        producttype : str, optional
            Sentinel-1 products: RAW, SLC, GRD, OCN
                SLC: Single Look Complex
                GRD: Ground Range Detected
                OCN: Ocean
            Sentinel-2 products: S2MSI1C, S2MSI2A, S2MSI2Ap
        polarisationmode : str, optional
            Used for Sentinel-1 products:
            Accepted entries are: HH, VV, HV, VH, HH+HV, VV+VH
        sensoroperationalmode : str, optional
            Used for Sentinel-1 products:
            Accepted entries are: SM, IW, EW, WV
                SM: Stripmap
                IW: Interferometric Wide Swath 
                EW: Extra Wide Swath
                WV: Wave
        swathidentifier : str, optional
            Used for Sentinel-1 products:
            Accepted entries are: S1, S2, S3, S4, S5, S6, IW, IW1, IW2, IW3, EW, EW1, EW2, EW3, EW4, EW5
        timeliness : str, optional
            Used for Sentinel-1 products:
                NRT: NRT-3h (Near Real Time)
                NTC: Fast-24h

        Returns
        -------
        list
            list of found products (tiles)

        """

        # load sentinel wrapper

        self.sentinel = sentinelWrapper.sentinelWrapper()
        
        # convert date to required format
        dateFrom = utils.convertDate(dateFrom, "%Y%m%d")
        dateTo = utils.convertDate(dateTo, "%Y%m%d")
        

        # print search info

        print("Search for Sentinel data:")
        self.printPosition()
        print("From: " + utils.convertDate(dateFrom, "%d.%m.%Y"))
        print("To: " + utils.convertDate(dateTo, "%d.%m.%Y"))
        print("Platform: " + platform)
        if tileLimit > 0:
            print("Tile-limit: %d" % tileLimit)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                print("%s: %s" % (key, str(value)))
        print("----------------------------\n")

        logger.info("Search for Sentinel data:")
        logger.info("From: " + utils.convertDate(dateFrom, "%d.%m.%Y"))
        logger.info("To: " + utils.convertDate(dateTo, "%d.%m.%Y"))
        logger.info("Platform: " + platform)
        if tileLimit > 0:
            logger.info("Tile-limit: %d" % tileLimit)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                logger.info("%s: %s" % (key, str(value)))        
        
        
        # search for sentinel data
        
        if int(tileLimit) > 0:
            products = self.sentinel.getSentinelProducts(self.lat, self.lon, dateFrom, dateTo, platform, limit=tileLimit, **kwargs)
        else:   
            products = self.sentinel.getSentinelProducts(self.lat, self.lon, dateFrom, dateTo, platform, **kwargs)

        print("Found tiles: %d\n" % len(products))
        logger.info("Found tiles: %d\n" % len(products))


        # start download

        if len(products) > 0:

            print("Download")
            print("-----------------\n")
            
            # index i serves as a counter
            i = 1

            # key of products is product id
            for key in products:

                # start- and endtime of sensoring
                beginposition = products[key]["beginposition"]
                endposition = products[key]["beginposition"]
                
                # folder name after unzip is < SENTINEL TILE TITLE >.SAFE
                folderName = products[key]["title"] + ".SAFE"

                tileId = None
                tile = db.getTile(productId = key)
                
                # check for previous downloads
                if not pathlib.Path(config.bigTilesDir / folderName).is_dir() and \
                  not pathlib.Path(config.bigTilesDir / (products[key]["title"] + ".zip") ).is_file():
                    
                    # no previous download detected...

                    # only add new tile to database if not existing
                    # this leads automatically to a resume functionality
                    if tile == None:
                        tileId = db.addTile(platform, key, beginposition, endposition, folderName)
                    else:
                        tileId = tile["rowid"]
                        # update download request date for existing tile in database
                        db.setLastDownloadRequestForTile(tileId)

                    # check if tile ready for download
                    if self.sentinel.readyForDownload(key):

                        # download sentinel product
                        # sentinel wrapper has a resume function for incomplete downloads
                        logger.info("Download started.")
                        db.setLastDownloadRequestForTile(tileId)
                        print("[%d/%d]: Download %s" % (i, len(products), products[key]["title"]))
                        download_complete = self.sentinel.downloadSentinelProduct(key)

                        if download_complete:

                            # if downloaded zip-file could be detected set download complete date in database
                            if pathlib.Path(config.bigTilesDir / (products[key]["title"] + ".zip") ).is_file():
                                db.setDownloadCompleteForTile(tileId)

                    else:

                        lastRequest = utils.minutesSinceLastDownloadRequest()

                        if lastRequest == None or lastRequest > config.copernicusRequestDelay:

                            if self.sentinel.requestOfflineTile(key) == True:

                                # Request successful
                                db.setLastDownloadRequestForTile(tileId)
                                print("Download of archived tile triggered. Please try again between 24 hours and 3 days later.")

                            else:

                                # Request error
                                db.clearLastDownloadRequestForTile(tileId)
                                print("Download request failed! Please try again later.")

                        else:

                            print(f"There has been already a download requested in the last {config.copernicusRequestDelay} minutes! Please try later.")

                else:

                    # zip file or folder from previous download detected...

                    if tile == None:
                        # if tile not yet in database add to database
                        # this could happen if database gets reset
                        tileId = db.addTile(platform, key, beginposition, endposition, folderName)
                        db.setDownloadCompleteForTile(tileId)
                    else:
                        tileId = tile["rowid"]
                    
                    print("[%d/%d]: %s already exists." % (i, len(products), products[key]["title"]))


                # if there is a point of interest (POI) then create connection between tile and POI in database

                if int(poiId) > 0:
                    
                    tilePoi = db.getTileForPoi(poiId, tileId)
                    if tilePoi == None:
                        db.addTileForPoi(poiId, tileId)

                i += 1
            

        # disconnect sentinel wrapper
        del self.sentinel
        
        # unpack new big tiles
        utils.unpackBigTiles()
        logger.info("Big tiles unpacked.")

        # if there is a point of interest (POI) => set date for tiles identified
        # this means that all tiles for the requested POI have been identified and downloaded
        if int(poiId) > 0:
            db.setTilesIdentifiedForPoi(poiId)

        # get projections of new downloaded tiles
        utils.saveMissingTileProjections()
        
        return products

        
    def downloadLandsatData(self, dateFrom, dateTo, platform, poiId = 0, tileLimit = 0, **kwargs):
        """Download Landsat tiles to directory specified in the config file.

        Parameters
        ----------
        dateFrom : str
            Start date for search request in a chosen format.
            The format must be recognizable by the dateutil lib.
            In case of doubt use the format 'YYYY-MM-DD'.
        dateTo : str
            End date for search request in a chosen format.
            The format must be recognizable by the dateutil lib.
            In case of doubt use the format 'YYYY-MM-DD'.
        platform : str
            Choose between 'LANDSAT_TM_C1', 'LANDSAT_ETM_C1' and 'LANDSAT_8_C1'
        poiId : int, optional
            ID of PointOfInterest record in sqlite database.
            This is primarly used by other functions to create a connection between the database records.
        tileLimit : int, optional
            Maximum number of tiles to be downloaded.
        cloudcoverpercentage : int, optional
            Value between 0 and 100 for maximum cloud cover percentage.

        Returns
        -------
        list
            list of found products (tiles)

        """

        # load landsat wrapper

        self.landsat = landsatWrapper.landsatWrapper()

        # default max cloud coverage is set to 100
        maxCloudCoverage = 100

        # convert date to required format
        dateFrom = utils.convertDate(dateFrom)
        dateTo = utils.convertDate(dateTo)


        # print search info

        print("Search for Landsat data:")
        self.printPosition()
        print("From: " + utils.convertDate(dateFrom, "%d.%m.%Y"))
        print("To: " + utils.convertDate(dateTo, "%d.%m.%Y"))
        print("Platform: " + platform)
        if tileLimit > 0:
            print("Tile-limit: %d" % tileLimit)
        for key, value in kwargs.items():
            if key == "cloudcoverpercentage":
                maxCloudCoverage = value
                print("%s: %s" %(key, str(value)))
        print("----------------------------\n")


        # search for landsat data
        
        products = self.landsat.getLandsatProducts(self.lat, self.lon, dateFrom, dateTo, platform, maxCloudCoverage, tileLimit)
        print("Found tiles: %d\n" % len(products))


        # start download

        if len(products) > 0:

            print("Download")
            print("-----------------\n")
            
            # index i serves as a counter
            i = 1

            for product in products:
                
                folderName = product["displayId"]

                # start- and endtime of sensoring
                beginposition = product["startTime"]
                endposition = product["endTime"]

                tileId = None
                tile = db.getTile(productId = product["entityId"])

                # TODO: check if existing tar file is complete => needs to be deleted and re-downloaded

                # check for previous downloads
                if not pathlib.Path(config.bigTilesDir / folderName).is_dir():             

                    # no previous download detected...

                    # only add new tile to database if not existing
                    # this leads automatically to a resume functionality
                    if tile == None:
                        tileId = db.addTile(platform, product["entityId"], beginposition, endposition, folderName)
                    else:
                        tileId = tile["rowid"]
                        # update download request date for existing tile in database
                        db.setLastDownloadRequestForTile(tileId)

                    # download landsat product
                    # landsat wrapper has NO resume function for incomplete downloads
                    logger.info("Download started.")
                    print("[%d/%d]: Download %s" % (i, len(products), product["displayId"]))
                    self.landsat.downloadLandsatProduct(product["entityId"])

                    # if downloaded tar-file could be detected set download complete date in database
                    if pathlib.Path(config.bigTilesDir / (product["displayId"] + ".tar.gz") ).is_file():
                        db.setDownloadCompleteForTile(tileId)

                else:

                    # tar file or folder from previous download detected...

                    if tile == None:
                        # if tile not yet in database add to database
                        # this could happen if database gets reset
                        tileId = db.addTile(platform, product["entityId"], beginposition, endposition, folderName)
                        db.setDownloadCompleteForTile(tileId)
                    else:
                        tileId = tile["rowid"]
                    
                    print("[%d/%d]: %s already exists." % (i, len(products), product["displayId"]))                    


                # if there is a point of interest (POI) then create connection between tile and POI in database

                if int(poiId) > 0:
                    
                    tilePoi = db.getTileForPoi(poiId, tileId)
                    if tilePoi == None:
                        db.addTileForPoi(poiId, tileId)     
                        
                i += 1       


        # disconnect landsat wrapper
        del self.landsat

        # unpack new big tiles
        utils.unpackBigTiles()
        logger.info("Big tiles unpacked.")
        
        # if there is a point of interest (POI) => set date for tiles identified
        # this means that all tiles for the requested POI have been identified and downloaded
        if int(poiId) > 0:
           db.setTilesIdentifiedForPoi(poiId)
        
        return products        


    def downloadAndCrop(self, groupname, dateFrom, dateTo, platform, width, height, tileLimit = 0, **kwargs):
        """Download and crop/clip Sentinel or Landsat tiles to directories specified in the config file.

        Parameters
        ----------
        groupname : str
            Short name to group datasets (groupname is used for folder structure in cropped tiles)
        dateFrom : str
            Start date for search request in a chosen format.
            The format must be recognizable by the dateutil lib.
            In case of doubt use the format 'YYYY-MM-DD'.
        dateTo : str
            End date for search request in a chosen format.
            The format must be recognizable by the dateutil lib.
            In case of doubt use the format 'YYYY-MM-DD'.
        platform : str
            Choose between 'Sentinel-1', 'Sentinel-2', 'LANDSAT_TM_C1', 'LANDSAT_ETM_C1' and 'LANDSAT_8_C1'
        width : int
            Width of cropped rectangle. The rectangle surrounds the given geolocation (center point).
        height : int
            Heigth of cropped rectangle. The rectangle surrounds the given geolocation (center point).
        tileLimit : int, optional
            Maximum number of tiles to be downloaded.
        cloudcoverpercentage : int, optional
            Value between 0 and 100 for maximum cloud cover percentage.
        producttype : str, optional
            Sentinel-1 products: RAW, SLC, GRD, OCN
                SLC: Single Look Complex
                GRD: Ground Range Detected
                OCN: Ocean
            Sentinel-2 products: S2MSI1C, S2MSI2A, S2MSI2Ap
        polarisationmode : str, optional
            Used for Sentinel-1 products:
            Accepted entries are: HH, VV, HV, VH, HH+HV, VV+VH
        sensoroperationalmode : str, optional
            Used for Sentinel-1 products:
            Accepted entries are: SM, IW, EW, WV
                SM: Stripmap
                IW: Interferometric Wide Swath 
                EW: Extra Wide Swath
                WV: Wave
        swathidentifier : str, optional
            Used for Sentinel-1 products:
            Accepted entries are: S1, S2, S3, S4, S5, S6, IW, IW1, IW2, IW3, EW, EW1, EW2, EW3, EW4, EW5
        timeliness : str, optional
            Used for Sentinel-1 products:
                NRT: NRT-3h (Near Real Time)
                NTC: Fast-24h

        Returns
        -------
        int
            number of found and downloaded tiles

        """


        # convert date formats
        dateFrom = utils.convertDate(dateFrom)
        dateTo = utils.convertDate(dateTo)


        # check if point of interest (POI) exists in database
        # if not, create new POI record

        poi = db.getPoi(groupname, self.lat, self.lon, dateFrom, dateTo, platform, width, height, tileLimit=tileLimit, **kwargs)

        if poi == None:     
            poiId = db.addPoi(groupname, self.lat, self.lon, dateFrom, dateTo, platform, width, height, tileLimit, **kwargs)
        else:
            poiId = poi["rowid"]


        # search and download tiles

        products = None

        if platform.startswith("Sentinel"):
            products = self.downloadSentinelData(dateFrom, dateTo, platform, poiId=poiId, tileLimit=tileLimit, **kwargs)
        
        if platform.startswith("LANDSAT"):
            products = self.downloadLandsatData(dateFrom, dateTo, platform, poiId=poiId, tileLimit=tileLimit, **kwargs)


        # if tiles found, unpack and crop them

        if not products == None and len(products) > 0:
            
            utils.cropTiles(poiId)
            logger.info("Tiles cropped.")

        
        if products == None:
            return 0
        else:
            return len(products)

