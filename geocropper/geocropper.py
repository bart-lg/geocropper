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

from geocropper.database import Database
import geocropper.config as config
import geocropper.sentinelWrapper as sentinelWrapper
import geocropper.landsatWrapper as landsatWrapper
import geocropper.asfWrapper as asfWrapper
import geocropper.csvImport as csvImport
import geocropper.utils as utils

from osgeo import gdal
# gdal library distributed by conda destroys PATH environment variable
# see -> https://github.com/OSGeo/gdal/issues/1231
# workaround: remove first entry...
# os.environ["PATH"] = os.environ["PATH"].split(';')[1]

logger = log.setup_custom_logger('main')
db = Database()


def import_all_csvs(delimiter=',', quotechar='"'):
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
    csvImport.import_all_csvs(delimiter, quotechar)


def download_sentinel_data(lat, lon, date_from, date_to, platform, poi_id = 0, tile_limit = 0, **kwargs):
    """Download Sentinel tiles to directory specified in the config file.

    Parameters
    ----------
    lat : float
        Latitude of the geolocation (WGS84 decimal).
    lon : float
        Longitude of the geolocation (WGS84 decimal).
    date_from : str
        Start date for search request in a chosen format.
        The format must be recognizable by the dateutil lib.
        In case of doubt use the format 'YYYY-MM-DD'.
    date_to : str
        End date for search request in a chosen format.
        The format must be recognizable by the dateutil lib.
        In case of doubt use the format 'YYYY-MM-DD'.
    platform : str
        Choose between 'Sentinel-1' and 'Sentinel-2'
    poi_id : int, optional
        ID of PointOfInterest record in sqlite database.
        This is primarly used by other functions to create a connection between the database records.
    tile_limit : int, optional
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

    sentinel = sentinelWrapper.SentinelWrapper()
    asf = asfWrapper.AsfWrapper()
    
    # convert date to required format
    date_from = utils.convert_date(date_from, "%Y%m%d")
    date_to = utils.convert_date(date_to, "%Y%m%d")
    

    # print search info

    print("Search for Sentinel data:")
    print("lat: " + str(lat))
    print("lon: " + str(lon))
    print("From: " + utils.convert_date(date_from, "%d.%m.%Y"))
    print("To: " + utils.convert_date(date_to, "%d.%m.%Y"))
    print("Platform: " + platform)
    if tile_limit > 0:
        print("Tile-limit: %d" % tile_limit)
    for key, value in kwargs.items():
        if key in config.optionalSentinelParameters:
            print("%s: %s" % (key, str(value)))
    print("----------------------------\n")

    logger.info("Search for Sentinel data:")
    logger.info("From: " + utils.convert_date(date_from, "%d.%m.%Y"))
    logger.info("To: " + utils.convert_date(date_to, "%d.%m.%Y"))
    logger.info("Platform: " + platform)
    if tile_limit > 0:
        logger.info("Tile-limit: %d" % tile_limit)
    for key, value in kwargs.items():
        if key in config.optionalSentinelParameters:
            logger.info("%s: %s" % (key, str(value)))        
    
    
    # search for sentinel data
    
    if int(tile_limit) > 0:
        products = sentinel.get_sentinel_products(lat, lon, date_from, date_to, platform, limit=tile_limit, **kwargs)
    else:   
        products = sentinel.get_sentinel_products(lat, lon, date_from, date_to, platform, **kwargs)

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
            folder_name = products[key]["title"] + ".SAFE"

            tile_id = None
            tile = db.get_tile(product_id = key)
            
            # check for previous downloads
            if not pathlib.Path(config.bigTilesDir / folder_name).is_dir() and \
               not pathlib.Path(config.bigTilesDir / (products[key]["title"] + ".zip") ).is_file():
                
                # no previous download detected...

                # only add new tile to database if not existing
                # this leads automatically to a resume functionality
                if tile == None:
                    tile_id = db.add_tile(platform, key, beginposition, endposition, folder_name)
                else:
                    tile_id = tile["rowid"]
                    # update download request date for existing tile in database
                    db.set_last_download_request_for_tile(tile_id)

                granule = products[key]["title"]

                # check if tile ready for download
                if sentinel.ready_for_download(key):

                    # download sentinel product
                    # sentinel wrapper has a resume function for incomplete downloads
                    logger.info("Download started.")
                    db.set_last_download_request_for_tile(tile_id)
                    print("[%d/%d]: Download %s" % (i, len(products), granule))
                    download_complete = sentinel.download_sentinel_product(key)

                    if download_complete:

                        # if downloaded zip-file could be detected set download complete date in database
                        if pathlib.Path(config.bigTilesDir / (granule + ".zip") ).is_file():
                            db.set_download_complete_for_tile(tile_id)

                else:

                    if granule.startswith("S1") and asf.download_S1_tile(granule, config.bigTilesDir):

                        db.set_download_complete_for_tile(tile_id)
                        print(f"Tile {granule} downloaded from Alaska Satellite Facility")

                    else:

                        last_request = utils.minutes_since_last_download_request()

                        if last_request == None or last_request > config.copernicusRequestDelay:

                            if sentinel.request_offline_tile(key) == True:

                                # Request successful
                                db.set_last_download_request_for_tile(tile_id)
                                print("Download of archived tile triggered. Please try again between 24 hours and 3 days later.")

                            else:

                                # Request error
                                db.clear_last_download_request_for_tile(tile_id)
                                print("Download request failed! Please try again later.")

                        else:

                            print(f"There has been already a download requested in the last {config.copernicusRequestDelay} minutes! Please try later.")

            else:

                # zip file or folder from previous download detected...

                if tile == None:
                    # if tile not yet in database add to database
                    # this could happen if database gets reset
                    tile_id = db.add_tile(platform, key, beginposition, endposition, folder_name)
                    db.set_download_complete_for_tile(tile_id)
                else:
                    tile_id = tile["rowid"]
                
                print("[%d/%d]: %s already exists." % (i, len(products), products[key]["title"]))


            # if there is a point of interest (POI) then create connection between tile and POI in database

            if int(poi_id) > 0:
                
                tile_poi = db.get_tile_for_poi(poi_id, tile_id)
                if tile_poi == None:
                    db.add_tile_for_poi(poi_id, tile_id)

            i += 1
        

    # disconnect sentinel wrapper
    del sentinel
    
    # unpack new big tiles
    utils.unpack_big_tiles()
    logger.info("Big tiles unpacked.")

    # if there is a point of interest (POI) => set date for tiles identified
    # this means that all tiles for the requested POI have been identified
    if int(poi_id) > 0:
        db.set_tiles_identified_for_poi(poi_id)

    # get projections of new downloaded tiles
    utils.save_missing_tile_projections()
    
    return products

        
def download_landsat_data(lat, lon, date_from, date_to, platform, poi_id = 0, tile_limit = 0, **kwargs):
    """Download Landsat tiles to directory specified in the config file.

    Parameters
    ----------
    lat : float
        Latitude of the geolocation (WGS84 decimal).
    lon : float
        Longitude of the geolocation (WGS84 decimal).        
    date_from : str
        Start date for search request in a chosen format.
        The format must be recognizable by the dateutil lib.
        In case of doubt use the format 'YYYY-MM-DD'.
    date_to : str
        End date for search request in a chosen format.
        The format must be recognizable by the dateutil lib.
        In case of doubt use the format 'YYYY-MM-DD'.
    platform : str
        Choose between 'LANDSAT_TM_C1', 'LANDSAT_ETM_C1' and 'LANDSAT_8_C1'
    poi_id : int, optional
        ID of PointOfInterest record in sqlite database.
        This is primarly used by other functions to create a connection between the database records.
    tile_limit : int, optional
        Maximum number of tiles to be downloaded.
    cloudcoverpercentage : int, optional
        Value between 0 and 100 for maximum cloud cover percentage.

    Returns
    -------
    list
        list of found products (tiles)

    """

    # load landsat wrapper

    landsat = landsatWrapper.LandsatWrapper()

    # default max cloud coverage is set to 100
    max_cloud_coverage = 100

    # convert date to required format
    date_from = utils.convert_date(date_from)
    date_to = utils.convert_date(date_to)


    # print search info

    print("Search for Landsat data:")
    print("lat: " + str(lat))
    print("lon: " + str(lon))
    print("From: " + utils.convert_date(date_from, "%d.%m.%Y"))
    print("To: " + utils.convert_date(date_to, "%d.%m.%Y"))
    print("Platform: " + platform)
    if tile_limit > 0:
        print("Tile-limit: %d" % tile_limit)
    for key, value in kwargs.items():
        if key == "cloudcoverpercentage":
            max_cloud_coverage = value
            print("%s: %s" %(key, str(value)))
    print("----------------------------\n")


    # search for landsat data
    
    products = landsat.get_landsat_products(lat, lon, date_from, date_to, platform, max_cloud_coverage, tile_limit)
    print("Found tiles: %d\n" % len(products))


    # start download

    if len(products) > 0:

        print("Download")
        print("-----------------\n")
        
        # index i serves as a counter
        i = 1

        for product in products:
            
            folder_name = product["displayId"]

            # start- and endtime of sensoring
            beginposition = product["startTime"]
            endposition = product["endTime"]

            tile_id = None
            tile = db.get_tile(product_id = product["entityId"])

            # TODO: check if existing tar file is complete => needs to be deleted and re-downloaded

            # check for previous downloads
            if not pathlib.Path(config.bigTilesDir / folder_name).is_dir():             

                # no previous download detected...

                # only add new tile to database if not existing
                # this leads automatically to a resume functionality
                if tile == None:
                    tile_id = db.add_tile(platform, product["entityId"], beginposition, endposition, folder_name)
                else:
                    tile_id = tile["rowid"]
                    # update download request date for existing tile in database
                    db.set_last_download_request_for_tile(tile_id)

                # download landsat product
                # landsat wrapper has NO resume function for incomplete downloads
                logger.info("Download started.")
                print("[%d/%d]: Download %s" % (i, len(products), product["displayId"]))
                landsat.download_landsat_product(product["entityId"])

                # if downloaded tar-file could be detected set download complete date in database
                if pathlib.Path(config.bigTilesDir / (product["displayId"] + ".tar.gz") ).is_file():
                    db.set_download_complete_for_tile(tile_id)

            else:

                # tar file or folder from previous download detected...

                if tile == None:
                    # if tile not yet in database add to database
                    # this could happen if database gets reset
                    tile_id = db.add_tile(platform, product["entityId"], beginposition, endposition, folder_name)
                    db.set_download_complete_for_tile(tile_id)
                else:
                    tile_id = tile["rowid"]
                
                print("[%d/%d]: %s already exists." % (i, len(products), product["displayId"]))                    


            # if there is a point of interest (POI) then create connection between tile and POI in database

            if int(poi_id) > 0:
                
                tile_poi = db.get_tile_for_poi(poi_id, tile_id)
                if tile_poi == None:
                    db.add_tile_for_poi(poi_id, tile_id)     
                    
            i += 1       


    # disconnect landsat wrapper
    del landsat

    # unpack new big tiles
    utils.unpack_big_tiles()
    logger.info("Big tiles unpacked.")
    
    # if there is a point of interest (POI) => set date for tiles identified
    # this means that all tiles for the requested POI have been identified and downloaded
    if int(poi_id) > 0:
       db.set_tiles_identified_for_poi(poi_id)
    
    return products        


def download_and_crop(lat, lon, groupname, date_from, date_to, platform, width, height, tile_limit = 0, **kwargs):
    """Download and crop/clip Sentinel or Landsat tiles to directories specified in the config file.

    Parameters
    ----------
    lat : float
        Latitude of the geolocation (WGS84 decimal).
    lon : float
        Longitude of the geolocation (WGS84 decimal).         
    groupname : str
        Short name to group datasets (groupname is used for folder structure in cropped tiles)
    date_from : str
        Start date for search request in a chosen format.
        The format must be recognizable by the dateutil lib.
        In case of doubt use the format 'YYYY-MM-DD'.
    date_to : str
        End date for search request in a chosen format.
        The format must be recognizable by the dateutil lib.
        In case of doubt use the format 'YYYY-MM-DD'.
    platform : str
        Choose between 'Sentinel-1', 'Sentinel-2', 'LANDSAT_TM_C1', 'LANDSAT_ETM_C1' and 'LANDSAT_8_C1'
    width : int
        Width of cropped rectangle. The rectangle surrounds the given geolocation (center point).
    height : int
        Heigth of cropped rectangle. The rectangle surrounds the given geolocation (center point).
    tile_limit : int, optional
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
    date_from = utils.convert_date(date_from)
    date_to = utils.convert_date(date_to)


    # check if point of interest (POI) exists in database
    # if not, create new POI record

    poi = db.get_poi(groupname, lat, lon, date_from, date_to, platform, width, height, tile_limit=tile_limit, **kwargs)

    if poi == None:     
        poi_id = db.add_poi(groupname, lat, lon, date_from, date_to, platform, width, height, tile_limit, **kwargs)
    else:
        poi_id = poi["rowid"]


    # search and download tiles

    products = None

    if platform.startswith("Sentinel"):
        products = download_sentinel_data(lat, lon, date_from, date_to, platform, poi_id=poi_id, tile_limit=tile_limit, **kwargs)
    
    if platform.startswith("LANDSAT"):
        products = download_landsat_data(date_from, date_to, platform, poi_id=poi_id, tile_limit=tile_limit, **kwargs)


    # if tiles found, unpack and crop them

    if not products == None and len(products) > 0:
        
        utils.crop_tiles(poi_id)
        logger.info("Tiles cropped.")

    
    if products == None:
        return 0
    else:
        return len(products)

