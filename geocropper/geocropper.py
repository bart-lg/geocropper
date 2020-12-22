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
import geocropper.download as download

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


def download_satellite_data(lat, lon, date_from, date_to, platform, 
    no_product_download=False, poi_id=0, tile_limit=0, tile_start=1, **kwargs):
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
        Choose between 
        - 'Sentinel-1' 
        - 'Sentinel-2'
        - 'LANDSAT_TM_C1'
        - 'LANDSAT_ETM_C1'
        - 'LANDSAT_8_C1'
    no_product_download : boolean, optional
        If true only meta data gets fetched.
        Default is False.
    poi_id : int, optional
        ID of PointOfInterest record in sqlite database.
        This is primarly used by other functions to create a connection between the database records.
    tile_limit : int, optional
        Maximum number of tiles to be downloaded.
        Default is no limit.
    tile_start : int, optional
        A tile_start parameter greater than 1 omits the first found tiles.
        Default is 1.
    cloudcoverpercentage : int, optional
        Parameter for Sentinel-2 products.
        Value between 0 and 100 for maximum cloud cover percentage.
    producttype : str, optional
        Sentinel-1 products: RAW, SLC, GRD, OCN
            SLC: Single Look Complex
            GRD: Ground Range Detected
            OCN: Ocean
        Sentinel-2 products: S2MSI1C, S2MSI2A, S2MSI2Ap
    polarisationmode : str, optional
        Parameter for Sentinel-1 products.
        Accepted entries are: HH, VV, HV, VH, HH+HV, VV+VH
    sensoroperationalmode : str, optional
        Parameter for Sentinel-1 products.
        Accepted entries are: SM, IW, EW, WV
            SM: Stripmap
            IW: Interferometric Wide Swath 
            EW: Extra Wide Swath
            WV: Wave
    swathidentifier : str, optional
        Parameter for Sentinel-1 products.
        Accepted entries are: S1, S2, S3, S4, S5, S6, IW, IW1, IW2, IW3, EW, EW1, EW2, EW3, EW4, EW5
    timeliness : str, optional
        Parameter for Sentinel-1 products.
            NRT: NRT-3h (Near Real Time)
            NTC: Fast-24h

    Returns
    -------
    list
        list of found products (tiles)

    """
    
    # convert date to required format
    date_from = utils.convert_date(date_from, "%Y%m%d")
    date_to = utils.convert_date(date_to, "%Y%m%d")
    

    # print search info

    if platform.lower().startswith("sentinel"):
        print("Search for Sentinel data:")
    if platform.lower().startswith("landsat"):
        print("Search for Landsat data:")
    print("lat: " + str(lat))
    print("lon: " + str(lon))
    print("From: " + utils.convert_date(date_from, "%d.%m.%Y"))
    print("To: " + utils.convert_date(date_to, "%d.%m.%Y"))
    print("Platform: " + platform)
    if tile_limit > 0:
        print("Tile-limit: %d" % tile_limit)
    if tile_start > 1:
        print("Tile-start: %d" % tile_start)
    for key, value in kwargs.items():
        if key in config.optionalSentinelParameters:
            print("%s: %s" % (key, str(value)))
    print("----------------------------\n")


    if platform.lower().startswith("sentinel"):
        logger.info("Search for Sentinel data:")
    if platform.lower().startswith("landsat"):
        logger.info("Search for Landsat data:")
    logger.info("From: " + utils.convert_date(date_from, "%d.%m.%Y"))
    logger.info("To: " + utils.convert_date(date_to, "%d.%m.%Y"))
    logger.info("Platform: " + platform)
    if tile_limit > 0:
        logger.info("Tile-limit: %d" % tile_limit)
    if tile_start > 1:
        logger.info("Tile-start: %d" % tile_start)
    for key, value in kwargs.items():
        if key in config.optionalSentinelParameters:
            logger.info("%s: %s" % (key, str(value)))      
    
    
    # search for sentinel data
    
    products = download.search_satellite_products(lat, lon, date_from, date_to, platform, 
        tile_limit=tile_limit, tile_start=tile_start, **kwargs)

    print("Found tiles: %d\n" % len(products))
    logger.info("Found tiles: %d\n" % len(products))

    if len(products) > 0:

        # add tile information to database

        for key, item in products:

            tile_id = download.save_product_key(platform, key, meta_data=item)

            # if there is a point of interest (POI) then create connection between tile and POI in database

            if int(poi_id) > 0:
                
                tile_poi = db.get_tile_for_poi(poi_id, tile_id)
                if tile_poi == None:
                    db.add_tile_for_poi(poi_id, tile_id)             

        # if there is a point of interest (POI) => set date for tiles identified
        # this means that all tiles for the requested POI have been identified
        if int(poi_id) > 0:
            db.set_tiles_identified_for_poi(poi_id)


        # start download

        if no_product_download == False:

            print("Download")
            print("-----------------\n")

            for i, (key, item) in enumerate(products.items()):

                tile = db.get_tile(product_id = key)
                download.download_product(tile)

    return products


def download_and_crop(lat, lon, groupname, date_from, date_to, platform, width, height, tile_limit = 0, tile_start=1, **kwargs):
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
    tile_start : int, optional
        A tile_start parameter greater than 1 omits the first found tiles.
        Default is 1.        
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

    products = download_satellite_data(lat, lon, date_from, date_to, platform, 
        poi_id=poi_id, tile_limit=tile_limit, tile_start=tile_start, **kwargs)

    # if tiles found, crop them

    if not products == None and len(products) > 0:
        
        utils.crop_tiles(poi_id)
        logger.info("Tiles cropped.")

    
    if products == None:
        return 0
    else:
        return len(products)


def start_and_crop_requested_downloads():

    print("\nStart requested downloads:")
    print("--------------------------------")

    tiles = db.get_requested_tiles()

    if not tiles == None:

        for tile in tiles:

            print(f"\nPlatform: {tile['platform']}")
            print(f"Tile: {tile['folderName']}")
            print(f"Product ID: {tile['productId']}")            
            print(f"First download request: {convert_date(tile['firstDownloadRequest'], new_format='%Y-%m-%d %H:%M:%S')}")
            if tile['lastDownloadRequest'] == None:
                print(f"Last download request: None\n")
            else:
                print(f"Last download request: {convert_date(tile['lastDownloadRequest'], new_format='%Y-%m-%d %H:%M:%S')}\n")

            download.download_product(tile)

    # crop outstanding points                    

    pois = db.get_uncropped_pois_for_downloaded_tiles()

    if not pois == None:

        print("\nCrop outstanding points:")
        print("------------------------------")

        for poi in pois:

            if poi['tileCropped'] == None and poi['cancelled'] == None:

                print(f"Crop outstanding point: lat:{poi['lat']} lon:{poi['lon']} \
                        groupname:{poi['groupname']} width:{poi['width']} height:{poi['height']}")
                crop_tiles(poi['rowid'])
    
        print("\nCropped all outstanding points!")