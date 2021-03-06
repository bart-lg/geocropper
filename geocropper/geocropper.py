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


def import_all_csvs(delimiter=',', quotechar='"', auto_load=True):
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
    auto_load : boolean, optional
        Loads (download and crop) data automatically if true, 
        otherwise data gets only imported into internal database.
        Default is true.

    """
    csvImport.import_all_csvs(delimiter, quotechar, auto_load)


def show_satellite_data(lat, lon, date_from, date_to, platform, 
    tile_limit=0, tile_start=1, **kwargs):
    """Shows satellite products for given search parameters.

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
    orbitdirection : str, optional
        Parameter for Sentinel-1 products (ASCENDING, DESCENDING).      
    filename : str, optional
        Parameter for Sentinel products.
        Wildcards are allowed. 
        Therefore, data can be searched for the serial identifier (A, B, C, ...) of the satellites.
        Example: "S1A_" for satellite A of the Sentinel-1 programme.        
    """

    # convert date to required format
    date_from = utils.convert_date(date_from, "%Y%m%d")
    date_to = utils.convert_date(date_to, "%Y%m%d")

    # print search info
    # TODO: move to a utils function (redundand code)

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

    # search for sentinel data
    
    products = download.search_satellite_products(lat, lon, date_from, date_to, platform, 
        tile_limit=tile_limit, tile_start=tile_start, **kwargs)

    if products != None and len(products) > 0:

        print("Found tiles: %d\n" % len(products))
        logger.info("Found tiles: %d\n" % len(products))        

        for key in products:

            print(f"\nKEY: {key}")

            for item_key, item in products[key].items():

                print(f"{item_key}: {item}")

    else:

        print("No tiles found.")


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
    orbitdirection : str, optional
        Parameter for Sentinel-1 products (ASCENDING, DESCENDING).             
    filename : str, optional
        Parameter for Sentinel products.
        Wildcards are allowed. 
        Therefore, data can be searched for the serial identifier (A, B, C, ...) of the satellites.
        Example: "S1A_" for satellite A of the Sentinel-1 programme.

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

    if products != None and len(products) > 0:

        print("Found tiles: %d\n" % len(products))
        logger.info("Found tiles: %d\n" % len(products))        

        # add tile information to database

        for key in products:

            meta_data = products[key]

            tile_id = download.save_product_key(platform, key, meta_data=meta_data)

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
                download.download_product(tile=tile)

    else:

        print("No tiles found.")                

    return products


def download_and_crop(lat, lon, groupname, date_from, date_to, platform, width, height, 
                      description = "", tile_limit = 0, tile_start=1, auto_crop=True, **kwargs):
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
    description : str, optional
        Description of the point of interest.
    tile_limit : int, optional
        Maximum number of tiles to be downloaded.
    tile_start : int, optional
        A tile_start parameter greater than 1 omits the first found tiles.
        Default is 1.      
    auto_crop: boolean, optional
        Crops tile immediately, if true. Otherwise an outstanding crop will be added to the database only.
        Default is true.
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
    orbitdirection : str, optional
        Parameter for Sentinel-1 products (ASCENDING, DESCENDING).
    filename : str, optional
        Parameter for Sentinel products.
        Wildcards are allowed. 
        Therefore, data can be searched for the serial identifier (A, B, C, ...) of the satellites.
        Example: "S1A_" for satellite A of the Sentinel-1 programme.                     

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

    poi = db.get_poi(groupname, lat, lon, date_from, date_to, platform, width, height, 
        description=description, tile_limit=tile_limit, tile_start=tile_start, **kwargs)

    if poi == None:     
        poi_id = db.add_poi(groupname, lat, lon, date_from, date_to, platform, 
            width, height, description=description, tile_limit=tile_limit, tile_start=tile_start, **kwargs)
    else:
        poi_id = poi["rowid"]

    # search and download tiles

    products = None

    products = download_satellite_data(lat, lon, date_from, date_to, platform, 
        poi_id=poi_id, tile_limit=tile_limit, tile_start=tile_start, **kwargs)

    # if tiles found, crop them

    if products != None and len(products) > 0 and auto_crop:
        
        utils.crop_tiles(poi_id)

    
    if products == None:
        return 0
    else:
        return len(products)


def download_and_crop_outstanding():

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

            download.download_product(tile=tile)

    # crop outstanding points                    

    pois = db.get_uncropped_pois_for_downloaded_tiles()

    if not pois == None:

        print("\nCrop outstanding points:")
        print("------------------------------")

        for poi in pois:

            if poi['tileCropped'] == None and poi['cancelled'] == None:

                print(f"Crop outstanding point: lat:{poi['lat']} lon:{poi['lon']} \
                        groupname:{poi['groupname']} width:{poi['width']} height:{poi['height']}")
                utils.crop_tiles(poi['rowid'])
    
        print("\nCropped all outstanding points!")


def get_number_of_outstanding_crops():
    pois = db.get_uncropped_pois_for_downloaded_tiles()
    return(len(pois))


def crop_outstanding(lower_boundary=None, upper_boundary=None):
    """Crops outstanding images. To easily run multiple processes boundaries can be set.
    """

    logger.info(f"Start of crop outstanding images lower_boundary:{lower_boundary} upper_boundary:{upper_boundary}")

    print("\nCropping outstanding images:")
    print("----------------------------\n")
    print(f"lower_boundary: {lower_boundary}")
    print(f"upper_boundary: {upper_boundary}\n")

    # get uncropped locations
    pois = db.get_uncropped_pois_for_downloaded_tiles()

    if upper_boundary != None and upper_boundary > 0 and len(pois) > upper_boundary:
        pois = pois[0:upper_boundary]

    if lower_boundary != None and lower_boundary > 0:
        if len(pois) > lower_boundary:
            pois = pois[lower_boundary:]
        else:
            print("Lower boundary higher than number of elements left")
            logger.warning("Lower boundary higher than number of uncropped elements left")
            exit()

    # index i serves as a counter
    i = 0    

    for poi in pois:

        i += 1

        print("\n############################################################")
        print("\n[ Crop outstanding pois... %d/%d ]" % (i, len(pois)))
        logger.info("[ ##### Crop outstanding pois... %d/%d ##### ]" % (i, len(pois)))
        if lower_boundary != None or upper_boundary != None:
            print(f"\n[ Boundaries: {lower_boundary}:{upper_boundary} ]")
            logger.info(f"\n[ Boundaries: {lower_boundary}:{upper_boundary} ]")

        if poi['tileCropped'] == None and poi['cancelled'] == None:

            print(f"Crop outstanding point: lat:{poi['lat']} lon:{poi['lon']} \
                    groupname:{poi['groupname']} width:{poi['width']} height:{poi['height']}")
            utils.crop_tiles(poi['rowid'])

    print(f"\nCropped all outstanding points! (lower_boundary:{lower_boundary} upper_boundary:{upper_boundary})")            


def unpack_big_tiles():
    """Unpacks all big tile archives in big tile directory
    """

    logger.info("start of unpacking tile zip/tar files")
    
    print("\nUnpack big tiles:")
    print("-----------------\n")

    # determine number of zip files        
    files_num_zip = len([f for f in os.listdir(config.bigTilesDir) 
         if f.endswith('.zip') and os.path.isfile(os.path.join(config.bigTilesDir, f))])

    # determine number of tar files
    files_num_tar = len([f for f in os.listdir(config.bigTilesDir) 
         if f.endswith('.tar.gz') and os.path.isfile(os.path.join(config.bigTilesDir, f))])

    # calculate number of total packed files
    files_num = files_num_zip + files_num_tar

    # start unpacking

    for item in os.listdir(config.bigTilesDir):

        if item.endswith(".zip") or item.endswith(".tar.gz"):
            utils.unpack_big_tile(item)

    logger.info("tile zip/tar files extracted")        


def combine_preview_images(folder, has_subdir=True):
    """Creates combined previews in a subdirectory of the cropped tiles folder.

    Parameters
    ----------
    folder : string
        Name of the subdirectory.
    has_subdir : boolean, optional
        Default is True.
        The cropped tiles directory can have two different structures.
        This parameter defines, if the passed directory has a further subdirectory.
    """
    utils.combine_images(folder, has_subdir)   


def load_imported_csv_data(lower_boundary=None, upper_boundary=None, auto_crop=True):
    csvImport.load_imported_csv_data(lower_boundary, upper_boundary, auto_crop)
    
def create_random_crops(crops_per_tile=30, output_folder="random_crops", width=1000, height=1000):
    utils.create_random_crops(crops_per_tile, output_folder, width, height)


