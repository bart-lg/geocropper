import geocropper.log as log
from tqdm import tqdm
import os
import subprocess
import pathlib
import shutil
import csv
import pyproj
from pprint import pprint
import rasterio
import numpy
import pandas
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
import geocropper.visualSelection as visualSelection

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
    
    if tile_limit == None:
        tile_limit = 0
    if tile_start == None:
        tile_start = 1

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
            print(f"First download request: {utils.convert_date(tile['firstDownloadRequest'], new_format='%Y-%m-%d %H:%M:%S')}")
            if tile['lastDownloadRequest'] == None:
                print(f"Last download request: None\n")
            else:
                print(f"Last download request: {utils.convert_date(tile['lastDownloadRequest'], new_format='%Y-%m-%d %H:%M:%S')}\n")

            download.download_product(tile=tile)

    # crop outstanding points                    

    pois = db.get_uncropped_pois_for_unpacked_tiles()

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
    pois = db.get_uncropped_pois_for_unpacked_tiles()
    return(len(pois))


def reset_cancelled_crops():
    db.reset_cancelled_tiles_for_pois()
    print("Cancelled crops got reseted.")


def crop_outstanding(lower_boundary=None, upper_boundary=None):
    """Crops outstanding images. To easily run multiple processes boundaries can be set.
    """

    logger.info(f"Start of crop outstanding images lower_boundary:{lower_boundary} upper_boundary:{upper_boundary}")

    print("\nCropping outstanding images:")
    print("----------------------------\n")
    print(f"lower_boundary: {lower_boundary}")
    print(f"upper_boundary: {upper_boundary}\n")

    # get uncropped locations
    pois = db.get_uncropped_pois_for_unpacked_tiles()

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


def combine_preview_images(folder, outside_cropped_tiles_dir=False, has_subdir=True, image_height=None, image_width=None):
    """Creates combined previews in a subdirectory of the cropped tiles folder.

    Parameters
    ----------
    folder : string
        Name of the subdirectory.
    outside_cropped_tiles_dir : boolean, optional
        Default is False.
        Set this to true if folder path is not a relative path within the cropped tiles folder,
        but an absolute path.
    has_subdir : boolean, optional
        Default is True.
        The cropped tiles directory can have two different structures.
        This parameter defines, if the passed directory has a further subdirectory.
    image_height : int, optional
        trimmed height of preview images
    image_width : int, optional
        trimmed width of preview images        
    """
    utils.combine_images(folder, outside_cropped_tiles_dir, has_subdir, image_height, image_width)   


def load_imported_csv_data(lower_boundary=None, upper_boundary=None, auto_crop=True):
    csvImport.load_imported_csv_data(lower_boundary, upper_boundary, auto_crop)
    
def create_random_crops(crops_per_tile=30, output_folder="random_crops", width=1000, height=1000):
    utils.create_random_crops(crops_per_tile, output_folder, width, height)

def trim_crops(source_dir, target_dir, width, height, has_subdir=True):
    utils.trim_crops(source_dir, target_dir, width, height, has_subdir)

def copy_big_tiles(target_dir, required_only=False):
    """Copies the required big tiles from big tiles folder to target path.

    Copies the required big tiles from big tiles folder to target path.
    The required tiles are determined from the internal database.

    Parameters
    ----------
    target_path : Path
        Path where the big tiles should be copied to.
    required_only : boolean, optional
        If true, only required tiles for the outstanding crops will be copied.
        If false, all tiles with existing entry in internal database will be copied.
    """        
    utils.copy_big_tiles(target_dir, required_only)
    

def retrieve_scene_classes(groupname):
    input_dir = config.croppedTilesDir / groupname
    print(f"Retrieve scene classes for {input_dir}...")
    for crops_dir in input_dir.glob("*"):
        if crops_dir.is_dir():
            print(f"Retrieve scene classes for subdir {crops_dir}")
            utils.retrieve_scene_classes(crops_dir)


def filter_and_move_crops(crops_path, output_path, lower_boundaries=None, upper_boundaries=None, use_database_scene_values=True, \
                          move_crops_without_scene_classifications=False):
    """Filters crops based on scene classification values (Sentinel-2) and moves them to new directory.

    Filters crops based on scene classification values (Sentinel-2) and moves them to new directory.
    Classifications (obtained from https://dragon3.esa.int/web/sentinel/technical-guides/sentinel-2-msi/level-2a/algorithm):
    0: NO_DATA
    1: SATURATED_OR_DEFECTIVE    
    2: DARK_AREA_PIXELS
    3: CLOUD_SHADOWS
    4: VEGETATION
    5: NOT_VEGETATED
    6: WATER
    7: UNCLASSIFIED
    8: CLOUD_MEDIUM_PROBABILITY
    9: CLOUD_HIGH_PROBABILITY
    10: THIN_CIRRUS
    11: SNOW

    Parameters
    ----------
    crops_path : str
        Path of crops. Crops must match with database entries (especially crop id), if database should be used.
    output_path : str
        Path where the filtered crops should be moved to.
    lower_boundaries : dict, optional
        Dictionary with lower boundary ratios of scene classes.
    upper_boundaries : dict, optional
        Dictionary with upper boundary ratios of scene classes.
    use_database_scene_values : boolean, optional
        If true, the scene ratios for each crop in the database will be used.
        Otherwise, the scene ratios get retrieved by the scene classification map (Sentinel-2).
        Default is true.
    move_crops_without_scene_classifications : boolean, optional
        Default is false.
    """

    crops_path = pathlib.Path(crops_path)
    output_path = pathlib.Path(output_path)

    utils.filter_and_move_crops(crops_path, output_path, lower_boundaries, upper_boundaries, use_database_scene_values, \
                          move_crops_without_scene_classifications)


def visual_selection(path, gap=config.previewBorder, image_start=1):
    visualSelection.start_visual_selection(path, gap, image_start)


def move_selected_crops(source_dir, target_dir, csv_file):
    """Moves crop folders with IDs given in CSV file to target folder.
    """

    source_path = pathlib.Path(source_dir)
    target_path = pathlib.Path(target_dir)
    csv_path = pathlib.Path(csv_file)

    target_path.mkdir(parents=True, exist_ok=True)

    print("Moving selected crops")
    print("=====================\n")

    print(f"source: {str(source_path.absolute())}")
    print(f"target: {str(target_path.absolute())}")
    print(f"csv: {str(csv_path.absolute())}\n")

    counter = 0

    with open(str(csv_path.absolute()), "r", newline="") as f:
        
        reader = csv.reader(f)
        
        for line in reader:
            if len(line) > 0 and len(line[0]) > 0 and line[0].isdigit():

                found = list(source_path.glob(f"{line[0]}_*"))

                if len(found) > 0:

                    folder_name = found[0]

                    if not folder_name.name.startswith("0_"):

                        crop = source_path / folder_name

                        # move directory
                        shutil.move(str(crop.absolute()), str(target_path.absolute()))

                        counter = counter + 1
                        print(f"[{counter}] found and moved: {crop.name}")

                else:

                    print(f"crop {line} not found...")


def create_csv_from_crops(csv_path, source_dir, outside_cropped_tiles_dir=False, has_subdir=True):
    """Creates a CSV file containing ID and coordinates of crops in the specified cropped tiles folder.

    Parameters
    ----------
    csv_path : string
        Path to csv file that should be created.
    source_dir : string
        Name of the group directory or direct path to source directory.
    outside_cropped_tiles_dir : boolean, optional
        Default is False.
        Set this to true if folder path is not a relative path within the cropped tiles folder,
        but an absolute path.
    has_subdir : boolean, optional
        Default is True.
        The cropped tiles directory can have two different structures.
        This parameter defines, if the passed directory has a further subdirectory.
    """

    if len(source_dir) > 0:

        with open(csv_path, "w", newline="") as csv_file:

            spamwriter = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

            if outside_cropped_tiles_dir:
                
                source_dir = pathlib.Path(source_dir)
                
                if has_subdir:
                
                    spamwriter.writerow(["request", "id", "lon", "lat", "image_datetime"])
                
                    for request in source_dir.glob("*"):
                
                        if request.is_dir():

                            for crop in request.glob("*"):

                                if not crop.name.startswith("0_"):

                                    # TODO: better exception handling... log if something went wrong
                                    try:
                                        crop_name = crop.name.split("_")
                                        crop_id = int(crop_name[0])
                                        crop_lon = float(crop_name[1])
                                        crop_lat = float(crop_name[2])
                                        if len(crop_name) > 3:
                                            crop_datetime = crop_name[3]
                                        else:
                                            crop_datetime = ""

                                        spamwriter.writerow([request.name, crop_id, crop_lon, crop_lat, crop_datetime])
                                    except:
                                        pass

                else:

                    spamwriter.writerow(["id", "lon", "lat", "image_datetime"])

                    for crop in source_dir.glob("*"):

                        if not crop.name.startswith("0_"):

                            try:
                                crop_name = crop.name.split("_")
                                crop_id = int(crop_name[0])
                                crop_lon = float(crop_name[1])
                                crop_lat = float(crop_name[2])
                                if len(crop_name) > 3:
                                    crop_datetime = crop_name[3]
                                else:
                                    crop_datetime = ""

                                spamwriter.writerow([crop_id, crop_lon, crop_lat, crop_datetime])                
                            except:
                                pass
                
            else:

                if has_subdir:
                    spamwriter.writerow(["groupname", "request", "id", "lon", "lat", "image_datetime"])
                else:
                    spamwriter.writerow(["groupname", "id", "lon", "lat", "image_datetime"])

                for group in config.croppedTilesDir.glob("*"):

                    if group.is_dir() and source_dir == group.name:

                        if has_subdir:
                            
                            for request in group.glob("*"):
                                    
                                if request.is_dir():

                                    for crop in request.glob("*"):

                                        if not crop.name.startswith("0_"):

                                            try:
                                                crop_name = crop.name.split("_")
                                                crop_id = int(crop_name[0])
                                                crop_lon = float(crop_name[1])
                                                crop_lat = float(crop_name[2])
                                                if len(crop_name) > 3:
                                                    crop_datetime = crop_name[3]
                                                else:
                                                    crop_datetime = ""

                                                spamwriter.writerow([group.name, request.name, crop_id, crop_lon, crop_lat, crop_datetime])                                
                                            except:
                                                pass

                        else:

                            for crop in group.glob("*"):

                                if not crop.name.startswith("0_"):

                                    try:
                                        crop_name = crop.name.split("_")
                                        crop_id = int(crop_name[0])
                                        crop_lon = float(crop_name[1])
                                        crop_lat = float(crop_name[2])
                                        if len(crop_name) > 3:
                                            crop_datetime = crop_name[3]
                                        else:
                                            crop_datetime = ""

                                        spamwriter.writerow([group.name, crop_id, crop_lon, crop_lat, crop_datetime])                              
                                    except:
                                        pass


def move_imperfect_S1_crops(source_dir, target_dir, outside_cropped_tiles_dir=False, has_subdir=True):
    """Moves imperfect Sentinel-1 crops to target dir.

    Parameters
    ----------  
    source_dir : string
        Name of the group directory or direct path to source directory.
    target_dir : string
        Path of target directory.
    outside_cropped_tiles_dir : boolean, optional
        Default is False.
        Set this to true if folder path is not a relative path within the cropped tiles folder,
        but an absolute path.
    has_subdir : boolean, optional
        Default is True.
        The cropped tiles directory can have two different structures.
        This parameter defines, if the passed directory has a further subdirectory.
    """

    target_dir = pathlib.Path(target_dir)

    if len(source_dir) > 0:

        print("Checking Sentinel-1 crops...")

        if outside_cropped_tiles_dir:

            print(f"Source directory: {source_dir}")

            source_dir = pathlib.Path(source_dir)
            if has_subdir:
                for request in source_dir.glob("*"):
                    if request.is_dir():
                        print(f"Subdirectory: {request.name}")
                        utils.move_imperfect_S1_crops(request, (target_dir / request.name))
            else:
                utils.move_imperfect_S1_crops(source_dir, target_dir)
        else:
            for group in config.croppedTilesDir.glob("*"):

                if source_dir == group.name:

                    print(f"Group directory: {group.name}")

                    if has_subdir:
                        for request in group.glob("*"):
                            if request.is_dir():
                                print(f"Subdirectory: {request.name}")
                                utils.move_imperfect_S1_crops(request, (target_dir / group.name / request.name))                            

                    else:
                        utils.move_imperfect_S1_crops(group, (target_dir / group.name))                                                    


def move_crops_containing_locations(csv_path, source_dir, target_dir, outside_cropped_tiles_dir=False, has_subdir=True, based_on_foldername=False):
    """Move crops that contain coordinates in CSV to target dir.

    Parameters
    ----------
    csv_path : string
        Path to csv file with coordinates (csv file containing lat and lon variable names).      
    source_dir : string
        Name of the group directory or direct path to source directory.
    target_dir : string
        Path of target directory.
    outside_cropped_tiles_dir : boolean, optional
        Default is False.
        Set this to true if folder path is not a relative path within the cropped tiles folder,
        but an absolute path.
    has_subdir : boolean, optional
        Default is True.
        The cropped tiles directory can have two different structures.
        This parameter defines, if the passed directory has a further subdirectory.
    based_on_foldername : boolean, optional
        Default is False.
        If true, compare with coordinates mentioned in crop folder name.
        If false, check if coordinates are within the preview.tif (crops must contain a georeferenced preview.tif).
    """

    target_dir = pathlib.Path(target_dir)
    csv_path = pathlib.Path(csv_path)

    if csv_path.exists() and len(source_dir) > 0:

        print("Checking crops...")

        if outside_cropped_tiles_dir:

            print(f"Source directory: {source_dir}")

            source_dir = pathlib.Path(source_dir)
            if has_subdir:
                for request in source_dir.glob("*"):
                    if request.is_dir():
                        print(f"Subdirectory: {request.name}")
                        utils.move_crops_containing_locations(csv_path, request, (target_dir / request.name), based_on_foldername)
            else:
                utils.move_crops_containing_locations(csv_path, source_dir, target_dir, based_on_foldername)
        else:
            for group in config.croppedTilesDir.glob("*"):

                if source_dir == group.name:

                    print(f"Group directory: {group.name}")

                    if has_subdir:
                        for request in group.glob("*"):
                            if request.is_dir():
                                print(f"Subdirectory: {request.name}")
                                utils.move_crops_containing_locations(csv_path, request, (target_dir / group.name / request.name), based_on_foldername)

                    else:

                        utils.move_crops_containing_locations(csv_path, group, (target_dir / group.name), based_on_foldername)


def stack_trimmed_images(source_dir, postfix="", output_dir=None):
    """Stack images of the same position with different capture dates and write them to tifs.
    
    Parameters
    ----------
    root_dir : string
        Path of trimmed crops of various recording times which shall be stacked
    postfix : string, optional
        Set desired postfix for selecting specific folders containing a certain string 
        (by default is empty "" and therefore selects every folder in source_dir).
    output_dir : string, optional
        Path where the stacked image shall be stored to (by default is equal to root_dir)
    """

    source_dir = pathlib.Path(source_dir)
    if output_dir == None:
        output_dir = source_dir
    else:
        output_dir = pathlib.Path(output_dir)

    lat_lon_set = utils.get_unique_lat_lon_set(source_dir, postfix)

    for position in tqdm(lat_lon_set, desc="Stacking images and writing tifs: "):
        image_path_list = utils.get_image_path_list(source_dir, position, postfix)
        utils.stack_trimmed_images(image_path_list, output_dir, position, postfix)


def compare_csv_locations(csv_dir, reference_csv_path=None, result_csv_path=None):
    """Compares lat and lon coordinates in multiple csv files and outlines how many coordinates occur multiply.

    Compares lat and lon coordinates in multiple csv files and outlines how many coordinates occur multiply.
    The reference csv can be used to specify a list of coordinates, which sould be checked. This csv can be located outside the csv_dir.

    Parameters
    ----------
    csv_dir : string
        Path containing the csv files which shall be checked.
    reference_csv_path : string, optional
        Path of the reference csv file.
    result_csv_path : string, optional
        Path of csv file for output of results.
    """

    csv_dir = pathlib.Path(csv_dir)
    if not csv_dir.exists():
        print("CSV directory does not exist!")
        return

    lat_lon_set = None

    print("Creating unique coordinate list...")

    if not isinstance(reference_csv_path, type(None)):
        
        reference_csv_path = pathlib.Path(reference_csv_path)
        if not reference_csv_path.exists():
            print("Reference CSV file does not exist. Continuing without reference file.")
            reference_csv_path = None

        else:
            lat_lon_set = utils.get_unique_lat_lon_set(csv_path=reference_csv_path)

    if isinstance(lat_lon_set, type(None)):
        for csv_file in csv_dir.glob("*.csv"):
            lat_lon_set = utils.get_unique_lat_lon_set(csv_path=csv_file)

    print("Counting coordinates in csv files...")

    csv_list = list(csv_dir.glob("*.csv"))
    lat_lon_list = numpy.array(list(lat_lon_set), str)
    lat_lon_counter = numpy.zeros((len(lat_lon_list), len(csv_list)), int)

    print(f"Total number of csv files: {len(csv_list)}")

    col_list = ["lon", "lat"]
    for i, csv_file in tqdm(enumerate(csv_list), desc="Scanning CSV files: "):
        data = pandas.read_csv(csv_file, usecols=col_list, dtype=str)
        for j in range(len(data)):
            lon = str(data["lon"][j])
            lat = str(data["lat"][j]) 
            lon, lat = utils.reduce_coordinate_digits(lon, lat)           
            list_index = numpy.where(lat_lon_list == f"{lon}_{lat}")[0]
            if len(list_index) > 0:
                lat_lon_counter[list_index[0]][i] = 1

    print("\nResults:")
    print("=========\n")

    print(f"Number of coordinates: {str(len(lat_lon_list))}")
    print(f"Number of scanned csv files: {str(len(csv_list))}")
    if not isinstance(reference_csv_path, type(None)):
        print(f"Reference csv file with coordinates: {reference_csv_path.name}")
    print("")

    sums = numpy.zeros(len(lat_lon_list), int)
    for i in range(len(lat_lon_list)):
        for j in range(len(lat_lon_counter[i])):
            sums[i] = sums[i] + lat_lon_counter[i][j]

    for i in range(len(csv_list)+1):
        count = numpy.count_nonzero(sums == i)
        print(f"{str(i)} occurences per coordinate: {str(count)}")
    print("")

    for i in range(len(csv_list)):
        found = 0
        for j in range(len(lat_lon_counter)):
            found = found + lat_lon_counter[j][i]
        percentage = round(found / len(lat_lon_counter) * 100)
        print(f"Share of found coordinates in {csv_list[i].name}: " + "{:.2f}%".format(round(percentage, 2)))

    if not isinstance(result_csv_path, type(None)):
        result_csv_path = pathlib.Path(result_csv_path)
        if not result_csv_path.exists():
            with open(result_csv_path.absolute(), "w", newline="") as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=",")
                spamwriter.writerow(["lon", "lat", "sum"] + [item.name for item in csv_list])
                for i in range(len(lat_lon_list)):
                    lon = lat_lon_list[i].split("_")[0]
                    lat = lat_lon_list[i].split("_")[1]
                    s = sums[i]
                    counts = numpy.array(lat_lon_counter[i], str)
                    spamwriter.writerow(numpy.append(numpy.array([str(lon), str(lat), str(s)]), counts))
