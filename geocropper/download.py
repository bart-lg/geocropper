import pathlib

import geocropper.config as config
import geocropper.utils as utils
import geocropper.database as database
import geocropper.sentinelWrapper as sentinelWrapper
import geocropper.landsatWrapper as landsatWrapper
import geocropper.asfWrapper as asfWrapper

import logging

# get logger object
logger = logging.getLogger('root')
db = database.Database()

def search_satellite_products(lat, lon, date_from, date_to, platform, tile_limit=0, tile_start=1, **kwargs):
    """Search for satellite products

    This function searches for Sentinel or Landsat products for the given location, 
    time frame and other parameters given in kwargs.

    Parameters
    ----------
    lat : float
        Latitude of the geolocation (WGS84 decimal).
    lon : float
        Longitude of the geolocation (WGS84 decimal).
    date_from : str
        Start date for search request in a chosen format.
        The format must be %Y%m%d.
    date_to : str
        End date for search request in a chosen format.
        The format must be %Y%m%d.
    platform : str
        Choose between 
        - 'Sentinel-1' 
        - 'Sentinel-2'
        - 'LANDSAT_TM_C1'
        - 'LANDSAT_ETM_C1'
        - 'LANDSAT_8_C1'
    tile_limit : int, optional
        Maximum number of tiles to be downloaded.
    tile_start : int, optional
        A tile_start parameter greater than 1 omits the first found tiles.
    cloudcoverpercentage : int, optional
        Parameter for Sentinel-2 and Landsat products.
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

    results = None


    # Sentinel

    if platform.lower().startswith("sentinel"):
        
        sentinel = sentinelWrapper.SentinelWrapper()

        if int(tile_limit) > 0:
            products = sentinel.get_sentinel_products(lat, lon, date_from, date_to, platform, 
                limit=tile_limit, **kwargs)
        else:   
            products = sentinel.get_sentinel_products(lat, lon, date_from, date_to, platform, **kwargs)

        if len(products) > 0:
            if tile_start > 1:
                for i, (key, item) in enumerate(products.items()):
                    if i >= (tile_start - 1):
                        results[key] = item
            else:
                results = products

    # Landsat

    if platform.lower().startswith("landsat"):

        landsat = landsatWrapper.LandsatWrapper()

        date_from = utils.convert_date(date_from, "%Y-%m-%d")
        date_to = utils.convert_date(date_to, "%Y-%m-%d")

        # copy cloudcoverpercentage to max_cloud_coverage (default is 100)
        max_cloud_coverage = 100
        for key, value in kwargs.items():
            if key == "cloudcoverpercentage":
                max_cloud_coverage = value      
    
        products = landsat.get_landsat_products(lat, lon, date_from, date_to, platform, 
            max_cloud_coverage, tile_limit)

        if len(products) > 0:
            for i, (key, item) in enumerate(products.items()):
                if i >= (tile_start - 1):
                    products[item["entityId"]] = item

    return results


def save_product_key(platform, key, meta_data=None):
    """Saves Sentinel or Landsat product key to internal database.

    Parameters
    ----------
    platform : string
        String starting either with 'Sentinel' or 'Landsat' (not case sensitive).
    key : string
        Product key of Sentinel or Landsat product.
    meta_data : dictionary
        Meta data of Sentinel or Landsat product.

    Returns
    -------
    tile_id : int
        Row id of tile in internal database.        

    """
    if platform == None or key == None:
        return None

    if meta_data == None:

        if platform.lower().startswith("sentinel"):

            # load sentinel wrapper and fetch meta data
            sentinel = sentinelWrapper.SentinelWrapper()
            meta_data = sentinel.get_product_data(key)

        if platform.lower().startswith("landsat"):

            # load landsat wrapper and fetch meta data
            landsat = landsatWrapper.LandsatWrapper()
            meta_data = landsat.get_product_data(platform, key)


    if meta_data != None:

        # tile_id is the internal id in the SQLITE database
        tile_id = None
        tile = db.get_tile(product_id = key)

        if tile == None:

            # start- and endtime of sensoring and folder name
            
            if platform.lower().startswith("sentinel"):
                beginposition = meta_data["beginposition"]
                endposition = meta_data["beginposition"]
                # folder name after unzip is < SENTINEL TILE TITLE >.SAFE
                folder_name = meta_data["title"] + ".SAFE"          
            
            if platform.lower().startswith("landsat"):
                beginposition = product["startTime"]
                endposition = product["endTime"]        
                folder_name = product["displayId"]

            tile_id = db.add_tile(
                platform=platform, 
                product_id=key, 
                beginposition=beginposition, 
                endposition=endposition, 
                folder_name=folder_name
            )

        else:
            tile_id = tile["rowid"]     

        return tile_id


def download_product(tile_id=None, tile=None):
    """Downloads Sentinel or Landsat product

    Parameters
    ----------
    tile_id : int, optional
        Row id of the tile record in the internal database.
    tile : list, optional
        Record of the tile in the internal database.
    """

    if tile_id == None and tile == None:
        return None

    if tile == None:
        tile = db.get_tile_by_rowid(row_id = tile_id)

    if check_for_existing_big_tile(tile) == False:

        if tile['platform'].lower().startswith("sentinel"):

            sentinel = sentinelWrapper.SentinelWrapper()

            # check if tile ready for download
            if sentinel.ready_for_download(tile['productId']):

                # download sentinel product
                # sentinel wrapper has a resume function for incomplete downloads
                logger.info("Download started.")
                db.set_last_download_request_for_tile(tile['rowid'])
                download_complete = sentinel.download_sentinel_product(tile['productId'])

                if download_complete and check_for_existing_big_tile(tile):

                    # download complete timestamp gets set in check_for_existing_big_tile
                    utils.unpack_big_tile(file_name=(tile['folderName'][:-5] + ".zip"), tile=tile)
                    utils.save_tile_projection(tile=tile)
                    return True

            else:

                if tile['folderName'].startswith("S1"):

                    asf = asfWrapper.AsfWrapper()

                    # try ASF as alternative source
                    granule = tile['folderName'][:-5]
                    download_complete = asf.download_S1_tile(granule + ".zip", config.bigTilesDir)

                    if download_complete and check_for_existing_big_tile(tile):

                        # download complete timestamp gets set in check_for_existing_big_tile
                        utils.unpack_big_tile(file_name=(tile['folderName'][:-5] + ".zip"), tile=tile)
                        utils.save_tile_projection(tile=tile)
                        return True

                # send download request to ESA server
                if sentinel.request_offline_tile(last_tile_download_request=tile['lastDownloadRequest'], product_id=tile['productId']):
                    
                    # update download request date for existing tile in database
                    db.set_last_download_request_for_tile(tile['rowid'])

        if tile['platform'].lower().startswith("landsat"):

            landsat = landsatWrapper.LandsatWrapper()

            logger.info("Download started.")
            db.set_last_download_request_for_tile(tile['rowid'])

            landsat.download_landsat_product(tile["productId"])

            if check_for_existing_big_tile(tile):

                # download complete timestamp gets set in check_for_existing_big_tile
                utils.unpack_big_tile(file_name=(tile['folderName'] + ".tar.gz"), tile=tile)
                utils.save_tile_projection(tile=tile)
                return True            


def check_for_existing_big_tile_archive(tile):

    if tile['platform'].lower().startswith("sentinel"):

        if pathlib.Path(config.bigTilesDir / (tile['folderName'][:-5] + ".zip") ).is_file():
            return True
        else:
            return False

    if tile['platform'].lower().startswith("landsat"):

        if pathlib.Path(config.bigTilesDir / (tile['folderName'] + ".tar.gz") ).is_file():
            return True
        else:
            return False

def check_for_existing_big_tile_folder(tile):

    if tile['platform'].lower().startswith("sentinel"):

        if pathlib.Path(config.bigTilesDir / tile['folderName']).is_dir():
            return True
        else:
            return False

    if tile['platform'].lower().startswith("landsat"):

        if pathlib.Path(config.bigTilesDir / tile['folderName']).is_dir():
            return True
        else:
            return False


def check_for_existing_big_tile(tile):
    """Checks if big tile is already downloaded.

    Parameters
    ----------
    tile : list
        Record of the tile in the internal database.    
    Returns
    -------
    boolean
        True or False.
    """

    if tile['platform'].lower().startswith("sentinel"):

        if not check_for_existing_big_tile_folder(tile) and \
           not check_for_existing_big_tile_archive(tile):

            if tile['downloadComplete'] != None:
            
                db.clear_download_complete_for_tile(tile['rowid'])

            return False

        else:

            if tile['downloadComplete'] == None:

                db.set_download_complete_for_tile(tile['rowid'])

            return True
        
    if tile['platform'].lower().startswith("landsat"):

        # TODO: check if existing tar file is complete => needs to be deleted and re-downloaded

        if not check_for_existing_big_tile_folder(tile):

            if tile['downloadComplete'] != None:
            
                db.clear_download_complete_for_tile(tile['rowid'])

            return False

        else:

            if tile['downloadComplete'] == None:

                db.set_download_complete_for_tile(tile['rowid'])

            return True
                                    