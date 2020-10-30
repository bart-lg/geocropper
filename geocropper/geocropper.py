import geocropper.log as log
import zipfile
import tarfile
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
                        db.setDownloadRequestForTile(tileId)

                    # download sentinel product
                    # sentinel wrapper has a resume function for incomplete downloads
                    logger.info("Download started.")
                    print("[%d/%d]: Download %s" % (i, len(products), products[key]["title"]))
                    self.sentinel.downloadSentinelProduct(key)

                    # if downloaded zip-file could be detected set download complete date in database
                    if pathlib.Path(config.bigTilesDir / (products[key]["title"] + ".zip") ).is_file():
                        db.setDownloadCompleteForTile(tileId)
                
                else:

                    # zip file or folder from previous download detected...

                    if tile == None:
                        # if tile not yet in database add to database
                        # this could happen if database gets reset
                        tileId = db.addTile(platform, key, beginposition, endposition, folderName)
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
        

        # if there is a point of interest (POI) => set date for tiles identified
        # this means that all tiles for the requested POI have been identified and downloaded
        if int(poiId) > 0:
            db.setTilesIdentifiedForPoi(poiId)
        
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
                        db.setDownloadRequestForTile(tileId)

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
        
        # if there is a point of interest (POI) => set date for tiles identified
        # this means that all tiles for the requested POI have been identified and downloaded
        if int(poiId) > 0:
           db.setTilesIdentifiedForPoi(poiId)
        
        return products        


    def unpackBigTiles(self):

        logger.info("start of unpacking tile zip/tar files")
        
        print("\nUnpack big tiles:")
        print("-----------------\n")

        # determine number of zip files        
        filesNumZip = len([f for f in os.listdir(config.bigTilesDir) 
             if f.endswith('.zip') and os.path.isfile(os.path.join(config.bigTilesDir, f))])

        # determine number of tar files
        filesNumTar = len([f for f in os.listdir(config.bigTilesDir) 
             if f.endswith('.tar.gz') and os.path.isfile(os.path.join(config.bigTilesDir, f))])

        # calculate number of total packed files
        filesNum = filesNumZip + filesNumTar

        # index i serves as a counter
        i = 1
        

        # start unpacking

        for item in os.listdir(config.bigTilesDir):

            if item.endswith(".zip") or item.endswith(".tar.gz"):
            
                print("[%d/%d] %s:" % (i, filesNum, item))

                # get path of the packed file
                filePath = config.bigTilesDir / item

                # unpack zip file if zip
                if item.endswith(".zip"):

                    # TODO: dirty... (is maybe first entry of zipRef)
                    # get tile by folder name
                    newFolderName = item[:-4] + ".SAFE"
                    tile = db.getTile(folderName = newFolderName)              

                    # unzip
                    with zipfile.ZipFile(file=filePath) as zipRef:
                        
                        # show progress bar based on number of files in archive
                        for file in tqdm(iterable=zipRef.namelist(), total=len(zipRef.namelist())):
                            zipRef.extract(member=file, path=config.bigTilesDir)

                    zipRef.close()


                # unpack tar file if tar
                if item.endswith(".tar.gz"):

                    # get tile by folder name
                    tile = db.getTile(folderName = item[:-7])

                    # create target directory, since there is no root dir in tar package
                    targetDir = config.bigTilesDir / tile["folderName"]
                    if not os.path.isdir(targetDir):
                        os.makedirs(targetDir)                    

                    # untar
                    with tarfile.open(name=filePath, mode="r:gz") as tarRef:

                        # show progress bar based on number of files in archive
                        for file in tqdm(iterable=tarRef.getmembers(), total=len(tarRef.getmembers())):
                            tarRef.extract(member=file, path=targetDir)

                    tarRef.close()


                # remove packed file
                os.remove(filePath)

                # set unpacked date in database
                db.setUnzippedForTile(tile["rowid"])

                i += 1


        logger.info("tile zip/tar files extracted")



    def cropTiles(self, poiId):
        
        print("\nCrop tiles:")
        print("-----------------")

        poi = db.getPoiFromId(poiId)

        print("(w: %d, h: %d)\n" % (poi["width"], poi["height"]))


        # crop tile if point of interest (POI) exists and width and height bigger than 0
        if not poi == None and poi["width"] > 0 and poi["height"] > 0:

            # calculate diagonal distance from center to corners
            diag = math.sqrt((poi["width"]/2)**2 + (poi["height"]/2)**2)

            # determine top left and bottom right coordinates in WGS84
            topLeftLon, topLeftLat, backAzimuth = (pyproj.Geod(ellps="WGS84").fwd(poi["lon"],poi["lat"],315,diag))
            bottomRightLon, bottomRightLat, backAzimuth = (pyproj.Geod(ellps="WGS84").fwd(poi["lon"],poi["lat"],135,diag))

            # convert to Point object (shapely)
            topLeft = Point(topLeftLon, topLeftLat)
            bottomRight = Point(bottomRightLon, bottomRightLat) 

            # get tiles that need to be cropped
            tiles = db.getTilesForPoi(poiId)
            
            # go through the tiles
            for tile in tiles:
            
                # crop if tile is not cropped yet (with the parameters of POI)
                if tile["tileCropped"] == None:

                    print("Cropping %s ..." % tile["folderName"])

                    if poi["platform"] == "Sentinel-1" or poi["platform"] == "Sentinel-2":
                        beginposition = utils.convertDate(tile["beginposition"], newFormat="%Y%m%d-%H%M")
                    else:
                        beginposition = utils.convertDate(tile["beginposition"], newFormat="%Y%m%d")

                    poiParameters = self.getPoiParametersForOutputFolder(poi)
                    connectionId = db.getTilePoiConnectionId(poiId, tile["rowid"])
                    mainTargetFolder = config.croppedTilesDir / poi["groupname"] / poiParameters / ( "%s_%s_%s_%s" % (connectionId, poi["lon"], poi["lat"], beginposition) )

                    # target directory for cropped image
                    targetDir = mainTargetFolder / "sensordata"
                    targetDir.mkdir(parents = True, exist_ok = True)               

                    # target directory for meta information
                    metaDir = mainTargetFolder / "original-metadata"

                    # target directory for preview image
                    previewDir = mainTargetFolder 
                    # previewDir.mkdir(parents = True, exist_ok = True)               

                    # SENTINEL 1 CROPPING
                    
                    if poi["platform"] == "Sentinel-1":

                        # Sentinel-1 cropping is not available yet

                        print("Cropping of Sentinel-1 data not yet supported.\n")
                        db.setCancelledTileForPoi(poiId, tile["rowid"])


                    # SENTINEL 2 CROPPING

                    # crop Sentinel-2 tile
                    if poi["platform"] == "Sentinel-2":

                        # Sentinel-2 img data are in jp2-format
                        # set appropriate format for GDAL lib
                        fileFormat="JP2OpenJPEG"

                        # go through "SAFE"-directory structure of Sentinel-2

                        is_S2L1 = True

                        pathGranule = config.bigTilesDir / tile["folderName"] / "GRANULE"
                        for mainFolder in os.listdir(pathGranule):

                            pathImgData = pathGranule / mainFolder / "IMG_DATA"
                            for imgDataItem in os.listdir(pathImgData):

                                pathImgDataItem = pathImgData / imgDataItem

                                # if Level-1 data pathImgDataItem is already an image file
                                # if Level-2 data pathImgDataItem is a directory with image files

                                if os.path.isdir(pathImgDataItem):

                                    # Level-2 data

                                    is_S2L1 = False

                                    targetSubDir = targetDir / imgDataItem
                                
                                    for item in os.listdir(pathImgDataItem):

                                        # set path of img file
                                        path = pathImgDataItem / item

                                        # CROP IMAGE
                                        utils.cropImg(path, item, topLeft, bottomRight, targetSubDir, fileFormat)

                                    utils.createPreviewRGBImage("*B04_10m.jp2", "*B03_10m.jp2", "*B02_10m.jp2", targetSubDir, previewDir)
                                
                                else:

                                    # Level-1 data

                                    # set path of image file
                                    path = pathImgDataItem

                                    # CROP IMAGE
                                    utils.cropImg(path, imgDataItem, topLeft, bottomRight, targetDir, fileFormat)

                            if is_S2L1:
                                utils.createPreviewRGBImage("*B04.jp2", "*B03.jp2", "*B02.jp2", targetDir, previewDir)                                

                        print("done.\n")        

                        if config.copyMetadata:                            
                            print("Copy metadata...")
                            metaDir.mkdir(parents = True)
                            tileDir = config.bigTilesDir / tile["folderName"]
                            for item in tileDir.rglob('*'):
                                if item.is_file() and item.suffix.lower() != ".jp2":
                                    targetDir = metaDir / item.parent.relative_to(tileDir)
                                    if not targetDir.exists():
                                        targetDir.mkdir(parents = True)
                                    shutil.copy(item, targetDir)
                            print("done.\n")

                        if config.createSymlink:
                            tileDir = config.bigTilesDir / tile["folderName"]
                            # TODO: set config parameter for realpath or relpath for symlinks
                            metaDir.symlink_to(os.path.realpath(str(tileDir.resolve()), str(metaDir.parent.resolve())))
                            print("Symlink created.")

                        # set date for tile cropped 
                        db.setTileCropped(poiId, tile["rowid"], mainTargetFolder)


                    # LANDSAT CROPPING

                    if poi["platform"].startswith("LANDSAT"):
                    
                        # Landsat img data are in GeoTiff-format
                        # set appropriate format for GDAL lib
                        fileFormat="GTiff"

                        # all images are in root dir of tile

                        # set path of root dir of tile
                        pathImgData = config.bigTilesDir / tile["folderName"]

                        # TODO: switch to pathlib (for item in pathImgData)
                        # go through all files in root dir of tile
                        for item in os.listdir(pathImgData):

                            # if file ends with tif then crop
                            if item.lower().endswith(".tif"):

                                # set path of image file
                                path = pathImgData / item

                                # CROP IMAGE
                                utils.cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat)

                        if poi["platform"] == "LANDSAT_8_C1":
                            r_band_search_pattern = "*B4.TIF"
                            g_band_search_pattern = "*B3.TIF"
                            b_band_search_pattern = "*B2.TIF"
                        else:
                            r_band_search_pattern = "*B3.TIF"
                            g_band_search_pattern = "*B2.TIF"
                            b_band_search_pattern = "*B1.TIF"                           
                        utils.createPreviewRGBImage(r_band_search_pattern, g_band_search_pattern, b_band_search_pattern, targetDir, previewDir)                         

                        print("done.")

                        if config.copyMetadata:
                            print("Copy metadata...")
                            metaDir.mkdir(parents = True)
                            for item in pathImgData.glob('*'):
                                if item.is_file():
                                    if item.suffix.lower() != ".tif":
                                        shutil.copy(item, metaDir)
                                if item.is_dir():
                                    shutil.copytree(item, (metaDir / item.name))
                            print("done.\n")

                        if config.createSymlink:
                            tileDir = pathImgData
                            # TODO: set config parameter for realpath or relpath for symlink
                            metaDir.symlink_to(os.path.realpath(str(tileDir.resolve()), str(metaDir.parent.resolve())))
                            print("Symlink created.")                            

                        # set date for tile cropped 
                        db.setTileCropped(poiId, tile["rowid"], mainTargetFolder)                                              


    def getPoiParametersForOutputFolder(self, poi):
        
        folderElements = []
        folderName = ""

        try:
            folderElements.append("df" + utils.convertDate(poi["dateFrom"], "%Y%m%d"))
        except:
            pass

        try:
            folderElements.append("dt" + utils.convertDate(poi["dateTo"], "%Y%m%d"))
        except:
            pass
            
        try:
            if poi["platform"] == "Sentinel-1":
                folderElements.append("pfS1")
            if poi["platform"] == "Sentinel-2":
                folderElements.append("pfS2")
            if poi["platform"] == "LANDSAT_TM_C1":
                folderElements.append("pfLTM")
            if poi["platform"] == "LANDSAT_ETM_C1":
                folderElements.append("pfLETM")
            if poi["platform"] == "LANDSAT_8_C1":
                folderElements.append("pfL8")
        except:
            pass
            
        try:
            folderElements.append("tl" + str(poi["tileLimit"]))
        except:
            pass
            
        try:
            folderElements.append("cc" + str(poi["cloudcoverpercentage"]))
        except:
            pass
            
        try:
            folderElements.append("pm" + str(poi["polarisatiomode"]))
        except:
            pass
            
        try:
            folderElements.append("pt" + str(poi["producttype"]))
        except:
            pass
            
        try:
            folderElements.append("som" + str(poi["sensoroperationalmode"]))
        except:
            pass
            
        try:
            folderElements.append("si" + str(poi["swathidentifier"]))
        except:
            pass
            
        try:
            folderElements.append("tls" + str(poi["timeliness"]))
        except:
            pass
            
        try:
            folderElements.append("w" + str(poi["width"]))
        except:
            pass
            
        try:
            folderElements.append("h" + str(poi["height"]))
        except:
            pass

        for item in folderElements:
            if not item.endswith("None"):            
                if len(folderName) > 0:
                    folderName = folderName + "_"
                folderName = folderName + item

        return folderName
            

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

            self.unpackBigTiles()
            logger.info("Big tiles unpacked.")
            
            self.cropTiles(poiId)
            logger.info("Tiles cropped.")

        
        if products == None:
            return 0
        else:
            return len(products)

