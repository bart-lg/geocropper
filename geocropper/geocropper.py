import geocropper.log as log
import zipfile
import tarfile
from tqdm import tqdm
import os
import subprocess
import pathlib
import shutil
from dateutil.parser import *
import pyproj
from functools import partial
from pprint import pprint
import rasterio
import math 
from shapely.geometry import Point
from shapely.ops import transform

from geocropper.database import database
import geocropper.config as config
import geocropper.sentinelWrapper as sentinelWrapper
import geocropper.landsatWrapper as landsatWrapper
import geocropper.csvImport as csvImport

from osgeo import gdal
# gdal library distributed by conda destroys PATH environment variable
# see -> https://github.com/OSGeo/gdal/issues/1231
# workaround: remove first entry...
os.environ["PATH"] = os.environ["PATH"].split(';')[1]

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
        dateFrom = self.convertDate(dateFrom, "%Y%m%d")
        dateTo = self.convertDate(dateTo, "%Y%m%d")
        

        # print search info

        print("Search for Sentinel data:")
        self.printPosition()
        print("From: " + self.convertDate(dateFrom, "%d.%m.%Y"))
        print("To: " + self.convertDate(dateTo, "%d.%m.%Y"))
        print("Platform: " + platform)
        if tileLimit > 0:
            print("Tile-limit: %d" % tileLimit)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                print("%s: %s" %(key, str(value)))
        print("----------------------------\n")
        
        
        # search for sentinel data
        
        if int(tileLimit) > 0:
            products = self.sentinel.getSentinelProducts(self.lat, self.lon, dateFrom, dateTo, platform, limit=tileLimit, **kwargs)
        else:   
            products = self.sentinel.getSentinelProducts(self.lat, self.lon, dateFrom, dateTo, platform, **kwargs)

        print("Found tiles: %d\n" % len(products))


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
        dateFrom = self.convertDate(dateFrom)
        dateTo = self.convertDate(dateTo)


        # print search info

        print("Search for Landsat data:")
        self.printPosition()
        print("From: " + self.convertDate(dateFrom, "%d.%m.%Y"))
        print("To: " + self.convertDate(dateTo, "%d.%m.%Y"))
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
                        beginposition = self.convertDate(tile["beginposition"], newFormat="%Y%m%d-%H%M")
                    else:
                        beginposition = self.convertDate(tile["beginposition"], newFormat="%Y%m%d")

                    poiParameters = self.getPoiParametersForOutputFolder(poi)
                    connectionId = db.getTilePoiConnectionId(poiId, tile["rowid"])
                    mainTargetFolder = config.croppedTilesDir / poi["groupname"] / poiParameters / ( "%s_%s_%s_%s" % (connectionId, poi["lon"], poi["lat"], beginposition) )

                    # target directory for cropped image
                    targetDir = mainTargetFolder / "sensordata"
                    targetDir.mkdir(parents = True)               

                    # target directory for meta information
                    metaDir = mainTargetFolder / "original-metadata"
                    metaDir.mkdir(parents = True)

                    # target directory for preview image
                    previewDir = mainTargetFolder / "preview"
                    previewDir.mkdir(parents = True)               

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
                                        self.cropImg(path, item, topLeft, bottomRight, targetSubDir, fileFormat)

                                    self.createPreviewRGBImage("*B04_10m.jp2", "*B03_10m.jp2", "*B02_10m.jp2", targetSubDir, previewDir)
                                
                                else:

                                    # Level-1 data

                                    # set path of image file
                                    path = pathImgDataItem

                                    # CROP IMAGE
                                    self.cropImg(path, imgDataItem, topLeft, bottomRight, targetDir, fileFormat)

                            if is_S2L1:
                                self.createPreviewRGBImage("*B04.jp2", "*B03.jp2", "*B02.jp2", targetDir, previewDir)                                

                        print("done.\n")                                    

                        print("Copy metadata...")
                        tileDir = config.bigTilesDir / tile["folderName"]
                        for item in tileDir.rglob('*'):
                            if item.is_file() and item.suffix.lower() != ".jp2":
                                targetDir = metaDir / item.parent.relative_to(tileDir)
                                if not targetDir.exists():
                                    targetDir.mkdir(parents = True)
                                shutil.copy(item, targetDir)
                        print("done.\n")

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
                                self.cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat)

                        if poi["platform"] == "LANDSAT_8_C1":
                            r_band_search_pattern = "*B4.TIF"
                            g_band_search_pattern = "*B3.TIF"
                            b_band_search_pattern = "*B2.TIF"
                        else:
                            r_band_search_pattern = "*B3.TIF"
                            g_band_search_pattern = "*B2.TIF"
                            b_band_search_pattern = "*B1.TIF"                           
                        self.createPreviewRGBImage(r_band_search_pattern, g_band_search_pattern, b_band_search_pattern, targetDir, previewDir)                         

                        print("done.")

                        print("Copy metadata...")
                        for item in pathImgData.glob('*'):
                            if item.is_file():
                                if item.suffix.lower() != ".tif":
                                    shutil.copy(item, metaDir)
                            if item.is_dir():
                                shutil.copytree(item, (metaDir / item.name))
                        print("done.\n")

                        # set date for tile cropped 
                        db.setTileCropped(poiId, tile["rowid"], mainTargetFolder)                        


    def getPoiParametersForOutputFolder(self, poi):
        
        folderElements = []
        folderName = ""

        try:
            folderElements.append("df" + self.convertDate(poi["dateFrom"], "%Y%m%d"))
        except:
            pass

        try:
            folderElements.append("dt" + self.convertDate(poi["dateTo"], "%Y%m%d"))
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
            folderElements.append("tl" + poi["tileLimit"])
        except:
            pass
            
        try:
            folderElements.append("cc" + poi["cloudcoverpercentage"])
        except:
            pass
            
        try:
            folderElements.append("pm" + poi["polarisatiomode"])
        except:
            pass
            
        try:
            folderElements.append("pt" + poi["producttype"])
        except:
            pass
            
        try:
            folderElements.append("som" + poi["sensoroperationalmode"])
        except:
            pass
            
        try:
            folderElements.append("si" + poi["swathidentifier"])
        except:
            pass
            
        try:
            folderElements.append("tls" + poi["timeliness"])
        except:
            pass
            
        try:
            folderElements.append("w" + poi["width"])
        except:
            pass
            
        try:
            folderElements.append("h" + poi["height"])
        except:
            pass

        for item in folderElements:
            if len(folderName) > 0:
                folderName = folderName + "_"
            folderName = folderName + item

        return folderName
            


    def cropImg(self, path, item, topLeft, bottomRight, targetDir, fileFormat):
    
        # open raster image file
        img = rasterio.open(str(path))

        # prepare parameters for coordinate system transform function 
        toTargetCRS = partial(pyproj.transform, \
            pyproj.Proj('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs '), pyproj.Proj(img.crs))

        # transform corner coordinates for cropping
        topLeftTransformed = transform(toTargetCRS, topLeft)
        bottomRightTransformed = transform(toTargetCRS, bottomRight)

        # open image with GDAL
        ds = gdal.Open(str(path))

        # make sure that target directory exists
        if not os.path.isdir(str(targetDir)):
            os.makedirs(str(targetDir))

        # CROP IMAGE
        ds = gdal.Translate(str(targetDir / item), ds, format=fileFormat, \
            projWin = [topLeftTransformed.x, topLeftTransformed.y, \
            bottomRightTransformed.x, bottomRightTransformed.y])

        ds = None


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
        dateFrom = self.convertDate(dateFrom)
        dateTo = self.convertDate(dateTo)


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
            
            self.cropTiles(poiId)

        
        if products == None:
            return 0
        else:
            return len(products)



    def convertDate(self, date, newFormat="%Y-%m-%d"):
        temp = parse(date)
        return temp.strftime(newFormat)


    def createPreviewRGBImage(self, r_band_search_pattern, g_band_search_pattern, b_band_search_pattern, source_dir, \
        target_dir, max_scale = 4096, exponential_scale = 0.5):

        print("Create preview image...")

        search_result = list(source_dir.glob(r_band_search_pattern))
        if len(search_result) == 0:
            return
        r_band = search_result[0]
        
        search_result = list(source_dir.glob(g_band_search_pattern))
        if len(search_result) == 0:
            return
        g_band = search_result[0]

        search_result = list(source_dir.glob(b_band_search_pattern))
        if len(search_result) == 0:
            return
        b_band = search_result[0]

        preview_file = "preview.tif"
        if ( target_dir / preview_file ).exists():
            i = 2
            preview_file = "preview(" + i + ").tif"
            while i < 100 and ( target_dir / preview_file ).exists():
                i = i + 1
                preview_file = "preview(" + i + ").tif"
            # TODO: throw exception if i > 99

        # rescale red band
        command = ["gdal_translate", "-ot", "Byte", "-scale", "0", str(max_scale), "0", "255", "-exponent", \
                   str(exponential_scale), str( r_band ), str( target_dir / "r-scaled.tif")]
        subprocess.call(command)

        # rescale green band
        command = ["gdal_translate", "-ot", "Byte", "-scale", "0", str(max_scale), "0", "255", "-exponent", \
                   str(exponential_scale), str( g_band ), str( target_dir / "g-scaled.tif")]
        subprocess.call(command)

        # rescale blue band
        command = ["gdal_translate", "-ot", "Byte", "-scale", "0", str(max_scale), "0", "255", "-exponent", \
                   str(exponential_scale), str( b_band ), str( target_dir / "b-scaled.tif")]
        subprocess.call(command)

        # create preview image
        command = ["gdal_merge.py", "-v", "-ot", "Byte", "-separate", "-of", "GTiff", "-co", "PHOTOMETRIC=RGB", \
                   "-o", str( target_dir / preview_file ), str( target_dir / "r-scaled.tif" ), str( target_dir / "g-scaled.tif" ), \
                   str( target_dir / "b-scaled.tif" )]
        subprocess.call(command)                   

        # remove scaled bands
        ( target_dir / "r-scaled.tif" ).unlink()
        ( target_dir / "g-scaled.tif" ).unlink()
        ( target_dir / "b-scaled.tif" ).unlink()
