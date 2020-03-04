import geocropper.log as log
import zipfile
import tarfile
from tqdm import tqdm
import os
import pathlib
from dateutil.parser import *
import pyproj
from osgeo import gdal
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

logger = log.setupCustomLogger('main')
db = database()

# TODO: addTile folderName not required anymore - improve code!

# TODO: use pathlib for all paths!


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
        Latitude of the geolocation
    lon : float
        Longitude of the geolocation

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
                        tileId = db.addTile(platform, key, folderName)
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
                        tileId = db.addTile(platform, key, folderName)
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

                tileId = None
                tile = db.getTile(productId = product["entityId"])

                # TODO: check if existing tar file is complete => needs to be deleted and re-downloaded

                # check for previous downloads
                if not pathlib.Path(config.bigTilesDir / folderName).is_dir() and \
                  not pathlib.Path(config.bigTilesDir / (product["displayId"] + ".tar.gz") ).is_file():                

                    # no previous download detected...

                    # only add new tile to database if not existing
                    # this leads automatically to a resume functionality
                    if tile == None:
                        tileId = db.addTile(platform, product["entityId"], folderName)
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
                        tileId = db.addTile(platform, product["entityId"], folderName)
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

                        pathGranule = config.bigTilesDir / tile["folderName"] / "GRANULE"
                        for mainFolder in os.listdir(pathGranule):

                            pathImgData = pathGranule / mainFolder / "IMG_DATA"
                            for imgDataItem in os.listdir(pathImgData):

                                pathImgDataItem = pathImgData / imgDataItem

                                # if Level-1 data pathImgDataItem is already an image file
                                # if Level-2 data pathImgDataItem is a directory with image files

                                # TODO: combine these two cases somehow...

                                if os.path.isdir(pathImgDataItem):

                                    # Level-2 data
                                
                                    for item in os.listdir(pathImgDataItem):

                                        # set path of img file
                                        path = pathImgDataItem / item

                                        # TODO: dirty... (removes ".SAFE" from folderName)
                                        tileFolderName = tile["folderName"]
                                        tileName = tileFolderName[:-5]

                                        # target directory for cropped image
                                        targetDir = config.croppedTilesDir / poi["country"] / \
                                            ("lat%s_lon%s" % (poi["lat"], poi["lon"])) / \
                                            ("w%s_h%s" % (poi["width"], poi["height"])) / \
                                            tileName / imgDataItem

                                        # CROP IMAGE
                                        self.cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat)
                                
                                else:

                                    # Level-1 data

                                    # set path of image file
                                    path = pathImgDataItem

                                    # TODO: dirty... (removes ".SAFE" from folderName)
                                    tileFolderName = tile["folderName"]
                                    tileName = tileFolderName[:-5]

                                    # target directory for cropped image
                                    targetDir = config.croppedTilesDir / poi["country"] / \
                                        ("lat%s_lon%s" % (poi["lat"], poi["lon"])) / \
                                        ("w%s_h%s" % (poi["width"], poi["height"])) / tileName                                        

                                    # CROP IMAGE
                                    self.cropImg(path, imgDataItem, topLeft, bottomRight, targetDir, fileFormat)


                        # set date for tile cropped 
                        db.setTileCropped(poiId, tile["rowid"])

                        print("done.\n")                                    


                    # LANDSAT CROPPING

                    if poi["platform"].startswith("LANDSAT"):
                    
                        # Landsat img data are in GeoTiff-format
                        # set appropriate format for GDAL lib
                        fileFormat="GTiff"

                        # all images are in root dir of tile

                        # set path of root dir of tile
                        pathImgData = config.bigTilesDir / tile["folderName"]

                        # go through all files in root dir of tile
                        for item in os.listdir(pathImgData):

                            # if file ends with tif then crop
                            if item.lower().endswith(".tif"):

                                # set path of image file
                                path = pathImgData / item

                                # target directory for cropped image
                                targetDir = config.croppedTilesDir / poi["country"] / \
                                        ("lat%s_lon%s" % (poi["lat"], poi["lon"])) / \
                                        ("w%s_h%s" % (poi["width"], poi["height"])) / tile["folderName"]

                                # CROP IMAGE
                                self.cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat)


                        # set date for tile cropped 
                        db.setTileCropped(poiId, tile["rowid"])

                        print("done.\n")


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


    def downloadAndCrop(self, dateFrom, dateTo, platform, width, height, tileLimit = 0, **kwargs):
        """Download and crop/clip Sentinel or Landsat tiles to directories specified in the config file.

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

        """


        # convert date formats
        dateFrom = self.convertDate(dateFrom)
        dateTo = self.convertDate(dateTo)


        # check if point of interest (POI) exists in database
        # if not, create new POI record

        poi = db.getPoi(self.lat, self.lon, dateFrom, dateTo, platform, width, height, tileLimit=tileLimit, **kwargs)

        if poi == None:     
            poiId = db.addPoi(self.lat, self.lon, dateFrom, dateTo, platform, width, height, tileLimit, **kwargs)
        else:
            poiId = poi["rowid"]


        # TODO: save metadata from search response?


        # search and download tiles

        if platform.startswith("Sentinel"):
            products = self.downloadSentinelData(dateFrom, dateTo, platform, poiId=poiId, tileLimit=tileLimit, **kwargs)
        
        if platform.startswith("LANDSAT"):
            products = self.downloadLandsatData(dateFrom, dateTo, platform, poiId=poiId, tileLimit=tileLimit, **kwargs)


        # if tiles found, unpack and crop them

        if len(products) > 0:

            self.unpackBigTiles()
            
            self.cropTiles(poiId)


        # TODO: check if there are any outstanding downloads or crops


    def convertDate(self, date, newFormat="%Y-%m-%d"):
        temp = parse(date)
        return temp.strftime(newFormat)
