import log
import config

logger = log.setupCustomLogger('main')

import sys
sys.path.append("./lib")

import sentinelWrapper
import landsatWrapper

from database import database
import zipfile
import tarfile
from tqdm import tqdm
import os
import pyproj
from osgeo import gdal
from functools import partial
from pprint import pprint
import rasterio
import math 
from shapely.geometry import Point
from shapely.ops import transform

import csvImport

logger.info("modules loaded")

db = database()

# TODO: convert date format of dateFrom and dateTo

# TODO: addTile folderName not required anymore - improve code!

# TODO: rename fromDate and toDate to dateFrom and dateTo (like in DB...)

def importAllCSVs():
    csvImport.importAllCSVs()

def init(lat, lon):
    return Geocropper(lat, lon)

class Geocropper:

    def __init__(self, lat , lon):
        self.lat = lat
        self.lon = lon
        print("\nGeocropper initialized.")

    def printPosition(self):
        print("lat: " + str(self.lat))
        print("lon: " + str(self.lon))
        logger.info("new geocropper instance initialized") 

    def downloadSentinelData(self, fromDate, toDate, platform, poiId = 0, tileLimit = 0, **kwargs):

        self.sentinel = sentinelWrapper.sentinelWrapper()
    
        print("\nSearch for Sentinel data:")
        self.printPosition()
        print("From: " + fromDate)
        print("To: " + toDate)
        print("Platform: " + platform)
        if tileLimit > 0:
            print("Tile-limit: %d" % tileLimit)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                print("%s: %s" %(key, str(value)))

        print("=========================================================\n")
        
        if int(tileLimit) > 0:
            products = self.sentinel.getSentinelProducts(self.lat, self.lon, fromDate, toDate, platform, limit=tileLimit, **kwargs)
        else:   
            products = self.sentinel.getSentinelProducts(self.lat, self.lon, fromDate, toDate, platform, **kwargs)
        
        print("Found tiles: %d\n" % len(products))

        print("Download")
        print("-----------------\n")

        # TODO: What if no tiles could be found??
        
        i = 1
        for key in products:
            
            folderName = products[key]["title"] + ".SAFE"
            tileId = None
            tile = db.getTile(productId = key)
            
            if not os.path.isdir("%s/%s" % (config.bigTilesDir, folderName)) and \
              not os.path.isfile("%s/%s.zip" % (config.bigTilesDir, products[key]["title"])):
                
                if tile == None:
                    tileId = db.addTile(platform, key, folderName)
                else:
                    tileId = tile["rowid"]
                    db.setDownloadRequestForTile(tileId)

                print("[%d/%d]: Download %s" % (i, len(products), products[key]["title"]))
                self.sentinel.downloadSentinelProduct(key)

                if os.path.isfile("%s/%s.zip" % (config.bigTilesDir, products[key]["title"])):
                    db.setDownloadCompleteForTile(tileId)
            
            else:

                if tile == None:
                    tileId = db.addTile(platform, key, folderName)
                else:
                    tileId = tile["rowid"]
                
                print("[%d/%d]: %s already exists." % (i, len(products), products[key]["title"]))

            if int(poiId) > 0:
                
                tilePoi = db.getTileForPoi(poiId, tileId)
                if tilePoi == None:
                    db.addTileForPoi(poiId, tileId)

            i += 1
            
        del self.sentinel
        
        if int(poiId) > 0:
            db.setTilesIdentifiedForPoi(poiId)
        
        return products
        
    def downloadLandsatData(self, fromDate, toDate, platform, poiId = 0, tileLimit = 0, **kwargs):
    
        self.landsat = landsatWrapper.landsatWrapper()
        
        print("\nSearch for Landsat data:")
        self.printPosition()
        print("From: " + fromDate)
        print("To: " + toDate)
        print("Platform: " + platform)
        if tileLimit > 0:
            print("Tile-limit: %d" % tileLimit)
        # dirty...
        maxCloudCoverage = 100
        for key, value in kwargs.items():
            if key == "cloudcoverpercentage":
                maxCloudCoverage = value
                print("%s: %s" %(key, str(value)))

        print("=========================================================\n")        
        
        products = self.landsat.getLandsatProducts(self.lat, self.lon, fromDate, toDate, platform, maxCloudCoverage, tileLimit)

        print("Found tiles: %d\n" % len(products))

        print("Download")
        print("-----------------\n")
        
        i = 1
        for product in products:
            
            folderName = product["displayId"]
            tileId = None
            tile = db.getTile(productId = product["entityId"])

            # TODO: check if existing tar file is complete

            if not os.path.isdir("%s/%s" % (config.bigTilesDir, folderName)) and \
              not os.path.isfile("%s/%s.tar.gz" % (config.bigTilesDir, product["displayId"])):

                if tile == None:
                    tileId = db.addTile(platform, product["entityId"], folderName)
                else:
                    tileId = tile["rowid"]
                    db.setDownloadRequestForTile(tileId)

                print("[%d/%d]: Download %s" % (i, len(products), product["displayId"]))
                self.landsat.downloadLandsatProduct(product["entityId"])

                if os.path.isfile("%s/%s.tar.gz" % (config.bigTilesDir, product["displayId"])):
                    db.setDownloadCompleteForTile(tileId)

            else:

                if tile == None:
                    tileId = db.addTile(platform, product["entityId"], folderName)
                else:
                    tileId = tile["rowid"]
                
                print("[%d/%d]: %s already exists." % (i, len(products), product["displayId"]))                    

            if int(poiId) > 0:
                
                tilePoi = db.getTileForPoi(poiId, tileId)
                if tilePoi == None:
                    db.addTileForPoi(poiId, tileId)     
                    
            i += 1       
            
        del self.landsat
        
        if int(poiId) > 0:
           db.setTilesIdentifiedForPoi(poiId)
        
        return products        


    def unpackBigTiles(self):

        logger.info("start of unpacking tile zip/tar files")
        
        print("\nUnpack big tiles:")
        print("-----------------\n")
        
        filesNumZip = len([f for f in os.listdir(config.bigTilesDir) 
             if f.endswith('.zip') and os.path.isfile(os.path.join(config.bigTilesDir, f))])
        filesNumTar = len([f for f in os.listdir(config.bigTilesDir) 
             if f.endswith('.tar.gz') and os.path.isfile(os.path.join(config.bigTilesDir, f))])
        filesNum = filesNumZip + filesNumTar

        i = 1
        for item in os.listdir(config.bigTilesDir):

            if item.endswith(".zip") or item.endswith(".tar.gz"):
            
                filePath = config.bigTilesDir + "/" + item
                print("[%d/%d] %s:" % (i, filesNum, item))

                if item.endswith(".zip"):

                    # dirty... (is maybe first entry of zipRef)
                    newFolderName = item[:-4] + ".SAFE"
                    tile = db.getTile(folderName = newFolderName)              

                    with zipfile.ZipFile(file=filePath) as zipRef:
                        
                        for file in tqdm(iterable=zipRef.namelist(), total=len(zipRef.namelist())):
                            zipRef.extract(member=file, path=config.bigTilesDir)

                    zipRef.close()



                if item.endswith(".tar.gz"):

                    tile = db.getTile(folderName = item[:-7])

                    targetDir = "%s/%s" % (config.bigTilesDir, tile["folderName"])

                    if not os.path.isdir(targetDir):
                        os.makedirs(targetDir)                    

                    with tarfile.open(name=filePath, mode="r:gz") as tarRef:

                        for file in tqdm(iterable=tarRef.getmembers(), total=len(tarRef.getmembers())):
                            tarRef.extract(member=file, path=targetDir)

                    tarRef.close()


                os.remove(filePath)

                db.setUnzippedForTile(tile["rowid"])

                i += 1

        logger.info("tile zip/tar files extracted")


    def cropTiles(self, poiId):
        
        print("\nCrop tiles:")
        print("-----------------")

        poi = db.getPoiFromId(poiId)

        print("(w: %d, h: %d)\n" % (poi["width"], poi["height"]))

        if not poi == None:

            diag = math.sqrt((poi["width"]/2)**2 + (poi["height"]/2)**2)

            topLeftLon, topLeftLat, backAzimuth = (pyproj.Geod(ellps="WGS84").fwd(poi["lon"],poi["lat"],315,diag))
            bottomRightLon, bottomRightLat, backAzimuth = (pyproj.Geod(ellps="WGS84").fwd(poi["lon"],poi["lat"],135,diag))

            topLeft = Point(topLeftLon, topLeftLat)
            bottomRight = Point(bottomRightLon, bottomRightLat) 

            tiles = db.getTilesForPoi(poiId)
            
            for tile in tiles:
            
                if tile["tileCropped"] == None:

                    print("Cropping %s ..." % tile["folderName"])

                    if poi["platform"] == "Sentinel-1":

                        fileFormat="GTiff"

                        pathItems = "%s/%s/measurement" % (config.bigTilesDir, tile["folderName"])

                        for item in os.listdir(pathItems):

                            if item.lower().endswith(".tiff"):

                                path = "%s/%s" % (pathItems, item)

                                targetDir = "%s/%s/lat%s_lon%s/w%s_h%s/%s" % \
                                    (config.croppedTilesDir, poi["country"], poi["lat"], poi["lon"], \
                                    poi["width"], poi["height"], tile["folderName"])

                                self.cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat)                        

                    if poi["platform"] == "Sentinel-2":

                        fileFormat="JP2OpenJPEG"

                        pathGranule = "%s/%s/GRANULE" \
                            % (config.bigTilesDir, tile["folderName"])
                        for mainFolder in os.listdir(pathGranule):

                            pathImgData = "%s/%s/IMG_DATA" % (pathGranule, mainFolder)
                            for imgDataItem in os.listdir(pathImgData):

                                pathImgDataItem = "%s/%s" % (pathImgData, imgDataItem)

                                # TODO: combine these two cases somehow...
                                if os.path.isdir(pathImgDataItem):
                                
                                    for item in os.listdir(pathImgDataItem):

                                        path = "%s/%s" % (pathImgDataItem, item)

                                        # dirty... (removes ".SAFE" from folderName)
                                        tileFolderName = tile["folderName"]
                                        tileName = tileFolderName[:-5]

                                        targetDir = "%s/%s/lat%s_lon%s/w%s_h%s/%s/%s" % \
                                            (config.croppedTilesDir, poi["country"], poi["lat"], poi["lon"], \
                                            poi["width"], poi["height"], tileName, imgDataItem)

                                        self.cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat)
                                
                                else:

                                    path = pathImgDataItem

                                    # dirty... (removes ".SAFE" from folderName)
                                    tileFolderName = tile["folderName"]
                                    tileName = tileFolderName[:-5]

                                    # TODO: the targetDir should reflect the spatial resolution as in the orig path
                                    targetDir = "%s/%s/lat%s_lon%s/w%s_h%s/%s" % \
                                        (config.croppedTilesDir, poi["country"], poi["lat"], poi["lon"], \
                                        poi["width"], poi["height"], tileName)

                                    self.cropImg(path, imgDataItem, topLeft, bottomRight, targetDir, fileFormat)


                    if poi["platform"].startswith("LANDSAT"):
                    
                        fileFormat="GTiff"

                        pathImgData = "%s/%s" % (config.bigTilesDir, tile["folderName"])

                        for item in os.listdir(pathImgData):

                            if item.lower().endswith(".tif"):

                                path = "%s/%s" % (pathImgData, item)

                                targetDir = "%s/%s/lat%s_lon%s/w%s_h%s/%s" % \
                                    (config.croppedTilesDir, poi["country"], poi["lat"], poi["lon"], \
                                    poi["width"], poi["height"], tile["folderName"])

                                self.cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat)

                    db.setTileCropped(poiId, tile["rowid"])
                    print("done.")

        print("")

    def cropImg(self, path, item, topLeft, bottomRight, targetDir, fileFormat):
    
        img = rasterio.open(path)

        toTargetCRS = partial(pyproj.transform, \
            pyproj.Proj('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs '), pyproj.Proj(img.crs))

        topLeftTransformed = transform(toTargetCRS, topLeft)
        bottomRightTransformed = transform(toTargetCRS, bottomRight)

        ds = gdal.Open(path)

        if not os.path.isdir(targetDir):
            os.makedirs(targetDir)

        ds = gdal.Translate("%s/%s" % (targetDir, item), ds, format=fileFormat, \
            projWin = [topLeftTransformed.x, topLeftTransformed.y, \
            bottomRightTransformed.x, bottomRightTransformed.y])

        ds = None


    def downloadAndCrop(self, fromDate, toDate, platform, width, height, tileLimit = 0, **kwargs):

        poi = db.getPoi(self.lat, self.lon, fromDate, toDate, platform, width, height, tileLimit=tileLimit, **kwargs)

        if poi == None:     
            poiId = db.addPoi(self.lat, self.lon, fromDate, toDate, platform, width, height, tileLimit, **kwargs)
        else:
            poiId = poi["rowid"]

        # TODO: save metadata from search response?

        if platform.startswith("Sentinel"):
            products = self.downloadSentinelData(fromDate, toDate, platform, poiId=poiId, tileLimit=tileLimit, **kwargs)
        
        if platform.startswith("LANDSAT"):
            products = self.downloadLandsatData(fromDate, toDate, platform, poiId=poiId, tileLimit=tileLimit, **kwargs)

        self.unpackBigTiles()
        
        self.cropTiles(poiId)

        # TODO: check if there are any outstanding downloads or crops
