import sys
sys.path.append("./lib")

import sentinelWrapper
from database import database
import zipfile
import tqdm
import os
import config
import pyproj
from osgeo import gdal
from functools import partial
from pprint import pprint
import rasterio
import math 
from shapely.geometry import Point
from shapely.ops import transform
from countries import countries

db = database()

class Geocropper:

    def __init__(self, lat , lon):
        self.lat = lat
        self.lon = lon
        self.sentinel = sentinelWrapper.sentinelWrapper()

    def printPosition(self):
        print("lat: " + str(self.lat))
        print("lon: " + str(self.lon))

    def downloadSentinelData(self, fromDate, toDate, platform, poiId = 0, tileLimit = 0, **kwargs):

        print("\nDownload Sentinel data for:")
        self.printPosition()
        print("From: " + fromDate)
        print("To: " + toDate)
        print("Platform: " + platform)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                print("%s: %s" %(key, str(value)))

        print("=========================================================\n")
        
        if int(tileLimit) > 0:
            products = self.sentinel.getSentinelProducts(self.lat, self.lon, fromDate, toDate, platform, limit=tileLimit, **kwargs)
        else:   
            products = self.sentinel.getSentinelProducts(self.lat, self.lon, fromDate, toDate, platform, **kwargs)
        print("Found tiles: " + str(len(products)))

        # TODO: What if no tiles could be found??
        
        i = 1
        for key in products:
            
            folderName = products[key]["title"] + ".SAFE"

            if not os.path.isdir(config.bigTilesDir + "/" + folderName) and \
              not os.path.isfile(config.bigTilesDir + "/" + products[key]["title"] + ".zip"):
                
                qresult = db.fetchQuery("SELECT * FROM Tiles WHERE platform = ? AND folderName = ? AND productId = ? ", (platform, folderName, key))
                if len(qresult) > 0:
                    db.query("UPDATE Tiles SET lastDownloadRequest = datetime('now', 'localtime') \
                        WHERE platform = ? AND folderName = ? AND productId = ?", (platform, folderName, key))
                else:
                    db.query("INSERT INTO Tiles (platform, folderName, productId, firstDownloadRequest, lastDownloadRequest) \
                        VALUES (?, ?, ?, datetime('now', 'localtime'), datetime('now', 'localtime'))", (platform, folderName, key))

                print("[" + str(i) + "/" + str(len(products)) + "]: Download " + products[key]["title"])
                self.sentinel.downloadSentinelProduct(key)

                if os.path.isfile(config.bigTilesDir + "/" + products[key]["title"] + ".zip"):
                    db.query("UPDATE Tiles SET downloadComplete = datetime('now', 'localtime') \
                        WHERE platform = ? AND folderName = ? AND productId = ?", (platform, folderName, key))
            
            else:

                qresult = db.fetchQuery("SELECT * FROM Tiles WHERE platform = ? AND folderName = ? AND productId = ? ", (platform, folderName, key))
                if len(qresult) == 0:
                    db.query("INSERT INTO Tiles (platform, folderName, productId) \
                        VALUES (?, ?, ?)", (platform, folderName, key))
                
                print("[" + str(i) + "/" + str(len(products)) + "]: " + products[key]["title"] + " already exists.")

            if int(poiId) > 0:
                
                tileObj = self.getTileFromDb(platform, folderName, key)
                tileId = tileObj[0][0]

                qresult = db.fetchQuery("SELECT * FROM TilesForPOIs WHERE poiId = ? AND tileId = ?", (poiId, tileId))
                if len(qresult) == 0:
                    db.query("INSERT INTO TilesForPOIs (poiId, tileId) VALUES ( ?, ?)", (poiId, tileId))

            i += 1
        
        if int(poiId) > 0:
            db.query("UPDATE PointOfInterests SET tilesIdentified = datetime('now', 'localtime') WHERE rowid = " + str(poiId))

        self.unpackBigTiles()
        
        return products

    def unpackBigTiles(self):
        
        print("\nUnpack big tiles:\n")
        
        for item in os.listdir(config.bigTilesDir):
            
            if item.endswith(".zip"):
                
                filePath = config.bigTilesDir + "/" + item
                print(item + ":")
                
                with zipfile.ZipFile(file=filePath) as zipRef:
                    
                    for file in tqdm.tqdm(iterable=zipRef.namelist(), total=len(zipRef.namelist())):
                        zipRef.extract(member=file, path=config.bigTilesDir)

                zipRef.close()
                os.remove(filePath)

                # dirty...
                newFolderName = item[:-4] + ".SAFE"
                db.query("UPDATE Tiles SET unzipped = datetime('now', 'localtime') WHERE folderName = '" + newFolderName + "'")

    def downloadAndCrop(self, fromDate, toDate, platform, width, height, tileLimit = 0, **kwargs):

        # TODO: first check for resumes

        query = "SELECT rowid FROM PointOfInterests WHERE lat = " + str(self.lat) + " AND lon = " + str(self.lon) + " "
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + "AND " + key + " = '" + str(value) + "' "

        qresult = db.fetchQuery(query + "AND dateFrom = ? AND dateTo = ? AND platform = ? AND width = ? AND height = ? AND tileLimit = ?", \
            (fromDate, toDate, platform, width, height, tileLimit))

        if len(qresult) == 0:

            query = "INSERT INTO PointOfInterests (lat, lon"
            for key, value in kwargs.items():
                if key in config.optionalSentinelParameters:
                    query = query + ", " + key
            query = query + ", country, dateFrom, dateTo, platform, width, height, tileLimit, description, poicreated) "
            query = query + " VALUES (" + str(self.lat) + ", " + str(self.lon)
            for key, value in kwargs.items():
                if key in config.optionalSentinelParameters:
                    query = query + ", '" + str(value) + "'"
            query = query + ", '" + self.getCountry() + "', '" + fromDate + "', '" + toDate + "', '" + platform + "', " + str(width) + ", " + str(height) \
                + ", " + str(tileLimit) + ", " + "'', datetime('now', 'localtime'))"
            poiId = db.query(query)

        else:
            poiId = qresult[0][0]

        products = self.downloadSentinelData(fromDate, toDate, platform, poiId=poiId, tileLimit=tileLimit, **kwargs)

    def getCountry(self):
        cc = countries.CountryChecker(config.worldBordersShapeFile)
        return cc.getCountry(countries.Point(self.lat, self.lon)).iso

    def getTileFromDb(self, platform, folderName, productId):

        qresult = db.fetchQuery("SELECT rowid, * FROM Tiles WHERE platform = ? AND folderName = ? AND productId = ? ", (platform, folderName, productId))
        return qresult

    def getPoiFromDb(self, lat, lon, fromDate, toDate, platform, width, height, **kwargs):

        query = "SELECT rowid, * FROM PointOfInterests WHERE lat = " + str(lat) + " AND lon = " + str(lon) + " AND dateFrom = '" + fromDate \
            + " AND dateTo = '" + toDate + "' AND platform = '" + platform + "' AND width = " + str(width) + " AND height = " + str(height)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + " AND " + key + " = '" + str(value) + "'"

        qresult = db.fetchQuery(query)
        return qresult
