import sqlite3
import geocropper.config as config

import sys
import os
sys.path.append(os.path.join(os.path.realpath('.'), "lib"))

from countries import countries

import logging


# NEVER USE DELETE IN TABLES WITH RELATIONS!
# SQLITE REUSES IDs!
# THIS COULD BE BAD FOR RELATIONS!


# get logger object
logger = logging.getLogger('root')


### DB structure

tables = {

    # table PointOfInterest
    # holds the information for a geocropper call - one record for every point/parameter combination
    # optional landsat parameters => max_cloud_cover (value in db stored as cloudcoverpercentage)
    "PointOfInterests": {
        "groupname":                "TEXT",
        "country":                  "TEXT",
        "lat":                      "REAL",
        "lon":                      "REAL",
        "dateFrom":                 "TEXT",
        "dateTo":                   "TEXT",
        "platform":                 "TEXT",
        "polarisationmode":         "TEXT",
        "producttype":              "TEXT",
        "sensoroperationalmode":    "TEXT",
        "swathidentifier":          "TEXT",
        "cloudcoverpercentage":     "TEXT",
        "timeliness":               "TEXT",
        "width":                    "INTEGER",
        "height":                   "INTEGER",
        "tileLimit":                "INTEGER",
        "description":              "TEXT",
        "tilesIdentified":          "TEXT",
        "poicreated":               "TEXT",
        "cancelled":                "TEXT"
    },

    # table Tiles
    # information about downloaded big tiles    
    "Tiles": {
        "platform":                 "TEXT",
        "folderName":               "TEXT",
        "productId":                "TEXT",
        "beginposition":            "TEXT",
        "endposition":              "TEXT",
        "firstDownloadRequest":     "TEXT",
        "lastDownloadRequest":      "TEXT",
        "downloadComplete":         "TEXT",
        "unzipped":                 "TEXT",
        "cancelled":                "TEXT",
        "projection":               "TEXT"
    },

    # table TilesForPOIs
    # n:m relation between tables PointOfInterest and Tiles
    # additional information: date of image cropping based on parameters of POI
    "TilesForPOIs": {
        "poiId":                    "INTEGER",
        "tileId":                   "INTEGER",
        "path":                     "TEXT",
        "tileCropped":              "TEXT",
        "cancelled":                "TEXT"
        #"projection":               "TEXT",
        #"leftTopCorner":            "TEXT",
        #"rightBottomCorner":        "TEXT",
        #"pixelWidth":               "INTEGER",
        #"pixelHeight":              "INTEGER"
    },

    # table CSVInput
    # holds imported records which have not yet been processed (loaded)
    "CSVInput": {
        "fileName":                 "TEXT",
        "groupname":                "TEXT",
        "lat":                      "REAL",
        "lon":                      "REAL",
        "dateFrom":                 "TEXT",
        "dateTo":                   "TEXT",
        "platform":                 "TEXT",
        "polarisationmode":         "TEXT",
        "producttype":              "TEXT",
        "sensoroperationalmode":    "TEXT",
        "swathidentifier":          "TEXT",
        "cloudcoverpercentage":     "TEXT",
        "timeliness":               "TEXT",
        "width":                    "INTEGER",
        "height":                   "INTEGER",
        "tileLimit":                "INTEGER",
        "description":              "TEXT",
        "csvImported":              "TEXT",
        "cancelled":                "TEXT"
    },   

    # table CSVLoaded
    # holds imported and processed/loaded records
    "CSVLoaded": {
        "fileName":                 "TEXT",
        "groupname":                "TEXT",
        "lat":                      "REAL",
        "lon":                      "REAL",
        "dateFrom":                 "TEXT",
        "dateTo":                   "TEXT",
        "platform":                 "TEXT",
        "polarisationmode":         "TEXT",
        "producttype":              "TEXT",
        "sensoroperationalmode":    "TEXT",
        "swathidentifier":          "TEXT",
        "cloudcoverpercentage":     "TEXT",
        "timeliness":               "TEXT",
        "width":                    "INTEGER",
        "height":                   "INTEGER",
        "tileLimit":                "INTEGER",
        "description":              "TEXT",
        "csvImported":              "TEXT",
        "cancelled":                "TEXT",
        "csvLoaded":                "TEXT"
    }  

}


### DB class

class database:

    def __init__(self):

        self.openConnection()


        # create new tables if not existing
        for tableName, tableContent in tables.items():

            elements = ""
            for columnName, dataType in tableContent.items():
                if elements == "":
                    elements = "%s %s" % (columnName, dataType)
                else:
                    elements = "%s, %s %s" % (elements, columnName, dataType)

            query = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + elements + ")"

            self.cursor.execute(query)

        # save changes to database
        self.connection.commit()

        logger.info("tables created if non existing")


        # check tables for missing columns (e.g. new columns in newer versions)
        for tableName, tableContent in tables.items():
            
            for columnName, dataType in tableContent.items():
            
                result = self.fetchFirstRowQuery(f"SELECT COUNT(*) AS num FROM pragma_table_info('{tableName}') \
                                                   WHERE name='{columnName}'")
            
                if result == None or result["num"] == 0:

                    # column is missing and needs to be appended
                    self.query(f"ALTER TABLE {tableName} ADD {columnName} {dataType};")
                    logger.info(f"db: column {columnName} added to table {tableName}")


    def __del__(self):

        self.closeConnection()


    def openConnection(self):

        logger.info("start DB connection")

        # open or create sqlite database file
        self.connection = sqlite3.connect(config.dbFile)

        # provide index-based and case-insensitive name-based access to columns
        self.connection.row_factory = sqlite3.Row

        # create sqlite cursor object to execute SQL commands
        self.cursor = self.connection.cursor()

        logger.info("DB connected")


    def closeConnection(self):

        self.connection.close()


    ### QUERIES ###
        
    # query function used for inserts and updates
    def query(self, query, values=None):
        logger.info("DB query: [%s] [values: %s]" % (query, values))
        if values == None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, values)
        # save changes
        self.connection.commit()
        newId = self.cursor.lastrowid
        return newId

    # query function used for selects returning all rows of result
    def fetchAllRowsQuery(self, query, values=None):
        if values == None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, values)
        return self.cursor.fetchall()

    # query function used for selects returning only first row of result
    def fetchFirstRowQuery(self, query, values=None):
        if values == None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, values)
        return self.cursor.fetchone()
        

    ### TILES ###
        
    def getTile(self, productId = None, folderName = None):
        if not productId == None and not folderName == None:
            qresult = self.fetchFirstRowQuery("SELECT rowid, * FROM Tiles WHERE productId = '?' AND folderName = '?'", \
                (productId, folderName))
        else:
            if not productId == None:
                qresult = self.fetchFirstRowQuery("SELECT rowid, * FROM Tiles WHERE productId = '%s'" % productId)
            if not folderName == None:
                qresult = self.fetchFirstRowQuery("SELECT rowid, * FROM Tiles WHERE folderName = '%s'" % folderName)            
        return qresult
        
    def addTile(self, platform, productId, beginposition, endposition, folderName = ""):
        newId = self.query("INSERT INTO Tiles (platform, folderName, productId, beginposition, endposition, \
            firstDownloadRequest) \
            VALUES (?, ?, ?, ?, ?, datetime('now', 'localtime'))", 
            (platform, folderName, productId, beginposition, endposition))
        logger.info("new tile inserted into database")
        return newId

    def getRequestedTiles(self):
        return self.fetchAllRowsQuery("SELECT rowid, * FROM Tiles WHERE downloadComplete IS NULL AND cancelled IS NULL ")
        
    def setUnzippedForTile(self, rowid):
        self.query("UPDATE Tiles SET unzipped = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (unzipped)")
     
    def setLastDownloadRequestForTile(self, rowid):
        self.query("UPDATE Tiles SET lastDownloadRequest = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (lastDownloadRequest)")
        
    def setDownloadCompleteForTile(self, rowid):
        self.query("UPDATE Tiles SET downloadComplete = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (downloadComplete)")

    def clearLastDownloadRequestForTile(self, rowid):
        self.query("UPDATE Tiles SET lastDownloadRequest = NULL WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (lastDownloadRequest cleared due to failed request)")

    def setCancelledTile(self, rowid):
        self.query("UPDATE Tiles SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (cancelled)")  

    def getLatestDownloadRequest(self):
        result = self.fetchFirstRowQuery("SELECT MAX(lastDownloadRequest) as latest FROM Tiles WHERE downloadComplete IS NULL")
        if result == None:
            return None
        else:
            return result["latest"]

    def updateTileProjection(self, rowid, projection):
        self.query("UPDATE Tiles SET projection = '%s' WHERE rowid = %d" % (projection, rowid))
        logger.info("projection updated for tileId " + str(rowid))

    def getTilesWithoutProjectionInfo(self):
        return self.fetchAllRowsQuery("SELECT rowid, * FROM Tiles WHERE projection IS NULL AND downloadComplete IS NOT NULL")


    ### POIS ###
    
    def getPoi(self, groupname, lat, lon, dateFrom, dateTo, platform, width, height, tileLimit = 0, **kwargs):

        query = "SELECT rowid, * FROM PointOfInterests WHERE groupname = '" + str(groupname)  + "' AND lat = " + str(lat) + " AND lon = " + str(lon) + " AND dateFrom = '" + dateFrom + "'" \
            + " AND dateTo = '" + dateTo + "' AND platform = '" + platform + "' AND width = " + str(width) + " AND height = " + str(height) \
            + " AND tileLimit = " + str(tileLimit)

        usedKeys = []

        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = "%s AND %s = '%s'" % (query, key, value)
                usedKeys.append(key)

        # check for unused keys
        # this is important to prevent fetching of different POIs with further arguments 
        for item in config.optionalSentinelParameters:
            if not ( item in usedKeys ):
                query = "%s AND %s IS NULL" % (query, item)

        qresult = self.fetchFirstRowQuery(query)
        return qresult
        
    def addPoi(self, groupname, lat, lon, dateFrom, dateTo, platform, width, height, tileLimit = 0, **kwargs):
        query = "INSERT INTO PointOfInterests (groupname, lat, lon"
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", " + key
        query = query + ", country, dateFrom, dateTo, platform, width, height, tileLimit, description, poicreated) "
        query = query + " VALUES ('" + str(groupname) + "'," + str(lat) + ", " + str(lon)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", '" + str(value) + "'"
        query = query + ", '" + self.getCountry(lat, lon) + "', '" + dateFrom + "', '" + dateTo + "', '" + platform + "', " + str(width) + ", " + str(height) \
            + ", " + str(tileLimit) + ", " + "'', datetime('now', 'localtime'))"
        poiId = self.query(query)

        logger.info("new PointOfInterest inserted into database")    
        return poiId
        
    def getCountry(self, lat, lon):
        cc = countries.CountryChecker(config.worldBordersShapeFile)
        country = cc.getCountry(countries.Point(lat, lon))
        if country == None:
            return "None"
        else:
            return country.iso
        
    def getPoiFromId(self, poiId):
        return self.fetchFirstRowQuery("SELECT rowid, * FROM PointOfInterests WHERE rowid = %d" % poiId)
        
    def setTilesIdentifiedForPoi(self, poiId):
        self.query("UPDATE PointOfInterests SET tilesIdentified = datetime('now', 'localtime') WHERE rowid = %d" % poiId)
        logger.info("PointOfInterest updated in database (tilesIdentified)")

    def setCancelledPoi(self, rowid):
        self.query("UPDATE PointOfInterests SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("PointOfInterest updated in database (cancelled)")        
        

    ### TILE-POI-CONNECTION ###
        
    def getTileForPoi(self, poiId, tileId):
        return self.fetchFirstRowQuery("SELECT Tiles.rowid, Tiles.*, TilesForPOIs.tileCropped FROM Tiles INNER JOIN TilesForPOIs ON Tiles.rowid = TilesForPOIs.tileId \
            WHERE TilesForPOIs.poiId = %d AND TilesForPOIs.tileId = %d" % (poiId, tileId))
        
    def getTilesForPoi(self, poiId):
        return self.fetchAllRowsQuery("SELECT Tiles.rowid, Tiles.*, TilesForPOIs.tileCropped FROM Tiles INNER JOIN TilesForPOIs ON Tiles.rowid = TilesForPOIs.tileId \
            WHERE TilesForPOIs.poiId = %d" % poiId)

    def getPoisForTile(self, tileId):
        return self.fetchAllRowsQuery("SELECT PointOfInterests.rowid, PointOfInterests.*, TilesForPOIs.tileCropped, TilesForPOIs.cancelled \
                                       FROM PointOfInterests INNER JOIN TilesForPOIs ON PointOfInterests.rowid = TilesForPOIs.poiId \
                                       WHERE TilesForPOIs.tileId = %d" % tileId)

    def getUncroppedPoisForDownloadedTiles(self):
        return self.fetchAllRowsQuery("SELECT PointOfInterests.rowid, PointOfInterests.*, TilesForPOIs.tileCropped, TilesForPOIs.cancelled \
                                       FROM PointOfInterests INNER JOIN TilesForPOIs ON PointOfInterests.rowid = TilesForPOIs.poiId \
                                       INNER JOIN Tiles ON TilesForPOIs.tileId = Tiles.rowid \
                                       WHERE Tiles.downloadComplete IS NOT NULL AND TilesForPOIs.tileCropped IS NULL AND TilesForPOIs.cancelled IS NULL")

    def getTilePoiConnectionId(self, poiId, tileId):
        data = self.fetchFirstRowQuery("SELECT rowid FROM TilesForPOIs WHERE poiId = %d AND tileId = %d" % (poiId, tileId))
        if data == None:
            return 0
        else:
            return data["rowid"]   
        
    def addTileForPoi(self, poiId, tileId):
        newId = self.query("INSERT INTO TilesForPOIs (poiId, tileId) VALUES ( %d, %d)" % (poiId, tileId))
        logger.info("new tile-poi connection inserted into database")
        return newId

    def setTileCropped(self, poiId, tileId, path):
        self.query("UPDATE TilesForPOIs SET tileCropped = datetime('now', 'localtime'), path = '%s' WHERE poiId = %d AND tileId = %d" % (path, poiId, tileId))
        logger.info("tile-poi updated in database (tileCropped)")

    def setCancelledTileForPoi(self, poiId, tileId):
        self.query("UPDATE TilesForPOIs SET cancelled = datetime('now', 'localtime') WHERE poiId = %d AND tileId = %d" % (poiId, tileId))
        logger.info("tile-poi updated in database (cancelled)")          
        

    ### CSV ###

    def importCsvRow(self, fileName, row):
        if not row == None:
            optionalFields = ["width", "height", "tileLimit", "description"]
            numFields = ["width", "height", "tileLimit"]
            keys = "fileName, groupname, lat, lon, dateFrom, dateTo, platform"
            values = "'%s', '%s', %s, %s, '%s', '%s', '%s'" % (fileName, row["groupname"], row["lat"], row["lon"], row["dateFrom"], row["dateTo"], row["platform"])
            for key, value in row.items():
                if key in config.optionalSentinelParameters or key in optionalFields:
                    if len(str(value)) > 0:
                        keys = "%s, %s" % (keys, key)
                        if key in numFields:
                            values = "%s, %s" % (values, value)
                        else:
                            values = "%s, '%s'" % (values, value)
            keys = keys + ", csvImported"
            values = values + ", datetime('now', 'localtime')"
            query = "INSERT INTO CSVInput (%s) VALUES (%s)" % (keys, values)
            csvImportRowId = self.query(query)
            return csvImportRowId

    def getImportedCSVdata(self):
        return self.fetchAllRowsQuery("SELECT rowid, * FROM CSVInput")

    def moveCSVItemToArchive(self, rowid):
        newId = self.query("INSERT INTO CSVLoaded SELECT *, datetime('now', 'localtime') as csvLoaded FROM CSVInput WHERE CSVInput.rowid = %d" % rowid)
        self.query("DELETE FROM CSVInput WHERE rowid = %d" % rowid)
        return newId

    def setCancelledImport(self, rowid):
        self.query("UPDATE CSVInput SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("import updated in database (cancelled)")
        self.moveCSVItemToArchive(rowid)
