import sqlite3
import config

from countries import countries

import logging

# NEVER USE DELETE IN TABLES WITH RELATIONS!
# SQLITE REUSES IDs!
# THIS COULD BE BAD FOR RELATIONS!


# get logger object
logger = logging.getLogger('root')

class database:

    def __init__(self):

        logger.info("start DB connection")

        # open or create sqlite database file
        self.connection = sqlite3.connect(config.dbFile)

        # provide index-based and case-insensitive name-based access to columns
        self.connection.row_factory = sqlite3.Row

        # create sqlite cursor object to execute SQL commands
        self.cursor = self.connection.cursor()

        logger.info("DB connected")


        # create new tables if not existing

        # table PointOfInterest
        # holds the information for a geocropper call - one record for every point/parameter combination
        query = "CREATE TABLE IF NOT EXISTS PointOfInterests \
            (country TEXT, lat REAL, lon REAL, dateFrom TEXT, dateTo TEXT, platform TEXT"
        for item in config.optionalSentinelParameters:
            query = "%s, %s" % (query, item)
        query = query + ", width INTEGER, height INTEGER, tileLimit INTEGER, description TEXT, tilesIdentified TEXT, poicreated TEXT, cancelled TEXT)"
        self.cursor.execute(query)

        # table Tiles
        # information about downloaded big tiles
        self.cursor.execute("CREATE TABLE IF NOT EXISTS Tiles \
            (platform TEXT, folderName TEXT, productId TEXT, firstDownloadRequest TEXT, lastDownloadRequest TEXT, \
            downloadComplete TEXT, unzipped TEXT, cancelled TEXT)")

        # table TilesForPOIs
        # n:m relation between tables PointOfInterest and Tiles
        # additional information: date of image cropping based on parameters of POI
        self.cursor.execute("CREATE TABLE IF NOT EXISTS TilesForPOIs (poiId INTEGER, tileId INTEGER, tileCropped TEXT, cancelled TEXT)")

        # table CSVInput
        # holds imported records which have not yet been processed (loaded)
        query = "CREATE TABLE IF NOT EXISTS CSVInput \
            (fileName TEXT, lat REAL, lon REAL, dateFrom TEXT, dateTo TEXT, platform TEXT"
        for item in config.optionalSentinelParameters:
            query = "%s, %s" % (query, item)
        query = query + ", width INTEGER, height INTEGER, tileLimit INTEGER, description TEXT, csvImported TEXT, cancelled TEXT)"
        self.cursor.execute(query)

        # table CSVLoaded
        # holds imported and processed/loaded records
        query = "CREATE TABLE IF NOT EXISTS CSVLoaded \
            (fileName TEXT, lat REAL, lon REAL, dateFrom TEXT, dateTo TEXT, platform TEXT"
        for item in config.optionalSentinelParameters:
            query = "%s, %s" % (query, item)
        query = query + ", width INTEGER, height INTEGER, tileLimit INTEGER, description TEXT, csvImported TEXT, cancelled TEXT, csvLoaded TEXT)"
        self.cursor.execute(query)

        # save changes to database
        self.connection.commit()

        logger.info("tables created if non existing")


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
        return self.cursor.lastrowid

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
        
    def addTile(self, platform, productId, folderName = ""):
        newId = self.query("INSERT INTO Tiles (platform, folderName, productId, firstDownloadRequest, lastDownloadRequest) \
            VALUES (?, ?, ?, datetime('now', 'localtime'), datetime('now', 'localtime'))", (platform, folderName, productId))
        logger.info("new tile inserted into database")
        return newId
        
    def setUnzippedForTile(self, rowid):
        self.query("UPDATE Tiles SET unzipped = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (unzipped)")
     
    def setDownloadRequestForTile(self, rowid):
        self.query("UPDATE Tiles SET lastDownloadRequest = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (lastDownloadRequest)")
        
    def setDownloadCompleteForTile(self, rowid):
        self.query("UPDATE Tiles SET downloadComplete = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (downloadComplete)")

    def setCancelledTile(self, rowid):
        self.query("UPDATE Tiles SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (cancelled)")     
        

    ### POIS ###
    
    def getPoi(self, lat, lon, dateFrom, dateTo, platform, width, height, tileLimit = 0, **kwargs):

        query = "SELECT rowid, * FROM PointOfInterests WHERE lat = " + str(lat) + " AND lon = " + str(lon) + " AND dateFrom = '" + dateFrom + "'" \
            + " AND dateTo = '" + dateTo + "' AND platform = '" + platform + "' AND width = " + str(width) + " AND height = " + str(height) \
            + " AND tileLimit = " + str(tileLimit)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = "%s AND %s = '%s'" % (query, key, value)

        qresult = self.fetchFirstRowQuery(query)
        return qresult
        
    def addPoi(self, lat, lon, dateFrom, dateTo, platform, width, height, tileLimit = 0, **kwargs):
        query = "INSERT INTO PointOfInterests (lat, lon"
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", " + key
        query = query + ", country, dateFrom, dateTo, platform, width, height, tileLimit, description, poicreated) "
        query = query + " VALUES (" + str(lat) + ", " + str(lon)
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
        return cc.getCountry(countries.Point(lat, lon)).iso    
        
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
        
    def addTileForPoi(self, poiId, tileId):
        newId = self.query("INSERT INTO TilesForPOIs (poiId, tileId) VALUES ( %d, %d)" % (poiId, tileId))
        logger.info("new tile-poi connection inserted into database")
        return newId

    def setTileCropped(self, poiId, tileId):
        self.query("UPDATE TilesForPOIs SET tileCropped = datetime('now', 'localtime') WHERE poiId = %d AND tileId = %d" % (poiId, tileId))
        logger.info("tile-poi updated in database (tileCropped)")

    def setCancelledTileForPoi(self, poiId, tileId):
        self.query("UPDATE TilesForPOIs SET cancelled = datetime('now', 'localtime') WHERE poiId = %d AND tileId = %d" % (poiId, tileId))
        logger.info("tile-poi updated in database (cancelled)")          
        

    ### CSV ###

    def importCsvRow(self, fileName, row):
        if not row == None:
            optionalFields = ["width", "height", "tileLimit", "description"]
            numFields = ["width", "height", "tileLimit"]
            keys = "fileName, lat, lon, dateFrom, dateTo, platform"
            values = "'%s', %s, %s, '%s', '%s', '%s'" % (fileName, row["lat"], row["lon"], row["dateFrom"], row["dateTo"], row["platform"])
            for key, value in row.items():
                if key in config.optionalSentinelParameters or key in optionalFields:
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
