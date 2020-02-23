import sqlite3
import config
from countries import countries

import logging

logger = logging.getLogger('root')

# NEVER USE DELETE!
# SQLITE REUSES IDs!
# THIS COULD BE BAD FOR RELATIONS!

class database:

    def __init__(self):

        logger.info("start DB connection")

        self.connection = sqlite3.connect(config.dbFile)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

        logger.info("DB connected")

        # if no tables there, create new tables...

        query = "CREATE TABLE IF NOT EXISTS PointOfInterests \
            (country TEXT, lat REAL, lon REAL, dateFrom TEXT, dateTo TEXT, platform TEXT"
        for item in config.optionalSentinelParameters:
            query = "%s, %s" % (query, item)
        query = query + ", width INTEGER, height INTEGER, tileLimit INTEGER, description TEXT, tilesIdentified TEXT, poicreated TEXT)"

        self.cursor.execute(query)
        self.cursor.execute("CREATE TABLE IF NOT EXISTS Tiles \
            (platform TEXT, folderName TEXT, productId TEXT, firstDownloadRequest TEXT, lastDownloadRequest TEXT, downloadComplete TEXT, unzipped TEXT)")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS TilesForPOIs (poiId INTEGER, tileId INTEGER, tileCropped TEXT)")

        self.connection.commit()

        logger.info("tables created if non existing")

    ### QUERIES ###
        
    def query(self, query, values=None):
        if values == None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, values)
        self.connection.commit()
        logger.info("DB query commited: [%s] [values: %s]" % (query, values))
        return self.cursor.lastrowid

    def fetchAllRowsQuery(self, query, values=None):
        if values == None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, values)
        return self.cursor.fetchall()

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
        
    ### POIS ###
    
    def getPoi(self, lat, lon, fromDate, toDate, platform, width, height, tileLimit = 0, **kwargs):

        query = "SELECT rowid, * FROM PointOfInterests WHERE lat = " + str(lat) + " AND lon = " + str(lon) + " AND dateFrom = '" + fromDate + "'" \
            + " AND dateTo = '" + toDate + "' AND platform = '" + platform + "' AND width = " + str(width) + " AND height = " + str(height) \
            + " AND tileLimit = " + str(tileLimit)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = "%s AND %s = '%s'" % (query, key, value)

        qresult = self.fetchFirstRowQuery(query)
        return qresult
        
    def addPoi(self, lat, lon, fromDate, toDate, platform, width, height, tileLimit = 0, **kwargs):
        query = "INSERT INTO PointOfInterests (lat, lon"
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", " + key
        query = query + ", country, dateFrom, dateTo, platform, width, height, tileLimit, description, poicreated) "
        query = query + " VALUES (" + str(lat) + ", " + str(lon)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", '" + str(value) + "'"
        query = query + ", '" + self.getCountry(lat, lon) + "', '" + fromDate + "', '" + toDate + "', '" + platform + "', " + str(width) + ", " + str(height) \
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
        
     
