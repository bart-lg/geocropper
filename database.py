import sqlite3
import config

class database:

    def __init__(self):

        self.connection = sqlite3.connect(config.dbFile)
        self.cursor = self.connection.cursor()

        # if no tables there, create new tables...

        query = "CREATE TABLE IF NOT EXISTS PointOfInterests \
            (country TEXT, lat REAL, lon REAL, dateFrom TEXT, dateTo TEXT, platform TEXT"
        for item in config.optionalSentinelParameters:
            query = query + ", " + item
        query = query + ", width INTEGER, height INTEGER, tileLimit INTEGER, description TEXT, tilesIdentified TEXT, poicreated TEXT)"

        self.cursor.execute(query)
        self.cursor.execute("CREATE TABLE IF NOT EXISTS Tiles \
            (platform TEXT, folderName TEXT, productId TEXT, firstDownloadRequest TEXT, lastDownloadRequest TEXT, downloadComplete TEXT, unzipped TEXT)")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS TilesForPOIs (poiId INTEGER, tileId INTEGER, tileCropped TEXT)")

        self.connection.commit()

    def query(self, query, values=None):
        if not values is None:
            self.cursor.execute(query, values)
        else:
            self.cursor.execute(query)
        self.connection.commit()
        return self.cursor.lastrowid

    def fetchQuery(self, query, values=None):
        if not values is None:
            self.cursor.execute(query, values)
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()