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

class Database:

    def __init__(self):

        self.open_connection()


        # create new tables if not existing
        for table_name, table_content in tables.items():

            elements = ""
            for column_name, data_type in table_content.items():
                if elements == "":
                    elements = "%s %s" % (column_name, data_type)
                else:
                    elements = "%s, %s %s" % (elements, column_name, data_type)

            query = "CREATE TABLE IF NOT EXISTS " + table_name + " (" + elements + ")"

            self.cursor.execute(query)

        # save changes to database
        self.connection.commit()

        logger.info("tables created if non existing")


        # check tables for missing columns (e.g. new columns in newer versions)
        for table_name, table_content in tables.items():
            
            for column_name, data_type in table_content.items():
            
                result = self.fetch_first_row_query(f"SELECT COUNT(*) AS num FROM pragma_table_info('{table_name}') \
                                                   WHERE name='{column_name}'")
            
                if result == None or result["num"] == 0:

                    # column is missing and needs to be appended
                    self.query(f"ALTER TABLE {table_name} ADD {column_name} {data_type};")
                    logger.info(f"db: column {column_name} added to table {table_name}")


    def __del__(self):

        self.close_connection()


    def open_connection(self):

        logger.info("start DB connection")

        # open or create sqlite database file
        self.connection = sqlite3.connect(config.dbFile)

        # provide index-based and case-insensitive name-based access to columns
        self.connection.row_factory = sqlite3.Row

        # create sqlite cursor object to execute SQL commands
        self.cursor = self.connection.cursor()

        logger.info("DB connected")


    def close_connection(self):

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
        new_id = self.cursor.lastrowid
        return new_id

    # query function used for selects returning all rows of result
    def fetch_all_rows_query(self, query, values=None):
        if values == None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, values)
        return self.cursor.fetchall()

    # query function used for selects returning only first row of result
    def fetch_first_row_query(self, query, values=None):
        if values == None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, values)
        return self.cursor.fetchone()
        

    ### TILES ###
        
    def get_tile(self, product_id = None, folder_name = None):
        if not product_id == None and not folder_name == None:
            qresult = self.fetch_first_row_query("SELECT rowid, * FROM Tiles WHERE productId = '?' AND folderName = '?'", \
                (product_id, folder_name))
        else:
            if not product_id == None:
                qresult = self.fetch_first_row_query("SELECT rowid, * FROM Tiles WHERE productId = '%s'" % product_id)
            if not folder_name == None:
                qresult = self.fetch_first_row_query("SELECT rowid, * FROM Tiles WHERE folderName = '%s'" % folder_name)            
        return qresult

    def get_tile_by_rowid(self, row_id):
        return self.fetch_first_row_query("SELECT rowid, * FROM tiles WHERE rowid = %d" % rowid)
        
    def add_tile(self, platform, product_id, beginposition, endposition, folder_name = ""):
        newId = self.query("INSERT INTO Tiles (platform, folderName, productId, beginposition, endposition, \
            firstDownloadRequest) \
            VALUES (?, ?, ?, ?, ?, datetime('now', 'localtime'))", 
            (platform, folder_name, product_id, beginposition, endposition))
        logger.info("new tile inserted into database")
        return newId

    def get_requested_tiles(self):
        return self.fetch_all_rows_query("SELECT rowid, * FROM Tiles WHERE downloadComplete IS NULL AND cancelled IS NULL ")
        
    def set_unzipped_for_tile(self, rowid):
        self.query("UPDATE Tiles SET unzipped = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (unzipped)")
     
    def set_last_download_request_for_tile(self, rowid):
        self.query("UPDATE Tiles SET lastDownloadRequest = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (lastDownloadRequest)")
        
    def set_download_complete_for_tile(self, rowid):
        self.query("UPDATE Tiles SET downloadComplete = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (downloadComplete)")

    def clear_download_complete_for_tile(self, rowid):
        self.query("UPDATE Tiles SET downloadComplete = null WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (downloadComplete cleared)")        

    def clear_last_download_request_for_tile(self, rowid):
        self.query("UPDATE Tiles SET lastDownloadRequest = NULL WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (lastDownloadRequest cleared due to failed request)")

    def set_cancelled_tile(self, rowid):
        self.query("UPDATE Tiles SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("tile updated in database (cancelled)")  

    def get_latest_download_request(self):
        result = self.fetch_first_row_query("SELECT MAX(lastDownloadRequest) as latest FROM Tiles WHERE downloadComplete IS NULL")
        if result == None:
            return None
        else:
            return result["latest"]

    def update_tile_projection(self, rowid, projection):
        self.query("UPDATE Tiles SET projection = '%s' WHERE rowid = %d" % (projection, rowid))
        logger.info("projection updated for tileId " + str(rowid))

    def get_tiles_without_projection_info(self):
        return self.fetch_all_rows_query("SELECT rowid, * FROM Tiles WHERE projection IS NULL AND downloadComplete IS NOT NULL")


    ### POIS ###
    
    def get_poi(self, groupname, lat, lon, date_from, date_to, platform, width, height, tile_limit = 0, **kwargs):

        query = "SELECT rowid, * FROM PointOfInterests WHERE groupname = '" + str(groupname)  + "' AND lat = " + str(lat) + " AND lon = " + str(lon) + " AND dateFrom = '" + date_from + "'" \
            + " AND dateTo = '" + date_to + "' AND platform = '" + platform + "' AND width = " + str(width) + " AND height = " + str(height) \
            + " AND tileLimit = " + str(tile_limit)

        used_keys = []

        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = "%s AND %s = '%s'" % (query, key, value)
                used_keys.append(key)

        # check for unused keys
        # this is important to prevent fetching of different POIs with further arguments 
        for item in config.optionalSentinelParameters:
            if not ( item in used_keys ):
                query = "%s AND %s IS NULL" % (query, item)

        qresult = self.fetch_first_row_query(query)
        return qresult
        
    def add_poi(self, groupname, lat, lon, date_from, date_to, platform, width, height, tile_limit = 0, **kwargs):
        query = "INSERT INTO PointOfInterests (groupname, lat, lon"
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", " + key
        query = query + ", country, dateFrom, dateTo, platform, width, height, tileLimit, description, poicreated) "
        query = query + " VALUES ('" + str(groupname) + "'," + str(lat) + ", " + str(lon)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", '" + str(value) + "'"
        query = query + ", '" + self.get_country(lat, lon) + "', '" + date_from + "', '" + date_to + "', '" + platform + "', " + str(width) + ", " + str(height) \
            + ", " + str(tile_limit) + ", " + "'', datetime('now', 'localtime'))"
        poi_id = self.query(query)

        logger.info("new PointOfInterest inserted into database")    
        return poi_id
        
    def get_country(self, lat, lon):
        cc = countries.CountryChecker(config.worldBordersShapeFile)
        country = cc.getCountry(countries.Point(lat, lon))
        if country == None:
            return "None"
        else:
            return country.iso
        
    def get_poi_from_id(self, poi_id):
        return self.fetch_first_row_query("SELECT rowid, * FROM PointOfInterests WHERE rowid = %d" % poi_id)
        
    def set_tiles_identified_for_poi(self, poi_id):
        self.query("UPDATE PointOfInterests SET tilesIdentified = datetime('now', 'localtime') WHERE rowid = %d" % poi_id)
        logger.info("PointOfInterest updated in database (tilesIdentified)")

    def set_cancelled_poi(self, rowid):
        self.query("UPDATE PointOfInterests SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("PointOfInterest updated in database (cancelled)")        
        

    ### TILE-POI-CONNECTION ###
        
    def get_tile_for_poi(self, poi_id, tile_id):
        return self.fetch_first_row_query("SELECT Tiles.rowid, Tiles.*, TilesForPOIs.tileCropped FROM Tiles INNER JOIN TilesForPOIs ON Tiles.rowid = TilesForPOIs.tileId \
            WHERE TilesForPOIs.poiId = %d AND TilesForPOIs.tileId = %d" % (poi_id, tile_id))
        
    def get_tiles_for_poi(self, poi_id):
        return self.fetch_all_rows_query("SELECT Tiles.rowid, Tiles.*, TilesForPOIs.tileCropped FROM Tiles INNER JOIN TilesForPOIs ON Tiles.rowid = TilesForPOIs.tileId \
            WHERE TilesForPOIs.poiId = %d" % poi_id)

    def get_pois_for_tile(self, tile_id):
        return self.fetch_all_rows_query("SELECT PointOfInterests.rowid, PointOfInterests.*, TilesForPOIs.tileCropped, TilesForPOIs.cancelled \
                                       FROM PointOfInterests INNER JOIN TilesForPOIs ON PointOfInterests.rowid = TilesForPOIs.poiId \
                                       WHERE TilesForPOIs.tileId = %d" % tile_id)

    def get_uncropped_pois_for_downloaded_tiles(self):
        return self.fetch_all_rows_query("SELECT PointOfInterests.rowid, PointOfInterests.*, TilesForPOIs.tileCropped, TilesForPOIs.cancelled \
                                       FROM PointOfInterests INNER JOIN TilesForPOIs ON PointOfInterests.rowid = TilesForPOIs.poiId \
                                       INNER JOIN Tiles ON TilesForPOIs.tileId = Tiles.rowid \
                                       WHERE Tiles.downloadComplete IS NOT NULL AND TilesForPOIs.tileCropped IS NULL AND TilesForPOIs.cancelled IS NULL")

    def get_tile_poi_connection_id(self, poi_id, tile_id):
        data = self.fetch_first_row_query("SELECT rowid FROM TilesForPOIs WHERE poiId = %d AND tileId = %d" % (poi_id, tile_id))
        if data == None:
            return 0
        else:
            return data["rowid"]   
        
    def add_tile_for_poi(self, poi_id, tile_id):
        newId = self.query("INSERT INTO TilesForPOIs (poiId, tileId) VALUES ( %d, %d)" % (poi_id, tile_id))
        logger.info("new tile-poi connection inserted into database")
        return newId

    def set_tile_cropped(self, poi_id, tile_id, path):
        self.query("UPDATE TilesForPOIs SET tileCropped = datetime('now', 'localtime'), path = '%s' WHERE poiId = %d AND tileId = %d" % (path, poi_id, tile_id))
        logger.info("tile-poi updated in database (tileCropped): poiId:%d tileId:%d" % (poi_id, tile_id))

    def set_cancelled_tile_for_poi(self, poi_id, tile_id):
        self.query("UPDATE TilesForPOIs SET cancelled = datetime('now', 'localtime') WHERE poiId = %d AND tileId = %d" % (poi_id, tile_id))
        logger.info("tile-poi updated in database (cancelled): poiId:%d tileId:%d" % (poi_id, tile_id))          
        

    ### CSV ###

    def import_csv_row(self, file_name, row):
        if not row == None:
            optional_fields = ["width", "height", "tileLimit", "description"]
            num_fields = ["width", "height", "tileLimit"]
            keys = "fileName, groupname, lat, lon, dateFrom, dateTo, platform"
            values = "'%s', '%s', %s, %s, '%s', '%s', '%s'" % (file_name, row["groupname"], row["lat"], row["lon"], row["dateFrom"], row["dateTo"], row["platform"])
            for key, value in row.items():
                if key in config.optionalSentinelParameters or key in optional_fields:
                    if len(str(value)) > 0:
                        keys = "%s, %s" % (keys, key)
                        if key in num_fields:
                            values = "%s, %s" % (values, value)
                        else:
                            values = "%s, '%s'" % (values, value)
            keys = keys + ", csvImported"
            values = values + ", datetime('now', 'localtime')"
            query = "INSERT INTO CSVInput (%s) VALUES (%s)" % (keys, values)
            csv_import_row_id = self.query(query)
            return csv_import_row_id

    def get_imported_csv_data(self):
        return self.fetch_all_rows_query("SELECT rowid, * FROM CSVInput")

    def move_csv_item_to_archive(self, rowid):
        new_id = self.query("INSERT INTO CSVLoaded SELECT *, datetime('now', 'localtime') as csvLoaded FROM CSVInput WHERE CSVInput.rowid = %d" % rowid)
        self.query("DELETE FROM CSVInput WHERE rowid = %d" % rowid)
        return new_id

    def set_cancelled_import(self, rowid):
        self.query("UPDATE CSVInput SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("import updated in database (cancelled)")
        self.move_csv_item_to_archive(rowid)
