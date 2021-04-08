import sqlite3
import numpy
import geocropper.config as config

import time
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
        "orbitdirection":           "TEXT",
        "filename":                 "TEXT",
        "width":                    "INTEGER",
        "height":                   "INTEGER",
        "tileLimit":                "INTEGER",
        "tileStart":                "INTEGER",
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
        "cancelled":                "TEXT",
        "sceneClass0":              "REAL",
        "sceneClass1":              "REAL",
        "sceneClass2":              "REAL",
        "sceneClass3":              "REAL",
        "sceneClass4":              "REAL",
        "sceneClass5":              "REAL",
        "sceneClass6":              "REAL",
        "sceneClass7":              "REAL",
        "sceneClass8":              "REAL",
        "sceneClass9":              "REAL",
        "sceneClass10":             "REAL",
        "sceneClass11":             "REAL"        
    },

    # table CSVInput
    # holds imported records which have not yet been processed (loaded)
    "CSVInput": {
        "csvFileName":              "TEXT",
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
        "orbitdirection":           "TEXT",
        "filename":                 "TEXT",
        "width":                    "INTEGER",
        "height":                   "INTEGER",
        "tileLimit":                "INTEGER",
        "tileStart":                "INTEGER",
        "description":              "TEXT",
        "csvImported":              "TEXT",
        "cancelled":                "TEXT"
    },   

    # table CSVLoaded
    # holds imported and processed/loaded records
    "CSVLoaded": {
        "csvFileName":              "TEXT",
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
        "orbitdirection":           "TEXT",
        "filename":                 "TEXT",
        "width":                    "INTEGER",
        "height":                   "INTEGER",
        "tileLimit":                "INTEGER",
        "tileStart":                "INTEGER",
        "description":              "TEXT",
        "csvImported":              "TEXT",
        "cancelled":                "TEXT",
        "csvLoaded":                "TEXT"
    }  

}

# Number of Sentinel-2 scene classes
scene_classes = 12


class DatabaseLockedError(Exception):
    """Exception raised for errors while quering the database.
    """

    def __init__(self):
        self.message = f"Database locked or other error. Please see the log-file for details. \
            Timeout:{config.databaseTimeout} Retries:{config.databaseRetryQueries}"
        super().__init__(self.message)


### DB class

class Database:

    def __init__(self):

        self.open_connection()

        try:

            logger.debug("[database] DB: start creating new tables")

            # create new tables if not existing
            for table_name, table_content in tables.items():

                elements = ""
                for column_name, data_type in table_content.items():
                    if elements == "":
                        elements = "%s %s" % (column_name, data_type)
                    else:
                        elements = "%s, %s %s" % (elements, column_name, data_type)

                query = "CREATE TABLE IF NOT EXISTS " + table_name + " (" + elements + ")"
                logger.debug(f"[database] SQL query: {query}")

                self.cursor.execute(query)

            # save changes to database
            self.connection.commit()

            logger.info("[database] tables created if non existing")


            # check tables for missing columns (e.g. new columns in newer versions)
            logger.debug("[database] start checking for missing columns in DB tables")

            for table_name, table_content in tables.items():
                
                for column_name, data_type in table_content.items():
                
                    result = self.fetch_first_row_query(f"SELECT COUNT(*) AS num FROM \
                                                        pragma_table_info('{table_name}') \
                                                        WHERE name='{column_name}'")
                
                    if result == None or result["num"] == 0:

                        # column is missing and needs to be appended
                        self.query(f"ALTER TABLE {table_name} ADD {column_name} {data_type};")
                        logger.info(f"[database] db: column {column_name} added to table {table_name}")

            logger.info("[database] columns checked in DB tables")

        except Exception as e:

            print(str(e))
            logger.critical(f"[database] Error in initial queries: {repr(e)}")
            raise SystemExit              


    def __del__(self):

        self.close_connection()


    def open_connection(self):

        try:

            logger.debug("[database] start DB connection")

            # open or create sqlite database file
            self.connection = sqlite3.connect(config.dbFile, timeout=config.databaseTimeout)

            # provide index-based and case-insensitive name-based access to columns
            self.connection.row_factory = sqlite3.Row

            # create sqlite cursor object to execute SQL commands
            self.cursor = self.connection.cursor()

            logger.info("[database] DB connected")

        except Exception as e:

            print(str(e))
            logger.critical(f"Error in connecting DB: {repr(e)}")
            raise SystemExit      


    def close_connection(self):

        self.connection.close()


    ### QUERIES ###
        
    # query function used for inserts and updates
    def query(self, query, values=None):

        try:

            attempt = 0
            query_done = False

            while attempt < config.databaseRetryQueries and not query_done:

                try:

                    attempt = attempt + 1

                    logger.debug(f"[database] DB query: [{query}] [values: {values}]")
                    
                    if values == None:
                        self.cursor.execute(query)
                    else:
                        self.cursor.execute(query, values)
                    
                    # save changes
                    self.connection.commit()
                    new_id = self.cursor.lastrowid
                    
                    query_done = True

                    logger.debug(f"[database] DB query: new_id: {new_id}")

                except Exception as e:

                    logger.warning(f"[database] Could not query database. \
                        Attempt:{attempt} Error: {repr(e)}")
                    time.sleep(5)

            if not query_done:

                raise DatabaseLockedError()

        except Exception as e:

            print(str(e))
            logger.critical(f"Error in query [{query}] [values: {values}]: {repr(e)}") 
            raise SystemExit

        return new_id

    # query function used for selects returning all rows of result
    def fetch_all_rows_query(self, query, values=None):
        
        try:

            attempt = 0
            query_done = False

            while attempt < config.databaseRetryQueries and not query_done:

                try:

                    attempt = attempt + 1

                    logger.debug(f"[database] DB query: [{query}] [values: {values}]")

                    if values == None:
                        self.cursor.execute(query)
                    else:
                        self.cursor.execute(query, values)

                    result = self.cursor.fetchall()

                    query_done = True

                    logger.debug(f"[database] DB query: result: {result}")

                except Exception as e:

                    logger.warning(f"[database] Could not query database. \
                        Attempt:{attempt} Error: {repr(e)}")
                    time.sleep(5)   

            if not query_done:

                raise DatabaseLockedError()                                     

        except Exception as e:

            print(str(e))
            logger.error(f"Error in query [{query}] [values: {values}]: {repr(e)}") 
            raise SystemExit          

        return result

    # query function used for selects returning only first row of result
    def fetch_first_row_query(self, query, values=None):
        
        try:

            attempt = 0
            query_done = False

            while attempt < config.databaseRetryQueries and not query_done:

                try:

                    attempt = attempt + 1

                    logger.debug(f"[database] DB query: [{query}] [values: {values}]")

                    if values == None:
                        self.cursor.execute(query)
                    else:
                        self.cursor.execute(query, values)

                    result = self.cursor.fetchone()

                    query_done = True

                    logger.debug(f"[database] DB query: result: {result}")

                except Exception as e:

                    logger.warning(f"[database] Could not query database. \
                        Attempt:{attempt} Error: {repr(e)}")
                    time.sleep(5)    
                    
            if not query_done:

                raise DatabaseLockedError()                                       

        except Exception as e:

            print(str(e))
            logger.error(f"Error in query [{query}] [values: {values}]: {repr(e)}") 
            raise SystemExit  

        return result
        

    ### TILES ###

    def get_all_tiles(self):
        logger.debug(f"[database] get_all_tiles")
        result = self.fetch_all_rows_query("SELECT rowid, * FROM Tiles")
        logger.debug(f"[database] get_all_tiles: all tiles fetched.")
        return result   


    def get_required_tiles(self):
        logger.debug(f"[database] get_required_tiles")
        result = self.fetch_all_rows_query("SELECT Tiles.rowid, Tiles.* FROM Tiles \
            INNER JOIN TilesForPOIs ON Tiles.rowid = TilesForPOIs.tileId \
            WHERE TilesForPOIs.tileCropped IS NULL AND TilesForPOIs.cancelled IS NULL \
            GROUP BY Tiles.rowid")
        logger.debug(f"[database] get_required_tiles: all required tiles fetched.")
        return result         
        
        
    def get_tile(self, product_id = None, folder_name = None):

        logger.debug(f"[database] get tile for product_id: {product_id} folder_name: {folder_name}")

        if not product_id == None and not folder_name == None:
            qresult = self.fetch_first_row_query(f"SELECT rowid, * FROM Tiles WHERE \
                                                 productId = '{product_id}' \
                                                 AND folderName = '{folder_name}'")
        else:
            if not product_id == None:
                qresult = self.fetch_first_row_query(f"SELECT rowid, * FROM Tiles WHERE \
                                                     productId = '{product_id}'")
            if not folder_name == None:
                qresult = self.fetch_first_row_query(f"SELECT rowid, * FROM Tiles WHERE \
                                                     folderName = '{folder_name}'")            

        logger.debug(f"[database] get tile result: {qresult}")

        return qresult


    def get_tile_by_rowid(self, row_id):
        logger.debug(f"[database] get_tile_by_rowid: {row_id}")
        result = self.fetch_first_row_query(f"SELECT rowid, * FROM tiles WHERE rowid = {row_id}")
        logger.debug(f"[database] get_tile_by_rowid result: {repr(result)}")
        return result

        
    def add_tile(self, platform, product_id, beginposition, endposition, folder_name = ""):
        logger.debug(f"[database] add_tile: platform:{platform} product_id:{product_id} \
                     beginposition:{beginposition} endposition:{endposition}")
        newId = self.query("INSERT INTO Tiles (platform, folderName, productId, \
            beginposition, endposition, firstDownloadRequest) \
            VALUES (?, ?, ?, ?, ?, datetime('now', 'localtime'))", 
            (platform, folder_name, product_id, beginposition, endposition))
        logger.info(f"[database] new tile inserted into database: [{newId}] {platform} {product_id}")
        return newId

    def get_requested_tiles(self):
        logger.debug("[database] get_requested_tiles")
        result = self.fetch_all_rows_query("SELECT rowid, * FROM Tiles WHERE \
            downloadComplete IS NULL AND cancelled IS NULL ")
        logger.debug(f"[database] get_requested_tiles: {repr(result)}")
        return result
        
    def set_unpacked_for_tile(self, rowid):
        logger.debug(f"[database] set_unpacked_for_tile {rowid}")
        self.query("UPDATE Tiles SET unzipped = datetime('now', 'localtime') \
            WHERE rowid = %d" % rowid)
        logger.debug(f"[database] tile updated in database (unzipped): {rowid}")
     
    def set_last_download_request_for_tile(self, rowid):
        logger.debug(f"[database] set_last_download_request_for_tile {rowid}")
        self.query("UPDATE Tiles SET lastDownloadRequest = datetime('now', 'localtime') \
            WHERE rowid = %d" % rowid)
        logger.debug(f"[database] tile updated in database (lastDownloadRequest): {rowid}")
        
    def set_download_complete_for_tile(self, rowid):
        logger.debug(f"[database] set_download_complete_for_tile {rowid}")
        self.query("UPDATE Tiles SET downloadComplete = datetime('now', 'localtime') \
            WHERE rowid = %d" % rowid)
        logger.info(f"[database] tile updated in database (downloadComplete): {rowid}")

    def clear_download_complete_for_tile(self, rowid):
        logger.debug(f"[database] clear_download_complete_for_tile {rowid}")
        self.query("UPDATE Tiles SET downloadComplete = null WHERE rowid = %d" % rowid)
        logger.info(f"[database] tile updated in database (downloadComplete cleared): {rowid}")        

    def clear_last_download_request_for_tile(self, rowid):
        logger.debug(f"[database] clear_last_download_request_for_tile {rowid}")
        self.query("UPDATE Tiles SET lastDownloadRequest = NULL WHERE rowid = %d" % rowid)
        logger.info(f"[database] tile updated in database \
            (lastDownloadRequest cleared due to failed request): {rowid}")

    def clear_unpacked_for_tile(self, rowid):
        logger.debug(f"[database] clear_unpacked_for_tile {rowid}")
        self.query("UPDATE Tiles SET unzipped = NULL WHERE rowid = %d" % rowid)
        logger.debug(f"[database] tile updated in database (unzipped cleared): {rowid}")

    def set_cancelled_tile(self, rowid):
        logger.debug(f"[database] set_cancelled_tile {rowid}")
        self.query("UPDATE Tiles SET cancelled = datetime('now', 'localtime') \
            WHERE rowid = %d" % rowid)
        logger.info(f"[database] tile updated in database (cancelled): {rowid}")  

    def get_latest_download_request(self):
        logger.debug("[database] get_latest_download_request")
        result = self.fetch_first_row_query("SELECT MAX(lastDownloadRequest) as latest FROM Tiles \
            WHERE downloadComplete IS NULL")
        logger.debug(f"[database] latest download request: {result}")
        if result == None:
            return None
        else:
            return result["latest"]

    def update_tile_projection(self, rowid, projection):
        logger.debug(f"[database] update_tile_projection {rowid} {projection}")
        self.query("UPDATE Tiles SET projection = '%s' WHERE rowid = %d" % (projection, rowid))
        logger.debug(f"[database] projection updated for tile {rowid} [{projection}] ")

    def get_tiles_without_projection_info(self):
        logger.debug(f"[database] get_tiles_without_projection_info")
        result = self.fetch_all_rows_query("SELECT rowid, * FROM Tiles WHERE \
            projection IS NULL AND downloadComplete IS NOT NULL")
        logger.debug(f"[database] tiles without projection info: {result}")
        return result


    ### POIS ###
    
    def get_poi(self, groupname, lat, lon, date_from, date_to, platform, width, height, 
                description = "", tile_limit = 0, tile_start = 1, **kwargs):

        logger.debug(f"[database] get_poi {groupname}, {lat}, {lon}, {date_from}, {date_to}, \
                     {platform}, {width}, {height}, {description}, {tile_limit}, {tile_start}, {repr(kwargs)}")

        # TODO: if not checked yet, lat and lon are mandatory for any import, so it is not checked here, 
        #       because in this case we want an error to be thrown
        query = "SELECT rowid, * FROM PointOfInterests WHERE groupname = '" + str(groupname) \
            + "' AND lat = " + str(lat) + " AND lon = " + str(lon) + " AND dateFrom = '" + str(date_from) + "'" \
            + " AND dateTo = '" + date_to + "' AND platform = '" + platform + "' AND description = '" + str(description) + "'"

        if not (width == None) and isinstance(width, int):
            query = "%s AND width = %d" % (query, width)
        else:
            query = query + " AND width IS NULL"

        if not (height == None) and isinstance(height, int):
            query = "%s AND height = %d" % (query, height)
        else:
            query = query + " AND height IS NULL"

        if not (tile_limit == None) and isinstance(tile_limit, int):
            query = "%s AND tileLimit = %d" % (query, tile_limit)
        else:
            query = query + " AND tileLimit IS NULL"

        if not (tile_start == None) and tile_start > 1:
            query = query + " AND tileStart = " + str(tile_start)
        else:
            query = query + " AND tileStart IS NULL"

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

        logger.debug(f"[database] get_poi query: {query}")

        qresult = self.fetch_first_row_query(query)

        logger.debug(f"[database] get_poi result: {repr(qresult)}")

        return qresult
        
    def add_poi(self, groupname, lat, lon, date_from, date_to, platform, width, height, 
                description = "", tile_limit = 0, tile_start = 1, **kwargs):

        if tile_limit == None:
            tile_limit = 0
        if tile_start == None:
            tile_start = 1

        logger.debug(f"[database] add_poi {groupname}, {lat}, {lon}, {date_from}, {date_to}, \
                     {platform}, {width}, {height}, {description}, {tile_limit}, {tile_start}, {repr(kwargs)}")

        query = "INSERT INTO PointOfInterests (groupname, lat, lon"
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", " + key
        query = query + ", country, dateFrom, dateTo, platform, width, height, tileLimit, tileStart, description, poicreated) "
        query = query + " VALUES ('" + str(groupname) + "'," + str(lat) + ", " + str(lon)
        for key, value in kwargs.items():
            if key in config.optionalSentinelParameters:
                query = query + ", '" + str(value) + "'"
        query = query + ", '" + self.get_country(lat, lon) + "', '" + date_from + "', '" + date_to + "', '" \
            + platform + "', " + str(width) + ", " + str(height) \
            + ", " + str(tile_limit) + ", " + str(tile_start) + ", "
        if isinstance(description, type(None)):
            query = query + "''"
        else:
            query = query + "'" + str(description) + "'"
        query = query + ", datetime('now', 'localtime'))"

        logger.debug(f"[database] add_poi query: {query}")

        poi_id = self.query(query)

        logger.info(f"[database] new PointOfInterest inserted into database: {poi_id} [lat:{lat} lon:{lon}]")  

        return poi_id
        
    def get_country(self, lat, lon):
        logger.debug(f"[database] get_country lat:{lat} lon:{lon}")
        country = None
        try:
            cc = countries.CountryChecker(config.worldBordersShapeFile)
            country = cc.getCountry(countries.Point(lat, lon))
        except Exception as e:
            logger.error(f"Error in get_country: {repr(e)}")
        logger.debug(f"[database] country for lat:{lat} lon:{lon}: {country}")
        if country == None:
            return "None"
        else:
            return country.iso
        
    def get_poi_from_id(self, poi_id):
        logger.debug(f"[database] get_poi_from_id {poi_id}")
        result = self.fetch_first_row_query("SELECT rowid, * FROM PointOfInterests WHERE rowid = %d" % poi_id)
        logger.debug(f"[database] get_poi result: {repr(result)}")
        return result

    def get_pois_for_coordinates(self, lat, lon):
        logger.debug(f"[database] get_pois_for_coordinates lat:{lat} lon:{lon}")
        result = self.fetch_all_rows_query(f"SELECT rowid, * FROM PointOfInterests WHERE lat LIKE '{str(lat)}%' \
            AND lon LIKE '{str(lon)}%'")
        logger.debug(f"[database] get_pois_for_coordinates result rows: {len(result)}")
        return result 
        
    def set_tiles_identified_for_poi(self, poi_id):
        logger.debug(f"[database] set_tiles_identified_for_poi {poi_id}")
        self.query("UPDATE PointOfInterests SET tilesIdentified = datetime('now', 'localtime') WHERE rowid = %d" % poi_id)
        logger.info(f"[database] PointOfInterest updated in database (tilesIdentified): {poi_id}")

    def set_cancelled_poi(self, rowid):
        logger.debug(f"[database] set_cancelled_poi {poi_id}")
        self.query("UPDATE PointOfInterests SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("[database] PointOfInterest updated in database (cancelled)")        
        

    ### TILE-POI-CONNECTION ###
        
    def get_tile_for_poi(self, poi_id, tile_id):
        logger.debug(f"[database] get_tile_for_poi poi:{poi_id} tile:{tile_id}")
        result = self.fetch_first_row_query("SELECT Tiles.rowid, Tiles.*, TilesForPOIs.tileCropped FROM Tiles \
            INNER JOIN TilesForPOIs ON Tiles.rowid = TilesForPOIs.tileId \
            WHERE TilesForPOIs.poiId = %d AND TilesForPOIs.tileId = %d" % (poi_id, tile_id))
        logger.debug(f"[database] get_tile_for_poi result: {repr(result)}")
        return result
        
    def get_tiles_for_poi(self, poi_id):
        logger.debug(f"[database] get_tiles_for_poi poi:{poi_id}")
        result = self.fetch_all_rows_query("SELECT Tiles.rowid, Tiles.*, TilesForPOIs.tileCropped FROM Tiles \
            INNER JOIN TilesForPOIs ON Tiles.rowid = TilesForPOIs.tileId \
            WHERE TilesForPOIs.poiId = %d" % poi_id)
        logger.debug(f"[database] get_tiles_for_poi result: {repr(result)}")
        return result

    def get_pois_for_tile(self, tile_id):
        logger.debug(f"[database] get_pois_for_tile tile:{tile_id}")
        result = self.fetch_all_rows_query("SELECT PointOfInterests.rowid, PointOfInterests.*, TilesForPOIs.tileCropped, \
                                            TilesForPOIs.cancelled FROM PointOfInterests INNER JOIN TilesForPOIs \
                                            ON PointOfInterests.rowid = TilesForPOIs.poiId \
                                            WHERE TilesForPOIs.tileId = %d" % tile_id)
        logger.debug(f"[database] get_pois_for_tile result: {repr(result)}")
        return result

    def get_uncropped_pois_for_unpacked_tiles(self):
        logger.debug("[database] get_uncropped_pois_for_unpacked_tiles")
        result = self.fetch_all_rows_query("SELECT PointOfInterests.rowid, PointOfInterests.*, TilesForPOIs.tileCropped, \
                                            TilesForPOIs.cancelled FROM PointOfInterests INNER JOIN TilesForPOIs \
                                            ON PointOfInterests.rowid = TilesForPOIs.poiId \
                                            INNER JOIN Tiles ON TilesForPOIs.tileId = Tiles.rowid \
                                            WHERE Tiles.unzipped IS NOT NULL AND TilesForPOIs.tileCropped IS NULL \
                                            AND TilesForPOIs.cancelled IS NULL")
        logger.debug(f"[database] get_uncropped_pois_for_unpacked_tiles result: {repr(result)}")
        return result        

    def get_tile_poi_connection_id(self, poi_id, tile_id):
        logger.debug(f"[database] get_tile_poi_connection_id poi:{poi_id} tile:{tile_id}")
        data = self.fetch_first_row_query("SELECT rowid FROM TilesForPOIs WHERE poiId = %d AND tileId = %d" % (poi_id, tile_id))
        logger.debug(f"[database] get_tile_poi_connection_id id:{data['rowid']} dataset:{repr(data)}")
        if data == None:
            return 0
        else:
            return data["rowid"]  

    def get_tile_poi_connection(self, connection_id):
        logger.debug(f"[database] get_tile_poi_connection {connection_id}")
        result = self.fetch_first_row_query("SELECT rowid, * FROM TilesForPOIs WHERE rowid = %d" % connection_id)
        logger.debug(f"[database] get_tile_poi_connection result: {repr(result)}")
        return result         

    def get_tile_poi_connections(self):
        logger.debug(f"[database] get_tile_poi_connections")
        result = self.fetch_all_rows_query("SELECT rowid, * FROM TilesForPOIs")
        logger.debug(f"[database] get_tile_poi_connections result rows: {len(result)}")
        return result         
        
    def add_tile_for_poi(self, poi_id, tile_id):
        logger.debug(f"[database] add_tile_for_poi poi:{poi_id} tile:{tile_id}")
        newId = self.query("INSERT INTO TilesForPOIs (poiId, tileId) VALUES ( %d, %d)" % (poi_id, tile_id))
        logger.info(f"[database] new tile-poi connection inserted into database poi:{poi_id} tile:{tile_id}")
        return newId

    def set_tile_cropped(self, poi_id, tile_id, path):
        logger.debug(f"[database] set_tile_cropped poi:{poi_id}, tile:{tile_id}, path:{path}")
        self.query("UPDATE TilesForPOIs SET tileCropped = datetime('now', 'localtime'), path = '%s' WHERE poiId = %d \
                    AND tileId = %d" % (path, poi_id, tile_id))
        logger.info("[database] tile-poi updated in database (tileCropped): poiId:%d tileId:%d" % (poi_id, tile_id))

    def set_cancelled_tile_for_poi(self, poi_id, tile_id):
        logger.debug(f"[database] set_cancelled_tile_for_poi poi:{poi_id}, tile:{tile_id}")
        self.query("UPDATE TilesForPOIs SET cancelled = datetime('now', 'localtime') WHERE poiId = %d AND tileId = %d" % (poi_id, tile_id))
        logger.info("[database] tile-poi updated in database (cancelled): poiId:%d tileId:%d" % (poi_id, tile_id))          

    def set_cancelled_tiles_for_pois(self):
        logger.debug(f"[database] set_cancelled_tiles_for_pois")
        self.query("UPDATE TilesForPOIs SET cancelled = datetime('now', 'localtime')")
        logger.info("[database] tile-poi: cancelled all crops")        

    def reset_cancelled_tile_for_poi(self, poi_id, tile_id):
        logger.debug(f"[database] reset_cancelled_tile_for_poi")
        self.query("UPDATE TilesForPOIs SET cancelled = NULL WHERE poiId = %d AND tileId = %d" % (poi_id, tile_id))
        logger.info("[database] tile-poi: cancelled crop reseted")

    def reset_cancelled_tiles_for_pois(self):
        logger.debug(f"[database] reset_cancelled_tiles_for_pois")
        self.query("UPDATE TilesForPOIs SET cancelled = NULL WHERE cancelled IS NOT NULL")
        logger.info("[database] tile-poi: cancelled crops reseted")

    def set_scence_class_ratios_for_crop(self, connection_id, ratios):
        logger.debug(f"[database] set_scence_class_ratios_for_crop connection_id:{connection_id}, ratios:{ratios}")
        if isinstance(ratios, dict) and len(ratios) > 0:
            query = "UPDATE TilesForPOIs "
            first = True
            for key in ratios:
                if int(key) >= scene_classes:
                    logger.warning(f"[database] Higher scene class provided than expected! max:{scene_classes-1} provided:{key}")
                    logger.warning(f"[database] Scene class information could not be stored! connection_id:{connection_id}")
                else:
                    if first:
                        query = query + "SET "
                        first = False
                    else:
                        query = query + ", "
                    # numpy.format_float_positional returns a float without scientific notation
                    query = query + f"sceneClass{key}={numpy.format_float_positional(ratios[key])} "
            query = query + f"WHERE rowid = {connection_id}"
            self.query(query)
        logger.info(f"[database] tile-poi updated in database (scene ratios): connection_id:{connection_id}, ratios:{ratios}")
        

    ### CSV ###

    def import_csv_row(self, file_name, row):
        logger.debug(f"[database] import_csv_row {file_name} {row}")
        if not row == None:
            optional_fields = ["width", "height", "tileLimit", "tileStart", "description"]
            num_fields = ["width", "height", "tileLimit", "tileStart"]
            keys = "csvFileName, groupname, lat, lon, dateFrom, dateTo, platform"
            values = "'%s', '%s', %s, %s, '%s', '%s', '%s'" % (file_name, row["groupname"], 
                row["lat"], row["lon"], row["dateFrom"], row["dateTo"], row["platform"])
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
            logger.debug(f"[database] import_csv_row query: {query}")
            csv_import_row_id = self.query(query)
            logger.info(f"[database] csv row imported file:{file_name} row:{row} db row_id:{csv_import_row_id}")
            return csv_import_row_id

    def get_imported_csv_data(self):
        logger.debug("[database] get_imported_csv_data")
        result = self.fetch_all_rows_query("SELECT rowid, * FROM CSVInput")
        logger.debug(f"[database] get_imported_csv_data result: {repr(result)}")
        return result

    def move_csv_item_to_archive(self, rowid):
        logger.debug(f"[database] move_csv_item_to_archive rowid:{rowid}")
        new_id = self.query("INSERT INTO CSVLoaded SELECT *, datetime('now', 'localtime') \
                             as csvLoaded FROM CSVInput WHERE CSVInput.rowid = %d" % rowid)
        self.query("DELETE FROM CSVInput WHERE rowid = %d" % rowid)
        logger.debug(f"[database] move_csv_item_to_archive dataset moved [new_id:{new_id}]")
        return new_id

    def set_cancelled_import(self, rowid):
        logger.debug(f"[database] set_cancelled_import {rowid}")
        self.query("UPDATE CSVInput SET cancelled = datetime('now', 'localtime') WHERE rowid = %d" % rowid)
        logger.info("[database] import updated in database (cancelled)")
        self.move_csv_item_to_archive(rowid)
