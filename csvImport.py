import csv
import shutil
import os
import geocropper
import database
import config

import logging

# NEEDED COLUMNS:
# lat, lon, dateFrom, dateTo, platform

# OPTIONAL COLUMNS:
# width, height (both mandatory for cropping)
# polarisationmode, producttype, sensoroperationalmode, swathidentifier, cloudcoverpercentage, timeliness, tileLimit, description

# Note: If no width and height is specified the tiles are not going to be cropped (download only)

logger = logging.getLogger('root')
db = database.database()

def importAllCSVs(delimiter=',', quotechar='"'):
    for item in os.listdir(config.csvInputDir):
        if item.endswith(".csv"):
            filePath = "%s/%s" % (config.csvInputDir, item)
            importCSV(filePath = filePath, delimiter = delimiter, quotechar = quotechar, autoLoad = False)
    loadImportedCSVdata()

def importCSV(filePath, delimiter=',', quotechar='"', autoLoad = True):
    
    fileName = os.path.basename(filePath)

    with open(filePath, newline='') as csvfile:
        content = csv.DictReader(csvfile, delimiter = delimiter, quotechar = quotechar)
        counter = 0
        for row in content:
            db.importCsvRow(fileName, row)
            counter += 1
        logger.info("CSV import: " + filePath)
        logger.info("%d rows imported into database." % counter)
    csvfile.close()
    
    if os.path.exists("%s/%s" % (config.csvArchiveDir, fileName)):
        i = 2
        filePrefix = os.path.splitext(fileName)[0]
        while os.path.exists("%s/%s(%s).csv" % (config.csvArchiveDir, filePrefix, i)) and i < 1000:
            i += 1
        fileName = "%s(%s).csv" % (filePrefix, i)

    if not os.path.isdir(config.csvArchiveDir):
        os.makedirs(config.csvArchiveDir)

    newPath = "%s/%s" % (config.csvArchiveDir, fileName)
    if os.path.exists(newPath):
        # TODO: ERROR OR WARNING MESSAGE!
        os.remove(filePath)
    else:
        shutil.move(filePath, newPath)

    if autoLoad:
        loadImportedCSVdata()

def loadImportedCSVdata():
    data = db.getImportedCSVdata()
    for item in data:
        if len(item) > 0:
            geoc = geocropper.init(item["lat"], item["lon"])
            kwargs = {}
            for key in item.keys():
                if key in config.optionalSentinelParameters and item[key] != None:
                    kwargs[key] = item[key]
            geoc.downloadAndCrop(fromDate = item["dateFrom"], toDate = item["dateTo"], platform = item["platform"], \
                width = item["width"], height = item["height"], tileLimit = item["tileLimit"], **kwargs)
            del geoc
            db.moveCSVItemToArchive(item["rowid"])
