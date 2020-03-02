import csv
import shutil
import os
import pathlib

import logging

import geocropper.geocropper as geocropper
import geocropper.database as database
import geocropper.config as config


# NEEDED COLUMNS:
# lat, lon, dateFrom, dateTo, platform

# OPTIONAL COLUMNS:
# width, height (both mandatory for cropping)
# polarisationmode, producttype, sensoroperationalmode, swathidentifier, cloudcoverpercentage, timeliness, tileLimit, description

# Note: If no width and height is specified the tiles are not going to be cropped (download only)

# get logger object
logger = logging.getLogger('root')

# open database
db = database.database()


# import all csv files in import directory
def importAllCSVs(delimiter=',', quotechar='"'):

    # go through all files in import directory
    for item in os.listdir(config.csvInputDir):

        # if file is csv file then import content to database
        if item.endswith(".csv"):
            filePath = config.csvInputDir / item
            importCSV(filePath = filePath, delimiter = delimiter, quotechar = quotechar, autoLoad = False)
    
    # load imported csv data: call geocropper for individual records
    loadImportedCSVdata()


# import specific csv file
def importCSV(filePath, delimiter=',', quotechar='"', autoLoad = True):
    
    # cut filename out of path
    fileName = os.path.basename(filePath)


    # open csv file
    with open(filePath, newline='') as csvfile:

        # read content in dictionary
        content = csv.DictReader(csvfile, delimiter = delimiter, quotechar = quotechar)

        # import rows to database
        counter = 0
        for row in content:
            db.importCsvRow(fileName, row)
            counter += 1

        logger.info("CSV import: " + str(filePath))
        logger.info("%d rows imported into database." % counter)

    csvfile.close()
    

    # check for unique csv filename in csv archive directory
    if pathlib.Path(config.csvArchiveDir / fileName).is_file():
        
        # if file exists try other variations

        i = 2

        # get filename without file extension
        filePrefix = os.path.splitext(fileName)[0]

        # loop through possible variations
        while pathlib.Path(config.csvArchiveDir / ("%s(%s).csv" % (filePrefix, i)) ).is_file() and i < 1000:
            i += 1

        # set new filename 
        fileName = "%s(%s).csv" % (filePrefix, i)


    # make sure that archive directory exists
    if not os.path.isdir(config.csvArchiveDir):
        os.makedirs(config.csvArchiveDir)


    # set new path/filename
    newPath = config.csvArchiveDir / fileName

    # move csv file to archive
    if os.path.exists(newPath):
        # TODO: ERROR OR WARNING MESSAGE!
        os.remove(filePath)
    else:
        shutil.move(filePath, newPath)


    # load imported csv data: call geocropper for individual records
    if autoLoad:
        loadImportedCSVdata()


# load imported csv data: call geocropper for individual records
def loadImportedCSVdata():

    # get imported and not yet loaded data
    data = db.getImportedCSVdata()

    # index i serves as a counter
    i = 1

    for item in data:

        # TODO: is it really necessary to check the length of items? it should always be more than 0...
        if len(item) > 0:

            print("\n############################################################")
            print("\n[ Load imported data... %d/%d ]" % (i, len(data)))

            # initialize geocropper instance
            geoc = geocropper.init(item["lat"], item["lon"])

            # create arguments out of imported content
            kwargs = {}
            for key in item.keys():
                if key in config.optionalSentinelParameters and item[key] != None:
                    kwargs[key] = item[key]

            # download and crop with geocropper module
            geoc.downloadAndCrop(dateFrom = item["dateFrom"], dateTo = item["dateTo"], platform = item["platform"], \
                width = item["width"], height = item["height"], tileLimit = item["tileLimit"], **kwargs)

            # cleanup
            del geoc

            # move database record to archive table
            db.moveCSVItemToArchive(item["rowid"])
            
        i += 1
