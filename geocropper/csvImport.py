import csv
import shutil
import os
import pathlib

import logging

import geocropper.geocropper as geocropper
import geocropper.database as database
import geocropper.config as config
import geocropper.utils as utils


# NEEDED COLUMNS:
# groupname, lat, lon, dateFrom, dateTo, platform

# OPTIONAL COLUMNS:
# width, height (both mandatory for cropping)
# polarisationmode, producttype, sensoroperationalmode, swathidentifier, cloudcoverpercentage, timeliness, tileLimit, description

# Note: If no width and height is specified the tiles are not going to be cropped (download only)

# get logger object
logger = logging.getLogger('root')

# open database
db = database.Database()


# import all csv files in import directory
def import_all_csvs(delimiter=',', quotechar='"', auto_load=True):

    # go through all files in import directory
    for item in os.listdir(config.csvInputDir):

        # if file is csv file then import content to database
        if item.endswith(".csv"):
            file_path = config.csvInputDir / item
            importcsv(file_path = file_path, delimiter = delimiter, quotechar = quotechar, auto_load = False)
    
    # load imported csv data: call geocropper for individual records
    if auto_load:
        load_imported_csv_data()


# import specific csv file
def importcsv(file_path, delimiter=',', quotechar='"', auto_load = True):
    
    # cut filename out of path
    file_name = os.path.basename(file_path)


    # open csv file
    with open(file_path, newline='', encoding = 'utf-8-sig') as csvfile:

        # read content in dictionary
        content = csv.DictReader(csvfile, delimiter = delimiter, quotechar = quotechar)

        # import rows to database
        counter = 0
        for row in content:
            db.import_csv_row(file_name, row)
            counter += 1

        logger.info("CSV import: " + str(file_path))
        logger.info("%d rows imported into database." % counter)

    csvfile.close()
    

    # check for unique csv file_name in csv archive directory
    if pathlib.Path(config.csvArchiveDir / file_name).is_file():
        
        # if file exists try other variations

        i = 2

        # get file_name without file extension
        file_prefix = os.path.splitext(file_name)[0]

        # loop through possible variations
        while pathlib.Path(config.csvArchiveDir / ("%s(%s).csv" % (file_prefix, i)) ).is_file() and i < 1000:
            i += 1

        # set new file_name 
        file_name = "%s(%s).csv" % (file_prefix, i)


    # make sure that archive directory exists
    if not os.path.isdir(config.csvArchiveDir):
        os.makedirs(config.csvArchiveDir)


    # set new path/file_name
    new_path = config.csvArchiveDir / file_name

    # move csv file to archive
    if os.path.exists(new_path):
        # TODO: ERROR OR WARNING MESSAGE!
        os.remove(file_path)
    else:
        shutil.move(file_path, new_path)


    # load imported csv data: call geocropper for individual records
    if auto_load:
        load_imported_csv_data()


# load imported csv data: call geocropper for individual records
def load_imported_csv_data(lower_boundary=None, upper_boundary=None, auto_crop=True):

    # get imported and not yet loaded data
    data = db.get_imported_csv_data()

    if upper_boundary > 0 and len(data) > upper_boundary:
        data = data[0:upper_boundary]

    if lower_boundary > 0:
        if len(data) > lower_boundary:
            data = data[lower_boundary:]
        else:
            print("Lower boundary higher than number of elements left")
            exit()

    # index i serves as a counter
    i = 0

    for item in data:

        i += 1

        # TODO: is it really necessary to check the length of items? it should always be more than 0...
        if len(item) > 0:

            print("\n############################################################")
            print("\n[ Load imported data... %d/%d ]" % (i, len(data)))
            logger.info("[ ##### Load imported data... %d/%d ##### ]" % (i, len(data)))
            if lower_boundary != None or upper_boundary != None:
                print(f"\n[ Boundaries: {lower_boundary}:{upper_boundary} ]")
                logger.info(f"\n[ Boundaries: {lower_boundary}:{upper_boundary} ]")

            # create arguments out of imported content
            kwargs = {}
            for key in item.keys():
                if key in config.optionalSentinelParameters and item[key] != None:
                    kwargs[key] = item[key]

            # download and crop with geocropper module
            geocropper.download_and_crop(item["lat"], item["lon"], groupname = item["groupname"], date_from = item["dateFrom"], date_to = item["dateTo"], platform = item["platform"], \
                width = item["width"], height = item["height"], tile_limit = item["tileLimit"], auto_crop=auto_crop, **kwargs)


            # move database record to archive table
            db.move_csv_item_to_archive(item["rowid"])

    

    # Turned off, because it creates combined preview images of all cropped tiles folder
    # TODO: combinedPreview only for new or changed cropped tiles folder

    # if config.combinedPreview:
    #     print("#### Create combined preview images...")
    #     utils.combineImages()
    #     print("done.\n")
    

    logger.info("[ ##### Load imported data... %d/%d ...done! ##### ]" % (i, len(data)))
    if lower_boundary != None or upper_boundary != None:
        logger.info(f"\n[ Boundaries: {lower_boundary}:{upper_boundary} ]")    
