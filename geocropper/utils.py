import numpy
from PIL import Image, ImageDraw, ImageFont
import math
import matplotlib.pyplot as pyplot
import os
import stat
import pathlib
import rasterio
import shutil
import pyproj
from pyproj import Geod
import pandas
import random
from shapely.geometry import Point
from shapely.geometry import Polygon
from shapely.geometry import shape
from shapely.ops import transform as shapely_transform
from rtree import index
import fiona
from dateutil.parser import *
from functools import partial
import zipfile
import tarfile
from tqdm import tqdm
import subprocess
import sys
from datetime import datetime
from distutils.dir_util import copy_tree
from skimage import transform
from sklearn import preprocessing
try:
    import otbApplication
except ImportError:
    pass
from tifffile import imsave


import geocropper.config as config
import geocropper.download as download
from geocropper.database import Database
import geocropper.sentinelWrapper as sentinelWrapper
import geocropper.asfWrapper as asfWrapper

import logging

from osgeo import gdal
from osgeo import gdal_array

# gdal library distributed by conda destroys PATH environment variable
# see -> https://github.com/OSGeo/gdal/issues/1231
# workaround: remove first entry...
if ";" in os.environ["PATH"]:
    os.environ["PATH"] = os.environ["PATH"].split(';')[1]

# get logger object
logger = logging.getLogger('root')
db = Database()


def convert_date(date, new_format="%Y-%m-%d"):
    """Converts a date object to a string of a given format. Default is %Y-%m-%d.
    """
    temp = parse(date)
    return temp.strftime(new_format)


def date_older_than_24h(date):
    then = datetime.fromisoformat(date)
    now = datetime.now()
    duration = now - then 
    duration_in_s = duration.total_seconds()
    hours = divmod(duration_in_s, 3600)[0]
    if hours >= 24:
        return True
    else:
        return False


def minutes_since_last_download_request():
    then = db.get_latest_download_request()
    if then != None:
        return minutes_since_timestamp(then)
    else:
        return None  


def minutes_since_timestamp(timestamp):
    now = datetime.now()
    then = datetime.fromisoformat(str(timestamp))
    duration = now - then
    duration_in_s = duration.total_seconds()
    minutes = divmod(duration_in_s, 60)[0]
    return int(minutes)


def get_xy_corner_coordinates(path, lat, lon, width, height):

    poi_transformed = transform_latlon_to_xy(path, Point(lon, lat))
    poi_x = poi_transformed.x
    poi_y = poi_transformed.y

    # open image with GDAL
    dataset = gdal.Open(str(path))
    
    upper_left_x, xres, xskew, upper_left_y, yskew, yres = dataset.GetGeoTransform()
    cols = dataset.RasterXSize
    rows = dataset.RasterYSize

    top_left_x = poi_x - (width/2) - (height/2) / yres * xskew
    top_left_y = poi_y + (height/2) - (width/2) / xres * yskew
    bottom_right_x = poi_x + (width/2) + (height/2) / yres * xskew
    bottom_right_y = poi_y - (height/2) + (width/2) / xres * yskew

    return({ "top_left": Point(top_left_x, top_left_y), "bottom_right": Point(bottom_right_x, bottom_right_y)})    


def transform_latlon_to_xy(path, point):

    # TODO: this function should not require a path, it should transform from WGS84 to UTM

    # open raster image file
    img = rasterio.open(str(path))

    # prepare parameters for coordinate system transform function
    to_target_crs = partial(pyproj.transform,
        pyproj.Proj('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs '), pyproj.Proj(img.crs))

    # transform corner coordinates for cropping
    point = shapely_transform(to_target_crs, point)

    return(point)


def transform_xy_to_latlon(path, point):

    # TODO: this function should not require a path, it should transform from UTM to WGS84

    img = rasterio.open(str(path))
    in_proj = pyproj.Proj(img.crs)
    lat, lon = in_proj(point.x, point.y, inverse=True)
    return({"lat": lat, "lon": lon})


def crop_image(path, item, top_left, bottom_right, target_dir, file_format, is_latlon = True):

    if (is_latlon):
        top_left = transform_latlon_to_xy(path, top_left)
        bottom_right = transform_latlon_to_xy(path, bottom_right)

    # open image with GDAL
    ds = gdal.Open(str(path))

    # make sure that target directory exists
    if not os.path.isdir(str(target_dir)):
        os.makedirs(str(target_dir))

    # CROP IMAGE
    ds = gdal.Translate(str(target_dir / item), ds, format=file_format,
                        projWin=[top_left.x, top_left.y,
                                 bottom_right.x, bottom_right.y])

    ds = None


def create_preview_images(source_dir, combine_preview_images=True, sentinel_type="S2", min_scale=-30, max_scale=30, exponential_scale=0.5):
    """Creates preview images for Sentinel-1 or Sentinel-2 crops. 
    Bands must be in crop root directory or in the subdirectory sensordata or in the subdirectory sensordata/R10m"""

    source_dir = pathlib.Path(source_dir)

    if sentinel_type == "S2":

        r_band_search_pattern = "*B04_10m.jp2"
        g_band_search_pattern = "*B03_10m.jp2"
        b_band_search_pattern = "*B02_10m.jp2"

        for crop_dir in tqdm(source_dir.glob("*"), desc="Creating preview images for crops"):

            if not crop_dir.name.startswith("0_"):

                if ( crop_dir / "sensordata" / "R10m" ).exists():
                    input_dir = crop_dir / "sensordata" / "R10m"
                else:
                    input_dir = crop_dir

                create_preview_rgb_image(r_band_search_pattern, g_band_search_pattern, b_band_search_pattern, input_dir, crop_dir)

    elif sentinel_type == "S1":

        for crop_dir in tqdm(source_dir.glob("*"), desc="Creating preview images for crops"):

            if not crop_dir.name.startswith("0_"):

                if ( crop_dir / "sensordata" / "s1_cropped.tif" ).exists():

                    create_preview_rg_image(( crop_dir / "sensordata" / "s1_cropped.tif" ), crop_dir, 
                        min_scale=min_scale, max_scale=max_scale, exponential_scale=exponential_scale)

                else:

                    try:
                        
                        vv_image = list(crop_dir.glob("*VV.tif"))[0]
                        vh_image = list(crop_dir.glob("*VH.tif"))[0]
                        
                        if vv_image.exists() and vh_image.exists():

                            create_preview_rg_image(vv_image, crop_dir, min_scale=min_scale, max_scale=max_scale, 
                                exponential_scale=exponential_scale, file2=vh_image)

                    except:
                        pass


    if combine_preview_images:
        print("Create combined preview images...")
        create_combined_images(source_dir)

    print("Done.")


def create_preview_rgb_image(r_band_search_pattern, g_band_search_pattern, b_band_search_pattern, source_dir,
                          target_dir, max_scale=4095, exponential_scale=0.5):

    search_result = list(source_dir.glob(r_band_search_pattern))
    if len(search_result) == 0:
        return False
    r_band = search_result[0]

    search_result = list(source_dir.glob(g_band_search_pattern))
    if len(search_result) == 0:
        return False
    g_band = search_result[0]

    search_result = list(source_dir.glob(b_band_search_pattern))
    if len(search_result) == 0:
        return False
    b_band = search_result[0]

    preview_file = "preview.tif"
    preview_file_small = "preview_small.tif"
    if (target_dir / preview_file).exists():
        i = 2
        preview_file = "preview(" + str(i) + ").tif"
        while i < 100 and (target_dir / preview_file).exists():
            i = i + 1
            preview_file = "preview(" + str(i) + ").tif"
        # TODO: throw exception if i > 99
        preview_file_small = "preview_small(" + str(i) + ").tif"

    logger.info("Create RGB preview image.")

    # rescale red band
    command = ["gdal_translate", "-q", "-ot", "Byte", "-scale", "0", str(max_scale), "0", "255", "-exponent",
               str(exponential_scale), os.path.realpath(str(r_band)), os.path.realpath(str(target_dir / "r-scaled.tif"))]
    subprocess.call(command)

    # rescale green band
    command = ["gdal_translate", "-q", "-ot", "Byte", "-scale", "0", str(max_scale), "0", "255", "-exponent",
               str(exponential_scale), os.path.realpath(str(g_band)), os.path.realpath(str(target_dir / "g-scaled.tif"))]
    subprocess.call(command)

    # rescale blue band
    command = ["gdal_translate", "-q", "-ot", "Byte", "-scale", "0", str(max_scale), "0", "255", "-exponent",
               str(exponential_scale), os.path.realpath(str(b_band)), os.path.realpath(str(target_dir / "b-scaled.tif"))]
    subprocess.call(command)

    # create preview image
    command = ["gdal_merge.py", "-ot", "Byte", "-separate", "-of", "GTiff", "-co", "PHOTOMETRIC=RGB",
               "-o", os.path.realpath(str(target_dir / preview_file)),
               os.path.realpath(str(target_dir / "r-scaled.tif")),
               os.path.realpath(str(target_dir / "g-scaled.tif")),
               os.path.realpath(str(target_dir / "b-scaled.tif"))]
    subprocess.call(command)

    # remove scaled bands
    try:
        (target_dir / "r-scaled.tif").unlink()
        (target_dir / "g-scaled.tif").unlink()
        (target_dir / "b-scaled.tif").unlink()
    except:
        logger.error("Error while removing temporary images.")
        logger.error(sys.exc_info()[0])        

    if config.resizePreviewImage:
        image = Image.open(str(target_dir / preview_file))
        small_image = image.resize((config.widthPreviewImageSmall, config.heightPreviewImageSmall), Image.ANTIALIAS)
        small_image.save(str(target_dir / preview_file_small))

    return True


def create_preview_rg_image(file1, target_dir, min_scale=-30, max_scale=30, exponential_scale=0.5, file2=None):
    """Creates a RGB preview file out of an db scaled image with only 2 bands or two separate db scaled images.
    """

    preview_file = "preview.tif"
    preview_file_small = "preview_small.tif"
    if (target_dir / preview_file).exists():
        i = 2
        preview_file = "preview(" + str(i) + ").tif"
        while i < 100 and (target_dir / preview_file).exists():
            i = i + 1
            preview_file = "preview(" + str(i) + ").tif"
        # TODO: throw exception if i > 99
        preview_file_small = "preview_small(" + str(i) + ").tif"

    logger.info("Create RGb preview image.")

    if exponential_scale == None:
        exp_option = ""
    else:
        exp_option = f"-exponent {exponential_scale}"

    # rescale from db scale min:-30 max:30 to min:0 max:255
    # since the min scale for the source is negative we need to provide subprocess.call with a whole string and turn the argument shell to True
    # docs subprocess: "If passing a single string, either shell must be True [...]"
    command = f"gdal_translate -b 1 -q -ot Byte -scale {min_scale} {max_scale} 0 255 " + \
              f"{str(exp_option)} {os.path.realpath(str(file1))} {os.path.realpath(str(target_dir / 'r-scaled.tif'))}"        
    subprocess.call(command, shell=True)    

    if not isinstance(file2, type(None)):
        command = f"gdal_translate -b 1 -q -ot Byte -scale {min_scale} {max_scale} 0 255 " + \
                  f"{str(exp_option)} {os.path.realpath(str(file2))} {os.path.realpath(str(target_dir / 'g-scaled.tif'))}"        
        subprocess.call(command, shell=True)    
    else:
        command = f"gdal_translate -b 2 -q -ot Byte -scale {min_scale} {max_scale} 0 255 " + \
                  f"{str(exp_option)} {os.path.realpath(str(file1))} {os.path.realpath(str(target_dir / 'g-scaled.tif'))}"            
        subprocess.call(command, shell=True)    

    # create empty blue band
    command = ["gdal_calc.py", "--quiet", "-A", os.path.realpath(str(target_dir / "r-scaled.tif")), 
               f"--outfile={os.path.realpath(str(target_dir)) }/b-empty.tif", "--calc=0" ]
    subprocess.call(command)               

    # create preview image
    command = ["gdal_merge.py", "-ot", "Byte", "-separate", "-of", "GTiff", "-co", "PHOTOMETRIC=RGB",
               "-o", os.path.realpath(str(target_dir / preview_file)),
               os.path.realpath(str(target_dir / "r-scaled.tif")),
               os.path.realpath(str(target_dir / "g-scaled.tif")),
               os.path.realpath(str(target_dir / "b-empty.tif"))]
    subprocess.call(command)

    # remove scaled bands
    try:
        (target_dir / "r-scaled.tif").unlink()
        (target_dir / "g-scaled.tif").unlink()
        (target_dir / "b-empty.tif").unlink()
    except:
        logger.error("Error while removing temporary images.")
        logger.error(sys.exc_info()[0])    

    if config.resizePreviewImage:
        image = Image.open(str(target_dir / preview_file))
        small_image = image.resize((config.widthPreviewImageSmall, config.heightPreviewImageSmall), Image.ANTIALIAS)
        small_image.save(str(target_dir / preview_file_small))      


def rows_cols_for_ratio(items, ratio):
    """Calculates optimum number of rows and columns for items for a given ratio.
    """
    cols = 0
    rows = 0
    while cols * rows < items:
        rows = rows + 1
        cols = math.ceil(rows * ratio)
    return cols, rows


def concat_images(image_path_list, output_file, gap=3, bcolor=(0, 0, 0), paths_to_file=None,
                  first_label_list=None, second_label_list=None, third_label_list=None,
                  write_image_text=True, center_point=False, image_height=None, image_width=None):
    """Combine images to one image

    Parameters
    ----------
    image_path_list : list
        paths of the images to combine
    output_file : str
        path of the output (combined image file)
    gap : int, optional
        number of pixels between the images (border)
        default is 3
    bcolor : tuple, optional
        tuple with rgb values for background and border
        default is (0,0,0) -> black
    paths_to_file : str, optional
        if defined a list of paths will be saved to the given path
    first_label_list : list, optional
        if defined the contained labels will be written on the image
        first label will be written in first line
        instead of the paths of the individual images
    second_label_list : list, optional
        if defined the contained labels will be written on the image 
        second label will be written in second line
    third_label_list : list, optional
        if defined the contained labels will be written on the image 
        third label will be written in third line        
    write_image_text : boolean, optional
        write paths or labels on image
        default is True
    center_point : boolean, optional
        marks the center of the individual preview images with a red dot
        default is False
    image_height : int, optional
        trimmed height of preview images
    image_width : int, optional
        trimmed width of preview images

    """

    # determine needed raster size
    if config.previewFormat == "1:1":
        raster_size_x = math.ceil(math.sqrt(len(image_path_list)))
        raster_size_y = raster_size_x
    else:
        preview_format = config.previewFormat.split(":")
        raster_size_x, raster_size_y = rows_cols_for_ratio(len(image_path_list), \
            int(preview_format[0]) / int(preview_format[1]))

    # determine max heigth and max width of all images
    max_height = 0
    max_width = 0

    for image_path in image_path_list:
        try:
            image = pyplot.imread(image_path)[:, :, :3]
            height, width = image.shape[:2]
            if height > max_height:
                max_height = height
            if width > max_width:
                max_width = width
        except:
            logger.error(f"Could not read image: {image_path}")
            logger.error(sys.exc_info()[0])

    # set max image height and width to trimmed size
    if image_height != None and image_height < max_height:
        max_height = image_height
    if image_width != None and image_width < max_width:
        max_width = image_width

    # add gap to width and height
    total_height = max_height * config.previewEnlargeFactor * raster_size_y + gap * (raster_size_y - 1)
    total_width = max_width * config.previewEnlargeFactor * raster_size_x + gap * (raster_size_x - 1)

    # assign positions to images
    # positions = [row, column, height_start, width_start]
    positions = numpy.zeros((len(list(image_path_list)), 4), dtype=int)
    for i, image_path in enumerate(image_path_list, 1):
        # determine position
        row = math.ceil(i / raster_size_x)
        column = i % raster_size_x
        if column == 0:
            column = raster_size_x

        # determine starting width and height
        height_start = (row - 1) * (max_height * config.previewEnlargeFactor + gap)
        width_start = (column - 1) * (max_width * config.previewEnlargeFactor + gap)

        positions[i-1][0] = int(row)
        positions[i-1][1] = int(column)
        positions[i-1][2] = int(height_start)
        positions[i-1][3] = int(width_start)

    # create empty image
    combined_image = numpy.full((total_height, total_width, 3), bcolor)

    # paste images to combined image
    for i, image_path in enumerate(image_path_list):

        try:

            # read image
            image = pyplot.imread(image_path)[:, :, :3]

            # determine width and height
            height, width = image.shape[:2]

            # trim image
            if height > max_height:
                image_height_start = math.floor(height / 2) - math.floor(max_height / 2)
                image_height_end = image_height_start + max_height
                height = max_height
            else:
                image_height_start = 0
                image_height_end = height

            if width > max_width:
                image_width_start = math.floor(width / 2) - math.floor(max_width / 2)
                image_width_end = image_width_start + max_width
                width = max_width
            else:
                image_width_start = 0
                image_width_end = width
            
            image = image[image_height_start:image_height_end, image_width_start:image_width_end] 
            height, width = image.shape[:2]

            # enlarge
            if config.previewEnlargeFactor > 1:
                image = transform.resize(image, (height*config.previewEnlargeFactor, width*config.previewEnlargeFactor), \
                    order=0, mode="constant", preserve_range=True)
                height, width = image.shape[:2]

            # paste image
            combined_image[positions[i][2]:(positions[i][2]+height), positions[i][3]:(positions[i][3]+width)] = image

            # center point
            if center_point:
                combined_image[positions[i][2] + round(height / 2), positions[i][3] + round(width / 2)] = (255, 0, 0)

        except:
            logger.error(f"Could not insert image {image_path} to combined preview image.")
            logger.error(sys.exc_info()[0])                

    # write file
    image = Image.fromarray(numpy.uint8(combined_image))

    # write paths on image
    if write_image_text:
        font = ImageFont.truetype(str(
            pathlib.Path(os.environ["CONDA_PREFIX"]) / "fonts" / "open-fonts" / "IBMPlexMono-Regular.otf"), config.previewImageFontSize)
        draw = ImageDraw.Draw(image)
        if first_label_list == None:
            first_label_list = image_path_list
        for i, first_label in enumerate(first_label_list):
            # draw.text requires coordinates in following order: width, height
            draw.text((positions[i][3] + 5, positions[i][2] + config.previewTopMarginFirstLabel), first_label, font=font, fill=(255, 0, 0))
        if second_label_list != None:
            for i, second_label in enumerate(second_label_list):
                # draw.text requires coordinates in following order: width, height
                draw.text((positions[i][3] + 5, positions[i][2] + config.previewTopMarginSecondLabel), second_label, font=font, fill=(255, 0, 0))
        if third_label_list != None:
            for i, third_label in enumerate(third_label_list):
                # draw.text requires coordinates in following order: width, height
                draw.text((positions[i][3] + 5, positions[i][2] + config.previewTopMarginThirdLabel), third_label, font=font, fill=(255, 0, 0))                

    image.save(output_file)

    # create file list
    if paths_to_file != None:
        file = open(paths_to_file, "w+")
        for i, image_path in enumerate(image_path_list, 1):
            position = i % raster_size_x
            if position != 1:
                file.write("\t")
            file.write(str(image_path))
            if position == 0:
                file.write("\r\n")


def create_combined_images(source_folder, image_height=None, image_width=None):

    if not config.combinedPreview:
        logger.warning("Combined preview started, but disabled in config!")
        return

    counter = 0
    image_path_list = []
    first_label_list = []
    second_label_list = []
    third_label_list = []

    combined_preview_folder = source_folder / "0_combined-preview"
    combined_preview_folder.mkdir(exist_ok=True)

    item_list = list(source_folder.glob("*"))

    item_list_sorted = []
    for item in item_list:
        num = item.name.split("_", 1)[0]
        if num.isdigit():
            item_list_sorted.append(int(num))
    
    item_list_sorted.sort()

    for i, item in tqdm(enumerate(item_list_sorted), desc="Images processed: "):

        if item > 0:

            in_path = list(source_folder.glob(f"{str(item)}_*"))[0]

            preview_file = in_path / "preview.tif"

            if preview_file.exists():

                image_path_list.append(preview_file)
                
                first_label_list.append(str(item))

                if config.previewShowDescription == True:
                    description = ""
                    try:
                        crop = db.get_tile_poi_connection(item)
                        poi = db.get_poi_from_id(crop["poiId"])
                        description = poi["description"]
                    except:
                        pass
                    second_label_list.append("D:" + str(description))

                if config.previewShowTileDate == True:
                    tile_date = ""
                    try:
                        crop = db.get_tile_poi_connection(item)
                        tile = db.get_tile_by_rowid(crop["tileId"])
                        tile_date = convert_date(tile["beginposition"], "%d.%m.%y")
                    except:
                        pass                        
                    third_label_list.append(tile_date)


            if (i > 0 and i % config.previewImagesCombined == 0) or (i+1) == len(item_list):

                counter = counter + 1

                output_file = combined_preview_folder / ("combined-preview-" + str(counter) + ".tif")
                summary_file = combined_preview_folder / ("combined-preview-" + str(counter) + "-paths.txt")

                concat_images(image_path_list, output_file, gap=config.previewBorder,
                              bcolor=config.previewBackground, paths_to_file=summary_file,
                              first_label_list=first_label_list, second_label_list=second_label_list, third_label_list=third_label_list, 
                              write_image_text=config.previewTextOnImage, center_point=config.previewCenterDot, 
                              image_height=image_height, image_width=image_width)

                image_path_list = []
                first_label_list = []
                second_label_list = []
                third_label_list = []


def combine_images(folder="", outside_cropped_tiles_dir=False, has_subdir=True, image_height=None, image_width=None):

    # TODO: create a new function that determines exact path of source dir based on the first three given arguments (redundancy in other functions)
    if outside_cropped_tiles_dir:
        source_dir = pathlib.Path(folder)
        if has_subdir:
            for request in source_dir.glob("*"):
                create_combined_images(request, image_height, image_width)
        else:
            create_combined_images(source_dir, image_height, image_width)        
    else:
        for group in config.croppedTilesDir.glob("*"):

            if len(folder) == 0 or (len(folder) > 0 and folder == str(group.name)):

                if has_subdir:
                    for request in group.glob("*"):
                        create_combined_images(request, image_height, image_width)
                else:
                    create_combined_images(group, image_height, image_width)


def create_random_crops(crops_per_tile=30, output_folder="random_crops", width=1000, height=1000):
    """Creates random crops out of existing Sentinel 2 level 2 big tiles.
    """
    max_loops_per_tile = 1000
    # outProj = pyproj.Proj(init='epsg:3857')

    output_path = config.croppedTilesDir / (output_folder + "_w" + str(width) + "_h" + str(height))
    if output_path.exists():
        i = 1
        output_path = config.croppedTilesDir / \
            (output_folder + "_w" + str(width) + "_h" + str(height) + "_" + str(i).zfill(2))
        while output_path.exists() and i < 100:
            i = i + 1
            output_path = config.croppedTilesDir / \
                (output_folder + "_w" + str(width) + "_h" + str(height) + "_" + str(i).zfill(2))

    if output_path.exists():
        print("Could not create a unique output folder!")
        return

    os.mkdir(output_path)
    j = 0

    print("Generating random crops...")

    for i, big_tile in enumerate(config.bigTilesDir.glob("S2*"), 1):

        print(f"### TILE {i}: {big_tile}")

        # Sentinel-2 img data are in jp2-format
        # set appropriate format for GDAL lib
        file_format = "JP2OpenJPEG"

        # go through "SAFE"-directory structure of Sentinel-2

        path_granule = big_tile / "GRANULE"
        for main_folder in os.listdir(path_granule):

            path_image_data = path_granule / main_folder / "IMG_DATA"

            hq_dir = path_image_data / "R10m"
            # Level-1 currently not supported
            # hq_dir only exists on Level-2 data
            if hq_dir.exists():

                # open image with GDAL
                file = list(hq_dir.glob("*_B02_10m.jp2"))[0]
                dataset = gdal.Open(str(file))

                # yres is negative!
                upper_left_x, xres, xskew, upper_left_y, yskew, yres = dataset.GetGeoTransform()
                cols = dataset.RasterXSize
                rows = dataset.RasterYSize
                lower_left_x = upper_left_x + (rows * xskew)
                lower_left_y = upper_left_y + (rows * yres)
                upper_right_x = upper_left_x + (cols * xres)
                upper_right_y = upper_left_y + (cols * yskew)
                lower_right_x = upper_left_x + (rows * xskew) + (cols * xres)
                lower_right_y = upper_left_y + (cols * yskew) + (rows * yres)

                random_crops = 0
                loop_counter = 0

                img = rasterio.open(str(file))

                while random_crops < crops_per_tile and loop_counter < max_loops_per_tile:

                    loop_counter = loop_counter + 1

                    # reduce the tile area where the random point may be generated randomly 
                    # by half of the crop height and width
                    reduced_upper_left_x = upper_left_x + (width/2) + (height/2) / yres * xskew
                    reduced_upper_left_y = upper_left_y - (height/2) + (width/2) / xres * yskew
                    reduced_lower_left_x = lower_left_x + (width/2) - (height/2) / yres * xskew
                    reduced_lower_left_y = lower_left_y + (height/2) + (width/2) / xres * yskew
                    reduced_upper_right_x = upper_right_x - (width/2) + (height/2) / yres * xskew
                    reduced_upper_right_y = upper_right_y - (height/2) - (width/2) / xres * yskew
                    reduced_lower_right_x = lower_right_x - (width/2) - (height/2) / yres * xskew
                    reduced_lower_right_y = lower_right_y + (height/2) - (width/2) / xres * yskew

                    # generate random point inside the reduced tile area
                    random_x = random.uniform(reduced_lower_right_x, reduced_upper_left_x)
                    random_y = random.uniform(reduced_lower_right_y, reduced_upper_left_y)

                    has_values = False

                    for file in hq_dir.glob("*_B*_10m.jp2"):
                        dataset = gdal.Open(str(file))
                        x_index = (random_x - lower_left_x) / xres
                        y_index = (random_y - lower_left_y) / ( yres * -1 )
                        x_index, y_index = int(x_index + 0.5), int(y_index + 0.5)
                        array = dataset.ReadAsArray()
                        pixel_val = array[y_index, x_index]
                        if pixel_val > 0:
                            has_values = True

                    if has_values:
                        random_crops = random_crops + 1
                        j = j + 1

                        top_left_x = random_x - (width/2) - (height/2) / yres * xskew
                        top_left_y = random_y + (height/2) - (width/2) / xres * yskew
                        bottom_right_x = random_x + (width/2) + (height/2) / yres * xskew
                        bottom_right_y = random_y - (height/2) + (width/2) / xres * yskew

                        # convert to Point object (shapely)
                        top_left = Point(top_left_x, top_left_y)
                        bottom_right = Point(bottom_right_x, bottom_right_y)

                        file = list(hq_dir.glob("*_B02_10m.jp2"))[0]
                        point = transform_xy_to_latlon(file, Point(random_x, random_y))

                        main_target_folder = output_path / ("%s_%s_%s" % (j, point["lat"], point["lon"]))

                        # target directory for cropped image
                        target_dir = main_target_folder / "sensordata"
                        target_dir.mkdir(parents=True, exist_ok=True)

                        # target directory for meta information
                        meta_dir = main_target_folder / "original-metadata"

                        # target directory for preview image
                        preview_dir = main_target_folder

                        for image_data_item in os.listdir(path_image_data):

                            path_image_data_item = path_image_data / image_data_item

                            target_sub_dir = target_dir / image_data_item

                            if os.path.isdir(path_image_data_item):

                                for item in os.listdir(path_image_data_item):

                                    # set path of img file
                                    path = path_image_data_item / item

                                    # CROP IMAGE
                                    crop_image(path, item, top_left, bottom_right, target_sub_dir, file_format, is_latlon=False)

                        create_preview_rgb_image("*B04_10m.jp2", "*B03_10m.jp2", "*B02_10m.jp2", target_sub_dir, preview_dir)

                        if config.copyMetadata:
                            meta_dir.mkdir(parents=True)
                            tile_dir = big_tile
                            for item in tile_dir.rglob('*'):
                                if item.is_file() and item.suffix.lower() != ".jp2":
                                    target_dir = meta_dir / item.parent.relative_to(tile_dir)
                                    if not target_dir.exists():
                                        target_dir.mkdir(parents=True)
                                    shutil.copy(item, target_dir)

                        if config.createSymlink:
                            tile_dir = big_tile
                            # TODO: set config parameter for realpath or relpath for symlink
                            try:
                                meta_dir.symlink_to(os.path.realpath(str(tile_dir.resolve())), str(meta_dir.parent.resolve()))
                            except PermissionError as e:
                                logger.error(f"Could not create symlink due to permission error!\n \
                                             source: {os.path.realpath(str(tile_dir.resolve()))}\n \
                                             symlink: {str(meta_dir.parent.resolve())}\n \
                                             {repr(e)}")
                                print(f"Could not create symlink to meta dir due to permission error!\n{str(e)}\n")

                        print(f"random crop {j} done.\n")

    print("### Generate combined preview images...")
    combine_images(str(output_path.name), has_subdir=False)
    print("done.")


def trim_crops(source_dir, target_dir, width, height, has_subdir=True):
    """Trim crops to smaller size. Reference point is the center of the image. 
    
    All directories in source_dir will be copied to target_dir 
    and all .jp2/.tif files within that directories will be trimmed to new size. 
    The preview images (preview.tif) will be deleted and new preview images will be created.
    Also new combined previews will be created.

    Parameters
    ----------
    source_dir : str
        path of the directory containing source crops
    target_dir : str
        path of the target directory
    width : int
        width of the trimmed crops in meter
    height : int
        height of the trimmed crops in meter

    """

    # convert to pathlib objects
    source_dir = pathlib.Path(source_dir)
    target_dir = pathlib.Path(target_dir)

    if has_subdir:
        for request in source_dir.glob("*"):
            create_trimmed_crops(request, ( target_dir / request.name ), width, height)
    else:
        create_trimmed_crops(source_dir, target_dir, width, height)


def create_trimmed_crops(source_dir, target_dir, width, height):

    # check if source_dir exists, exit if no
    if not os.path.isdir(str(source_dir)):
        sys.exit("ERROR trim_crops: source_dir is not a directory!")

    # check if target_dir exists, exit if yes
    if os.path.exists(str(target_dir)):
        sys.exit("ERROR trim_crops: target_dir already exists!")

    # create target_dir
    os.makedirs(str(target_dir))

    # set file types to trim
    file_types = [".tif", ".jp2"]
 
    # get subfolders of folder_in
    for n, folder_in in enumerate(source_dir.glob("*"), 1):

        if folder_in.is_dir() and folder_in.name != "0_combined-preview":

            print(f"{n}: {folder_in}")

            # determine output folder
            folder_out = target_dir / folder_in.name

            # copy whole subfolder first
            # copy_function default is shutil.copy2 
            # shutil.cop2: Identical to copy() except that copy2() also attempts to preserve file metadata.
            try:
                shutil.copytree(folder_in, folder_out, symlinks = True, copy_function=shutil.copy)
            except:
                logger.warning("Error in shutil.copytree.")
                logger.warning(sys.exc_info()[0])

            if not folder_out.exists():
                logger.error(sys.exc_info()[0])
                exit()

            # chmod to 664 for files and 775 for dirs
            try:
                dir_mod = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
                file_mod = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH
                os.chmod(folder_out, dir_mod)
                for item in folder_out.rglob("*"):
                    if item.is_dir():
                        os.chmod(item, dir_mod)                    
                    else:
                        os.chmod(item, file_mod)                    
            except:
                logger.warning("Could not change file or folder permissions with chmod!")
                logger.warning(sys.exc_info()[0])

            # get all files of subfolder
            for file in folder_out.rglob("*"):

                # trim files with matching suffix
                if file.suffix in file_types:

                    # remove old preview files
                    if file.name == "preview.tif":
                        # remove file
                        file.unlink()                      

                    elif file.suffix == ".jp2" or file.suffix == ".tif":

                        if file.suffix == ".jp2":
                            # Sentinel-2 img data are in jp2-format
                            # set appropriate format for GDAL lib
                            file_format = "JP2OpenJPEG"
                        else:
                            file_format = "GTiff"

                        try:

                            # open image with GDAL
                            img = gdal.Open(str(file))
                            proj = img.GetProjection()

                            # yres is negative!
                            upper_left_x, xres, xskew, upper_left_y, yskew, yres = img.GetGeoTransform()

                            # get number of columns and rows
                            cols = img.RasterXSize
                            rows = img.RasterYSize

                            # determine center pixel
                            # if cols/rows are even take the next pixel at the top left
                            center_pixel_col = math.ceil(cols / 2)
                            center_pixel_row = math.ceil(rows / 2)

                            # determine coordinates of pixel
                            center_pixel_x = xres * center_pixel_col + xskew * center_pixel_row + upper_left_x 
                            center_pixel_y = yskew * center_pixel_col + yres * center_pixel_row + upper_left_y

                            # shift to center of the pixel
                            center_pixel_x += xres / 2
                            center_pixel_y += yres / 2

                            # calculate top left and bottom right coordinate of area to trim
                            top_left_x = center_pixel_x - (width/2) - (height/2) / yres * xskew
                            top_left_y = center_pixel_y + (height/2) - (width/2) / xres * yskew
                            bottom_right_x = center_pixel_x + (width/2) + (height/2) / yres * xskew
                            bottom_right_y = center_pixel_y - (height/2) + (width/2) / xres * yskew                        

                            # trim image
                            img = gdal.Translate(
                                str(file) + "_new.jp2", 
                                str(file), 
                                format=file_format,
                                outputSRS=proj,
                                projWin=[
                                    top_left_x, 
                                    top_left_y,
                                    bottom_right_x, 
                                    bottom_right_y
                                ])                      

                            # save and replace

                            img = None

                            file.unlink()
                            file_new = pathlib.Path(str(file) + "_new.jp2")
                            file_new.rename(file)

                        except:
                            print(f"Error creating clipped file ({str(file.absolute())})!\n")
                            logger.error(f"Error creating clipped file ({str(file.absolute())})!")
                            logger.error(sys.exc_info()[0])                        

            # create preview image
            s1_image = folder_out / "sensordata" / "s1_cropped.tif"
            if s1_image.exists():
                create_preview_rg_image(s1_image, folder_out, exponential_scale=None)
            else:
                create_preview_rgb_image("*B04_10m.jp2", "*B03_10m.jp2", "*B02_10m.jp2", folder_out / "sensordata" / "R10m", folder_out)
                
    # create combined previews
    create_combined_images(target_dir)


def move_latlon(lat, lon, azimuth, distance):
    """Move on WGS84 ellipsoid from a given point (lat/lon) in certain direction and distance.

    Parameters
    ----------
    lat : float
        latitude of origin point
    lon : float
        longitude of origin point
    azimuth : float
        direction in degree
    distance : int
        distance in meters

    Returns
    -------
    Tuple
        lat : float
            latitude of new point
        lon : float
            longitude of new point

    """
    geoid = Geod(ellps="WGS84")
    lon_new, lat_new, az_new = geoid.fwd(lon, lat, azimuth, distance)
    return lat_new, lon_new


def get_latlon_corner_coordinates(lat, lon, width, height):
    """Get corner coordinates for crop on WGS84 ellipsoid given the center point and width and height of the crop.

    Parameters
    ----------
    lat : float
        latitude of origin point
    lon : float
        longitude of origin point
    width : int
        width of crop in meters
    height : int
        height of crop in meters

    Returns
    -------
    List
        Point
            top left point in WKT format
        Point
            top right point in WKT format            
        Point
            bottom left point in WKT format
        Point
            bottom right point in WKT format
        Point
            top left point in WKT format            

    """
    
    # determine top left corner
    lat_temp, lon_temp = move_latlon(lat, lon, 270, (width / 2))
    top_left_lat, top_left_lon = move_latlon(lat_temp, lon_temp, 0, (height / 2))

    # determine top right corner
    lat_temp, lon_temp = move_latlon(lat, lon, 90, (width / 2))
    top_right_lat, top_right_lon = move_latlon(lat_temp, lon_temp, 0, (height / 2))    

    # determine bottom right corner
    lat_temp, lon_temp = move_latlon(lat, lon, 90, (width / 2))
    bottom_right_lat, bottom_right_lon = move_latlon(lat_temp, lon_temp, 180, (height / 2))

    # determine bottom right corner
    lat_temp, lon_temp = move_latlon(lat, lon, 270, (width / 2))
    bottom_left_lat, bottom_left_lon = move_latlon(lat_temp, lon_temp, 180, (height / 2))    

    return ([ Point(top_left_lon, top_left_lat), 
              Point(top_right_lon, top_right_lat),
              Point(bottom_right_lon, bottom_right_lat),
              Point(bottom_left_lon, bottom_left_lat),
              Point(top_left_lon, top_left_lat)
              ])


def unpack_big_tile(file_name, tile=None):

    if file_name.endswith(".zip") or file_name.endswith(".tar.gz"):
    
        # get path of the packed file
        file_path = config.bigTilesDir / file_name

        # unpack zip file if zip
        if file_name.endswith(".zip"):

            if tile == None:

                # TODO: dirty... (is maybe first entry of zip_ref)
                # get tile by folder name
                new_folder_name = file_name[:-4] + ".SAFE"
                tile = db.get_tile(folder_name = new_folder_name)              

            # unzip
            with zipfile.ZipFile(file=file_path) as zip_ref:
                
                # show progress bar based on number of files in archive
                print("Unpack file: " + file_name)
                for file in tqdm(iterable=zip_ref.namelist(), total=len(zip_ref.namelist())):
                    zip_ref.extract(member=file, path=config.bigTilesDir)

            zip_ref.close()


        # unpack tar file if tar
        if file_name.endswith(".tar.gz"):

            if tile == None:

                # get tile by folder name
                tile = db.get_tile(folder_name = file_name[:-7])

            # create target directory, since there is no root dir in tar package
            target_dir = config.bigTilesDir / tile["folderName"]
            if not os.path.isdir(target_dir):
                os.makedirs(target_dir)                    

            # untar
            with tarfile.open(name=file_path, mode="r:gz") as tar_ref:

                # show progress bar based on number of files in archive
                print("Unpack file: " + file_name)
                for file in tqdm(iterable=tar_ref.getmembers(), total=len(tar_ref.getmembers())):
                    tar_ref.extract(member=file, path=target_dir)

            tar_ref.close()


        # remove packed file
        os.remove(file_path)

        # set unpacked date in database
        db.set_unpacked_for_tile(tile["rowid"])


def crop_tiles(poi_id):
    
    print("\nCrop tiles:")
    print("-----------------")

    poi = db.get_poi_from_id(poi_id)

    print("(w: %d, h: %d)\n" % (poi["width"], poi["height"]))


    # crop tile if point of interest (POI) exists and width and height bigger than 0
    if not poi == None and poi["width"] > 0 and poi["height"] > 0:

        # get tiles that need to be cropped
        tiles = db.get_tiles_for_poi(poi_id)
        
        # go through the tiles
        for tile in tiles:
        
            # crop if tile is not cropped yet (with the parameters of POI)
            if tile["tileCropped"] == None and not tile["unzipped"] == None:

                print("Cropping %s ..." % tile["folderName"])

                if download.check_for_existing_big_tile_folder(tile) == False:

                    print("Big tile folder missing!")
                    print("Cropping not possible.")

                    if download.check_for_existing_big_tile_archive(tile) == False:

                        logger.warning("Big tile missing although marked as unzipped in database.")
                        db.clear_download_complete_for_tile(tile['rowid'])
                        db.clear_unpacked_for_tile(tile['rowid'])
                        print("Big tile archive missing!")
                        print("The internal database got updated (missing download).")
                        print("Please start the download process again.")

                    else:

                        logger.warning("Big tile not unpacked, but crop function started.")
                        db.clear_unpacked_for_tile(tile['rowid'])
                        print("Big tile archive found!")
                        print("Since download processes are maybe running, no automatic unpacking is performed at this point.")
                        print("Please unpack big tiles using the function unpack_big_tiles() or unpack manually and start again.\n")

                    # skip this tile
                    continue

                if poi["platform"] == "Sentinel-1" or poi["platform"] == "Sentinel-2":
                    beginposition = convert_date(tile["beginposition"], new_format="%Y%m%d-%H%M")
                else:
                    beginposition = convert_date(tile["beginposition"], new_format="%Y%m%d")

                poi_parameters = get_poi_parameters_for_output_folder(poi)
                connection_id = db.get_tile_poi_connection_id(poi_id, tile["rowid"])
                main_target_folder = config.croppedTilesDir / poi["groupname"] / poi_parameters / ( "%s_%s_%s_%s" % (connection_id, poi["lon"], poi["lat"], beginposition) )

                # target directory for cropped image
                sensor_target_dir = main_target_folder / "sensordata"
                sensor_target_dir.mkdir(parents = True, exist_ok = True)               

                # target directory for meta information
                meta_target_dir = main_target_folder / "original-metadata"

                # target directory for preview image
                preview_dir = main_target_folder 
                # preview_dir.mkdir(parents = True, exist_ok = True)   


                # SENTINEL 1 CROPPING
                
                if poi["platform"] == "Sentinel-1":

                    if config.gptSnap.exists():

                        corner_coordinates = get_latlon_corner_coordinates(poi["lat"], poi["lon"], poi["width"], poi["height"])

                        # convert S1 crops to UTM projection or leave it in WGS84
                        if config.covertS1CropsToUTM == True:
                            projection = "AUTO:42001"
                        else:
                            projection = "EPSG:4326"

                        target_file = sensor_target_dir / "s1_cropped.tif"

                        # preprocess and crop using SNAP GPT
                        poly = Polygon([[p.x, p.y] for p in corner_coordinates])
                        command = [str(config.gptSnap), os.path.realpath(str(config.xmlSnap)), 
                                   ("-PinDir=" + os.path.realpath(str(config.bigTilesDir / tile["folderName"]))),
                                   ("-Psubset=" + poly.wkt),
                                   ("-PoutFile=" + os.path.realpath(str(target_file))),
                                   ("-PmapProjection=" + projection)]
                        subprocess.call(command)

                        if target_file.exists():

                            print("done.\n")

                            # create preview image
                            create_preview_rg_image(str(target_file), main_target_folder, exponential_scale=None)

                            # set date for tile cropped 
                            db.set_tile_cropped(poi_id, tile["rowid"], main_target_folder)

                        else:

                            print("Sentinel-1 crop could not be created!")

                            # cancel crop
                            db.set_cancelled_tile_for_poi(poi_id, tile["rowid"])


                        # copy or link metadata
                        if config.copyMetadata:                            
                            print("Copy metadata...")
                            meta_target_dir.mkdir(parents = True)
                            tile_dir = config.bigTilesDir / tile["folderName"]
                            for item in tile_dir.rglob('*'):
                                if item.is_file() and item.suffix.lower() != ".tiff" and item.suffix.lower() != ".safe":
                                    sensor_target_dir = meta_target_dir / item.parent.relative_to(tile_dir)
                                    if not sensor_target_dir.exists():
                                        sensor_target_dir.mkdir(parents = True)
                                    shutil.copy(item, sensor_target_dir)
                            print("done.\n")    

                        if config.createSymlink:
                            tile_dir = config.bigTilesDir / tile["folderName"]
                            if not meta_target_dir.exists():
                                # TODO: set config parameter for realpath or relpath for symlinks
                                try:
                                    meta_target_dir.symlink_to(os.path.realpath(str(tile_dir.resolve())), str(meta_target_dir.parent.resolve()))
                                    print("Symlink created.")                        
                                except PermissionError as e:
                                    logger.error(f"Could not create symlink due to permission error!\n \
                                                 source: {os.path.realpath(str(tile_dir.resolve()))}\n \
                                                 symlink: {str(meta_target_dir.parent.resolve())}\n \
                                                 {repr(e)}")
                                    print(f"Could not create symlink to meta dir due to permission error!\n{str(e)}\n")                                

                    else:
                        print("SNAP GPT not configured. Sentinel-1 tiles cannot be cropped.\n")
                        db.set_cancelled_tile_for_poi(poi_id, tile["rowid"])  


                # SENTINEL 2 CROPPING

                if poi["platform"] == "Sentinel-2":

                    corner_coordinates = None

                    # Sentinel-2 img data are in jp2-format
                    # set appropriate format for GDAL lib
                    file_format="JP2OpenJPEG"

                    # go through "SAFE"-directory structure of Sentinel-2

                    is_s2l1 = True

                    path_granule = config.bigTilesDir / tile["folderName"] / "GRANULE"
                    for main_folder in os.listdir(path_granule):

                        path_image_data = path_granule / main_folder / "IMG_DATA"
                        for image_data_item in os.listdir(path_image_data):

                            path_image_data_item = path_image_data / image_data_item

                            # if Level-1 data path_image_data_item is already an image file
                            # if Level-2 data path_image_data_item is a directory with image files

                            if os.path.isdir(path_image_data_item):

                                # Level-2 data

                                is_s2l1 = False

                                target_sub_dir = sensor_target_dir / image_data_item
                            
                                for item in os.listdir(path_image_data_item):

                                    # set path of img file
                                    path = path_image_data_item / item

                                    if corner_coordinates == None:
                                        corner_coordinates = get_xy_corner_coordinates(path, poi["lat"], poi["lon"], poi["width"], poi["height"])

                                    # CROP IMAGE
                                    crop_image(path, item, corner_coordinates["top_left"], corner_coordinates["bottom_right"], 
                                                  target_sub_dir, file_format, is_latlon=False)

                                create_preview_rgb_image("*B04_10m.jp2", "*B03_10m.jp2", "*B02_10m.jp2", target_sub_dir, preview_dir)
                            
                            else:

                                # Level-1 data

                                # set path of image file
                                path = path_image_data_item

                                if corner_coordinates == None:
                                    corner_coordinates = get_xy_corner_coordinates(path, poi["lat"], poi["lon"], poi["width"], poi["height"])

                                # CROP IMAGE
                                crop_image(path, image_data_item, corner_coordinates["top_left"], corner_coordinates["bottom_right"], 
                                              sensor_target_dir, file_format, is_latlon=False)

                        if is_s2l1:
                            create_preview_rgb_image("*B04.jp2", "*B03.jp2", "*B02.jp2", sensor_target_dir, preview_dir)                                

                    print("done.\n")        

                    if config.copyMetadata:                            
                        print("Copy metadata...")
                        meta_target_dir.mkdir(parents = True)
                        tile_dir = config.bigTilesDir / tile["folderName"]
                        for item in tile_dir.rglob('*'):
                            if item.is_file() and item.suffix.lower() != ".jp2":
                                sensor_target_dir = meta_target_dir / item.parent.relative_to(tile_dir)
                                if not sensor_target_dir.exists():
                                    sensor_target_dir.mkdir(parents = True)
                                shutil.copy(item, sensor_target_dir)
                        print("done.\n")

                    if config.createSymlink:
                        tile_dir = config.bigTilesDir / tile["folderName"]
                        if not meta_target_dir.exists():
                            try:
                                # TODO: set config parameter for realpath or relpath for symlinks
                                meta_target_dir.symlink_to(os.path.realpath(str(tile_dir.resolve())), str(meta_target_dir.parent.resolve()))
                                print("Symlink created.")
                            except PermissionError as e:
                                logger.error(f"Could not create symlink due to permission error!\n \
                                             source: {os.path.realpath(str(tile_dir.resolve()))}\n \
                                             symlink: {str(meta_target_dir.parent.resolve())}\n \
                                             {repr(e)}")
                                print(f"Could not create symlink to meta dir due to permission error!\n{str(e)}\n")                                

                    # set date for tile cropped 
                    db.set_tile_cropped(poi_id, tile["rowid"], main_target_folder)


                # LANDSAT CROPPING

                if poi["platform"].startswith("LANDSAT"):
                
                    print("Cropping of Landsat data not yet supported.\n")
                    db.set_cancelled_tile_for_poi(poi_id, tile["rowid"])

                    # # Landsat img data are in GeoTiff-format
                    # # set appropriate format for GDAL lib
                    # file_format="GTiff"

                    # # all images are in root dir of tile

                    # # set path of root dir of tile
                    # path_image_data = config.bigTilesDir / tile["folderName"]

                    # # TODO: switch to pathlib (for item in path_image_data)
                    # # go through all files in root dir of tile
                    # for item in os.listdir(path_image_data):

                    #     # if file ends with tif then crop
                    #     if item.lower().endswith(".tif"):

                    #         # set path of image file
                    #         path = path_image_data / item

                    #         # CROP IMAGE
                    #         crop_image(path, item, topLeft, bottomRight, sensor_target_dir, file_format)

                    # if poi["platform"] == "LANDSAT_8_C1":
                    #     r_band_search_pattern = "*B4.TIF"
                    #     g_band_search_pattern = "*B3.TIF"
                    #     b_band_search_pattern = "*B2.TIF"
                    # else:
                    #     r_band_search_pattern = "*B3.TIF"
                    #     g_band_search_pattern = "*B2.TIF"
                    #     b_band_search_pattern = "*B1.TIF"                           
                    # create_preview_rgb_image(r_band_search_pattern, g_band_search_pattern, b_band_search_pattern, sensor_target_dir, preview_dir)                         

                    # print("done.")

                    # if config.copyMetadata:
                    #     print("Copy metadata...")
                    #     meta_target_dir.mkdir(parents = True)
                    #     for item in path_image_data.glob('*'):
                    #         if item.is_file():
                    #             if item.suffix.lower() != ".tif":
                    #                 shutil.copy(item, meta_target_dir)
                    #         if item.is_dir():
                    #             shutil.copytree(item, (meta_target_dir / item.name))
                    #     print("done.\n")

                    # if config.createSymlink:
                    #     tile_dir = path_image_data
                    #     try:
                        #     # TODO: set config parameter for realpath or relpath for symlink
                        #     meta_target_dir.symlink_to(os.path.realpath(str(tile_dir.resolve())), str(meta_target_dir.parent.resolve()))
                        #     print("Symlink created.")                            
                        # except PermissionError as e:
                        #     logger.error(f"Could not create symlink due to permission error! {repr(e)}")
                        #     print(f"Could not create symlink to meta dir due to permission error!\n{str(e)}\n")                        

                    # # set date for tile cropped 
                    # db.set_tile_cropped(poi_id, tile["rowid"], main_target_folder) 


def get_poi_parameters_for_output_folder(poi):
    
    folder_elements = []
    folder_name = ""

    try:
        folder_elements.append("df" + convert_date(poi["dateFrom"], "%Y%m%d"))
    except:
        pass

    try:
        folder_elements.append("dt" + convert_date(poi["dateTo"], "%Y%m%d"))
    except:
        pass
        
    try:
        if poi["platform"] == "Sentinel-1":
            folder_elements.append("pfS1")
        if poi["platform"] == "Sentinel-2":
            folder_elements.append("pfS2")
        if poi["platform"] == "LANDSAT_TM_C1":
            folder_elements.append("pfLTM")
        if poi["platform"] == "LANDSAT_ETM_C1":
            folder_elements.append("pfLETM")
        if poi["platform"] == "LANDSAT_8_C1":
            folder_elements.append("pfL8")
    except:
        pass
        
    try:
        folder_elements.append("tl" + str(poi["tileLimit"]))
    except:
        pass
        
    try:
        folder_elements.append("cc" + str(poi["cloudcoverpercentage"]))
    except:
        pass
        
    try:
        folder_elements.append("pm" + str(poi["polarisatiomode"]))
    except:
        pass
        
    try:
        folder_elements.append("pt" + str(poi["producttype"]))
    except:
        pass
        
    try:
        folder_elements.append("som" + str(poi["sensoroperationalmode"]))
    except:
        pass
        
    try:
        folder_elements.append("si" + str(poi["swathidentifier"]))
    except:
        pass
        
    try:
        folder_elements.append("tls" + str(poi["timeliness"]))
    except:
        pass
        
    try:
        folder_elements.append("w" + str(poi["width"]))
    except:
        pass
        
    try:
        folder_elements.append("h" + str(poi["height"]))
    except:
        pass

    for item in folder_elements:
        if not item.endswith("None"):            
            if len(folder_name) > 0:
                folder_name = folder_name + "_"
            folder_name = folder_name + item

    return folder_name    


def save_missing_tile_projections():

    tiles = db.get_tiles_without_projection_info()
    for tile in tiles:
        save_tile_projection(tile=tile)


def save_tile_projection(product_id=None, tile=None):

    if tile == None and product_id == None:
        return None

    if tile == None:
        tile = db.get_tile(product_id)

    if tile != None and tile["downloadComplete"] != None:

        main_folder = config.bigTilesDir / tile["folderName"]
        projection = None

        if tile["platform"] == "Sentinel-1":

            image_folder = main_folder / "measurement"

            image = list(image_folder.glob("*.tiff"))[0]

            projection = get_projection_from_file(image, tile["platform"])

        if tile["platform"] == "Sentinel-2":

            image_folder = main_folder / "GRANULE"

            image = list(image_folder.rglob("*_B02*.jp2"))[0]

            projection = get_projection_from_file(image, tile["platform"])

        # TODO: save tile projection of Landsat product

        # save projection to database
        if projection != None:
            db.update_tile_projection(tile["rowid"], projection)


def get_projection_from_file(path, platform):

    if path != None and path.exists():

        img = rasterio.open(str(path))

        if platform == "Sentinel-1":

            # Sentinel-1 uses ground control points (GCPs)
            gcps, gcp_crs = img.gcps
            projection = gcp_crs

        if platform == "Sentinel-2":

            projection = img.crs
    
        return str(projection)


def retrieve_scene_classes(crops_path):
    """Retrieves the ratio of the Sentinel scene classification values within the crop
    and stores the result to the database.

    Retrieves the ratio of the Sentinel scene classification values within the crop
    and stores the result to the database.
    Classifications (obtained from https://dragon3.esa.int/web/sentinel/technical-guides/sentinel-2-msi/level-2a/algorithm):
    0: NO_DATA
    1: SATURATED_OR_DEFECTIVE    
    2: DARK_AREA_PIXELS
    3: CLOUD_SHADOWS
    4: VEGETATION
    5: NOT_VEGETATED
    6: WATER
    7: UNCLASSIFIED
    8: CLOUD_MEDIUM_PROBABILITY
    9: CLOUD_HIGH_PROBABILITY
    10: THIN_CIRRUS
    11: SNOW

    Parameters
    ----------
    crops_path : Path
        Path of crops. Crops must match with database entries (especially crop id).
    """

    # file containing information (Sentinel-2 only)
    filename_postfix = "_SCL_20m.jp2"

    for crop in tqdm(crops_path.glob("*"), desc="Retrieving scene classes: "):

        if crop.is_dir() and crop.name != "0_combined-preview":

            crop_id = crop.name.split("_")[0]

            # TODO: check path and coordinates in database (the abs path [better would be rel] is stored for each crop)

            scl_folder = crop / "sensordata" / "R20m"

            if scl_folder.is_dir():

                ratios = None

                for scl_image_path in scl_folder.glob(f"*{filename_postfix}"):

                    if ratios == None:

                        scl_image_obj = gdal.Open(str(scl_image_path))
                        scl_image = numpy.array(scl_image_obj.GetRasterBand(1).ReadAsArray())
                        pixels = scl_image.shape[0] * scl_image.shape[1]

                        ratios = {}

                        unique, counts = numpy.unique(scl_image, return_counts=True)
                        occurences = dict(zip(unique, counts))

                        for key in occurences:
                            ratios[key] = occurences[key] / pixels

                db.set_scence_class_ratios_for_crop(crop_id, ratios)


def refresh_unzipped_big_tiles():
    tiles = db.get_all_tiles()
    for tile in tiles:
        if download.check_for_existing_big_tile_folder(tile) == True:
            db.set_unpacked_for_tile(tile['rowid'])
        elif download.check_for_existing_big_tile_archive(tile) == True:
            db.set_download_complete_for_tile(tile['rowid'])
        else:
            db.clear_download_complete_for_tile(tile['rowid'])
            db.clear_unpacked_for_tile(tile['rowid'])


def copy_big_tiles(target_path, required_only=False):
    """Copies the required big tiles from big tiles folder to target path.

    Copies the required big tiles from big tiles folder to target path.
    The required tiles are determined from the internal database.

    Parameters
    ----------
    target_path : Path
        Path where the big tiles should be copied to.
    required_only : boolean, optional
        If true, only required tiles for the outstanding crops will be copied.
        If false, all tiles with existing entry in internal database will be copied.        
    """    

    target_path = pathlib.Path(target_path)

    required_tiles = set()
    if required_only:
        tiles = db.get_required_tiles()
    else:
        tiles = db.get_all_tiles()

    for tile in tiles:
        required_tiles.add(tile['folderName'])

    try:
        target_path.mkdir(exist_ok=True, parents=True)
    except OSError as error:
        print (f"Creation of the directory {target_path} failed")
        print(error)
        sys.exit()

    for required_tile in tqdm(required_tiles, desc="Copying big tiles: "):
        tile_path = config.bigTilesDir / required_tile
        if tile_path.is_dir():
            copy_tree(str(tile_path.absolute()), str((target_path / required_tile).absolute()), preserve_mode=0, preserve_times=0)
            # TODO: this is probably not the best solution
            tile = db.get_tile(folder_name=required_tile)
            db.set_unpacked_for_tile(tile['rowid'])


def filter_and_move_crops(crops_path, output_path, lower_boundaries=None, upper_boundaries=None, use_database_scene_values=True, \
                          move_crops_without_scene_classifications=False, \
                          raster_path=None, shape_file=None, attribute_name=None, include_values=None, \
                          exclude_values=None, include_distance=0):
    """Filters crops based on scene classification values (Sentinel-2) or based on attribute values in shape file and moves them to new directory.

    If shape file is specified, crops are filtered by attribute values of intersecting polygons.

    Otherwise crops are filtered based on scene classification values (Sentinel-2).
    Classifications (obtained from https://dragon3.esa.int/web/sentinel/technical-guides/sentinel-2-msi/level-2a/algorithm):
    0: NO_DATA
    1: SATURATED_OR_DEFECTIVE    
    2: DARK_AREA_PIXELS
    3: CLOUD_SHADOWS
    4: VEGETATION
    5: NOT_VEGETATED
    6: WATER
    7: UNCLASSIFIED
    8: CLOUD_MEDIUM_PROBABILITY
    9: CLOUD_HIGH_PROBABILITY
    10: THIN_CIRRUS
    11: SNOW

    The filtered crops will be moved to the output path.

    Main Parameters
    ---------------
    crops_path : Path
        Path of crops. Crops must match with database entries (especially crop id), if database should be used.
    output_path : Path
        Path where the filtered crops should be moved to.

    Parameters (filter by scene classification values of Sentinel-2)
    ----------------------------------------------------------------
    lower_boundaries : dict, optional
        Dictionary with lower boundary ratios of scene classes.
    upper_boundaries : dict, optional
        Dictionary with upper boundary ratios of scene classes.
    use_database_scene_values : boolean, optional
        If true, the scene ratios for each crop in the database will be used.
        Otherwise, the scene ratios get retrieved by the scene classification map (Sentinel-2).
        Default is true.
    move_crops_without_scene_classifications : boolean, optional
        Default is false.

    Parameters (filter by shape file)
    ---------------------------------
    raster_path : str
        Relative path to raster file of crops. Wildcards supported by the glob function of pathlib can be used. 
    shape_file : str, optional
        Path of the shapefile. Currently the shape file and the crops must use the same coordinate reference system (CRS).
    attribute_name : str, optional
        Name of the polygons' attribute that is used for filtering.
    include_values : tuple, optional
        Tuple of possible attribute values. If crop intersects with polygon having this attribute value, the crop will be moved.
    exclude_values : tuple, optional
        Tuple of possible attribute values. If crop intersects with polygon having this attribute value, the crop will not be moved.
    include_distance : int, optional
        By default the range of each crop will be compared to the polygons in the shape file. 
        The parameter include_distance can be used to extend the range of each crop in meters.
        Assumes that crops and shape file are using UTM as CRS.
    """

    try:
        output_path.mkdir(exist_ok=True, parents=True)
    except OSError as error:
        print (f"Creation of the directory {output_path} failed")
        print(error)
        sys.exit()       

    # counter for filtered out crops
    counter = 0
    total_crops = 0

    if not isinstance(include_values, type(None)):
        include_values = tuple(str(x) for x in include_values)

    if not isinstance(exclude_values, type(None)):
        exclude_values = tuple(str(x) for x in exclude_values)

    if not isinstance(shape_file, type(None)):

        # Filter by polygons in shape file

        shape_file = pathlib.Path(shape_file)
        if not shape_file.exists():
            return None

        with fiona.open(shape_file.absolute(), "r") as shape_feature_collection:

            ## Instantiate index class (should be faster, but we probably lose further attribute infos)
            # idx = index.Index()
            # for i, feature in enumerate(shape_feature_collection):
            #     idx.insert(i, shape(feature['geometry']).bounds)

            for crop in tqdm(crops_path.glob("*"), desc="Filtering and moving crops: "):

                if crop.is_dir() and crop.name != "0_combined-preview":

                    total_crops = total_crops + 1

                    if not isinstance(exclude_values, type(None)):
                        move = True
                    else:
                        move = False

                    try:
                        raster_file = list(crop.glob(raster_path))[0]
                    except:
                        print(f"Raster file not found for crop: {crop.name}")
                        continue

                    with rasterio.open(raster_file) as raster:

                        left = raster.bounds.left - include_distance
                        bottom = raster.bounds.bottom - include_distance
                        right = raster.bounds.right + include_distance
                        top = raster.bounds.top + include_distance
                        
                        p1 = Point(left,top)
                        p2 = Point(left,bottom)
                        p3 = Point(right,bottom)
                        p4 = Point(right,top)
                        
                        pointList = [p1, p2, p3, p4, p1]
                        poly = Polygon([[p.x, p.y] for p in pointList])

                        for feature in shape_feature_collection:
                            if shape(feature['geometry']).intersects(shape(poly)):
                                clc_code = str(feature['properties'][attribute_name])

                                if not isinstance(exclude_values, type(None)):
                                    if clc_code in exclude_values:
                                        move = False

                                elif not isinstance(include_values, type(None)):
                                    if clc_code in include_values:
                                        move = True

                    if move:

                        shutil.move(str(crop.absolute()), str(output_path.absolute()))

                    else:
                        counter = counter + 1

    else:

        # Filter by scene classification values (Sentinel-2)

        if lower_boundaries == None and upper_boundaries == None:
            return None

        # file containing information (Sentinel-2 only)
        filename_postfix = "_SCL_20m.jp2"        

        for crop in tqdm(crops_path.glob("*"), desc="Filtering and moving crops: "):

            if crop.is_dir() and crop.name != "0_combined-preview":

                total_crops = total_crops + 1

                move = False
                ratios = None

                crop_id = crop.name.split("_")[0]

                if use_database_scene_values:

                    ratios = {}

                    crop_meta_data = db.get_tile_poi_connection(int(crop_id))
                    for key in crop_meta_data.keys():
                        if key.startswith("sceneClass"):
                            if crop_meta_data[key] != None:
                                ratios[int(key.replace("sceneClass", ""))] = crop_meta_data[key]

                else:

                    scl_folder = crop / "sensordata" / "R20m"

                    if scl_folder.is_dir():

                        for scl_image_path in scl_folder.glob(f"*{filename_postfix}"):

                            if ratios == None:

                                scl_image_obj = gdal.Open(str(scl_image_path))
                                scl_image = numpy.array(scl_image_obj.GetRasterBand(1).ReadAsArray())
                                pixels = scl_image.shape[0] * scl_image.shape[1]

                                ratios = {}

                                unique, counts = numpy.unique(scl_image, return_counts=True)
                                occurences = dict(zip(unique, counts))

                                for key in occurences:
                                    ratios[key] = occurences[key] / pixels

                                scl_image_obj = None


                if ratios == None or ( isinstance(ratios, dict) and len(ratios) == 0 ):
                    
                    if move_crops_without_scene_classifications:
                        move = True

                else:

                    move = True

                    if lower_boundaries != None and isinstance(lower_boundaries, dict) and len(lower_boundaries) > 0:

                        for key in lower_boundaries:

                            if not ( key in ratios and ratios[key] >= lower_boundaries[key] ):

                                move = False

                    if upper_boundaries != None and isinstance(upper_boundaries, dict) and len(upper_boundaries) > 0:

                        for key in ratios:

                            if key in upper_boundaries and ratios[key] > upper_boundaries[key]:

                                move = False
                                print(f"### [{counter+1}] CROP: {crop.name}")
                                print(f"prevent moving: key:{key} ratio:{ratios[key]} boundary:{upper_boundaries[key]}")

                if move:

                    # TODO: save new path to database (if crop exists in database)

                    shutil.move(str(crop.absolute()), str(output_path.absolute()))

                else:
                    counter = counter + 1

    print(f"Total crops: {total_crops}")
    print(f"Moved crops: {str(total_crops - counter)}")
    print(f"Filtered out crops: {counter}")


def move_imperfect_S1_crops(source_dir, target_dir):

    crop_list = list(source_dir.glob("*"))

    for crop in tqdm(crop_list, desc="Checking crops: "):

        if not crop.name.startswith("0_"):

            s1_image = crop / "sensordata" / "s1_cropped.tif"
            
            if s1_image.exists():

                raster_array = None

                # Read raster data as numeric array from file
                try:
                    raster_array = gdal_array.LoadFile(str(s1_image.absolute()))
                except:
                    print(f"Error in reading file. Moving crop {crop.name}")
                    target_dir.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(crop.absolute()), str(target_dir.absolute()))

                if isinstance(raster_array, numpy.ndarray):
                    pixels = raster_array.shape[1] * raster_array.shape[2]
                    pixels_na = 0

                    for row in range(raster_array.shape[1]):
                        for col in range(raster_array.shape[2]):
                            if raster_array[0][row][col] == 0 and raster_array[1][row][col] == 0:
                                pixels_na = pixels_na + 1

                    # if 10% of pixels contain value 0 in both bands, move crop to target dir
                    if pixels_na / pixels >= 0.1:
                        print(f"\nMoving crop {crop.name} (imperfect area: {math.ceil(pixels_na / pixels * 100)}%)")
                        target_dir.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(crop.absolute()), str(target_dir.absolute()))                      


def get_coordinate_list_from_csv(csv_path):
    data = None
    csv_path = pathlib.Path(csv_path)
    if csv_path.exists():
        col_list = ["lon", "lat"]
        data = pandas.read_csv(csv_path, usecols=col_list, dtype=str)
    return data


def move_crops_containing_locations(csv_path, source_dir, target_dir, based_on_foldername=False):

    crop_list = list(source_dir.glob("*"))
    coordinates = get_coordinate_list_from_csv(csv_path)

    if based_on_foldername:
        print("Checking locations based on folder name.")
    else:
        print("Checking locations within preview.tif images.")

    if isinstance(coordinates, pandas.core.frame.DataFrame) and len(coordinates) > 0:

        for crop in tqdm(crop_list, desc="Checking crops: "):

            if not crop.name.startswith("0_"):

                if not based_on_foldername:

                    preview_file = crop / "preview.tif"
                        
                    if not preview_file.exists():
                        continue

                    try:
                        img = rasterio.open(str(preview_file.absolute()))
                    except:
                        print(f"Could not open preview.tif: {crop.name}")
                        continue

                move_crop = False
                crop_components = crop.name.split("_")
                if len(crop_components) >= 3:
                    crop_lon = str(crop_components[1])
                    crop_lat = str(crop_components[2])
                    crop_lon, crop_lat = reduce_coordinate_digits(crop_lon, crop_lat)
                else:
                    if based_on_foldername:
                        print(f"Crop name contains no coordinates: {crop.name}")
                        continue
                    else:
                        crop_lon = None
                        crop_lat = None

                for i in range(len(coordinates)):

                    lon = str(coordinates["lon"][i])
                    lat = str(coordinates["lat"][i])
                    lon, lat = reduce_coordinate_digits(lon, lat)

                    if based_on_foldername:

                        if ( lon in crop_lon or crop_lon in lon ) and ( lat in crop_lat or crop_lat in lat ):
                            move_crop = True

                    else:

                        inProj = pyproj.Proj(init='epsg:4326')
                        outProj = pyproj.Proj(img.crs)

                        x, y = pyproj.transform(inProj, outProj, lon, lat)

                        row, col = rasterio.transform.rowcol(img.transform, x, y)

                        if row > 0 and row <= img.shape[0] and col > 0 and col <= img.shape[1]:
                            img.close()
                            move_crop = True

                    if move_crop:
                        try:
                            print(f"\nMoving crop {crop.name} (contains point lat:{lat} lon:{lon})")
                            target_dir.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(crop.absolute()), str(target_dir.absolute()))

                        except:
                            print(f"\nCould not move: {crop.name}")

                        break                     


def get_unique_lon_lat_set(source_dir=None, has_subdir=True, postfix="", csv_path=None, lon_lat_set=None, delimiter="_", lon_position=1, lat_position=2):
    """Loops through every folder in source dir, optionally looks for matching postfix if defined and returns 
    a set of strings with unique longitude and latitude positions.

    Parameters
    ----------
    source_dir : path, optional
        Path of trimmed crops 
    has_subdir : boolean, optional
        True if source_dir has subdirectories containing the crop directories.
        False if source_dir contains the crop directories
        Default is True.
    postfix : string, optional
        Set desired postfix for selecting specific folders containing a certain string.
        Default is empty string and therefore every folder in source_dir is included.
    csv_path : path, optional
        Path to csv file containing lat lon coordinates.
    lon_lat_set : set, optional
        Set containing coordinates, which should be appended with new ones. 
    delimiter : str, optional
        Delimiter used for crop folder names.
        Default is "_"
    lon_position : int, optional
        Position of lon coordinate in crop folder name starting at 0.
        Default is 1.        
    lat_position : int, optional
        Position of lat coordinate in crop folder name starting at 0.
        Default is 2.
    """
    
    # Set is an unordered list which allows no duplicated entries
    if not isinstance(lon_lat_set, set):
        lon_lat_set = set()

    print(f"Reading unique positions...")

    if isinstance(source_dir, pathlib.PurePath):

        if has_subdir:
            search_folders = list(source_dir.glob(f"*{postfix}"))
        else:
            search_folders = [source_dir]

        for folder in search_folders:
            if folder.is_dir():
                for crop in tqdm(folder.glob("*"), desc=f"Reading crops in {folder.name}: "):
                    if crop.is_dir() and not crop.name.startswith("0_"):
                        lon = crop.name.split("_")[lon_position]
                        lat = crop.name.split("_")[lat_position]
                        # reduce digits to specified length
                        lon, lat = reduce_coordinate_digits(lon, lat)
                        lon_lat_set.add((lon, lat))

    elif isinstance(csv_path, pathlib.PurePath):

        if csv_path.exists():
            col_list = ["lon", "lat"]
            data = pandas.read_csv(csv_path, usecols=col_list, dtype=str)

            for i in range(len(data)):

                lon = str(data["lon"][i])
                lat = str(data["lat"][i])
                # reduce digits to specified length
                lon, lat = reduce_coordinate_digits(lon, lat)

                lon_lat_set.add(lon + "_" + lat)

    return lon_lat_set


def reduce_coordinate_digits(lon, lat):
    lon = lon[ : ( lon.find(".") + config.coordinateDecimalsForComparison + 1 ) ]
    lat = lat[ : ( lat.find(".") + config.coordinateDecimalsForComparison + 1 ) ]
    return lon, lat


def get_image_path_list(source_dir, location, postfix=""):
    """Find one or more images of a specified location in the source directory and returns a list containing the 
    paths of these images.

    Parameters
    ----------
    source_dir: Path
        Path of trimmed crops
    location: Tuple
        Set the location of the image in longitude and latitude format as strings inside a tuple (e.g. ("-68.148781", "44.77383"))
    postfix: String, optional
        Set desired postfix for selecting specific folders containing a certain string 
        (by default is empty "" and therefore selects every folder in source_dir) 
    """

    lon, lat = location
    image_path_list = []
    for folder in source_dir.glob(f"*{postfix}"):
        if folder.is_dir() and folder.name.split("_")[-1] != "stacked":
            for crop in folder.glob(f"*{lon}*_{lat}*"):
                if crop.is_dir():
                    image_path = crop / "sensordata" / "s1_cropped.tif"
                    if image_path.exists():
                        image_path_list.append(image_path)

    return image_path_list


def stack_trimmed_images(source_dir, image_path_list, target_dir, location, postfix="", tif_band_name_list=["s1_stacked_VV.tif", "s1_stacked_VH.tif"], \
    split_target_dir=False):
    """Stacks trimmed images from provided image_path_list with rasterio in order to preserve the georeferencing
    and writes both bands to tif files ("s1_stacked_VV.tif", "s1_stacked_VH.tif").

    Parameters
    ----------
    source_dir: String
        Path of trimmed crops of various recording times which shall be stacked 
    image_path_list: List
        List containing paths of images of the same location with different capture dates.
    target_dir: Path
        Path where the stacked image shall be stored
    location: Tuple
        Set the location of the image in longitude and latitude format as strings inside a tuple (e.g. ("-68.148781", "44.77383"))
    postfix: String, optional
        Set desired postfix for selecting specific folders containing a certain string 
        (by default is empty "" and therefore selects every folder in source_dir)
    tif_band_name_list: String, optional
        Set names for the bands of the tif tiles (by default the bands are called ["s1_stacked_VV.tif", "s1_stacked_VH.tif"])
    split_target_dir: Boolean, optional
        Splits target dir into several dirs with different stack sizes.
        Default is false.        
    """

    rasterio_image_list = []
    ref_left_upper_corner_coordinates = None

    for image_path in image_path_list:

        image = rasterio.open(image_path)

        if isinstance(ref_left_upper_corner_coordinates, type(None)):

            ref_left_upper_corner_coordinates = image.transform * (0, 0)
            rasterio_image_list.append(image)

        else:

            img_left_upper_corner_coordinates = image.transform * (0, 0)

            # check if distance of the two points is within a 10x10 meter square
            if abs(ref_left_upper_corner_coordinates[0] - img_left_upper_corner_coordinates[0]) <= 10 and \
               abs(ref_left_upper_corner_coordinates[1] - img_left_upper_corner_coordinates[1]) <= 10:
                rasterio_image_list.append(image)
            else:
                print(f"WARNING - Image omitted due to different corner coordinates: {image_path} \
                    [ref: {repr(ref_left_upper_corner_coordinates)} img: {repr(img_left_upper_corner_coordinates)}]")   
        
    if split_target_dir:
        size_appendix = f"{len(rasterio_image_list)}-img"
    else:
        size_appendix = ""

    # if a postfix exists the foldername for the stacked_images will be complemented with the postfix string
    stacked_image_path = target_dir / ("_".join(filter(None, [source_dir.name, postfix, "stacked", size_appendix]))) / "_".join(location)

    try:
        stacked_image_path.mkdir(exist_ok=True, parents=True)
    except OSError as error:
        print (f"Creation of the directory {stacked_image_path} failed")
        print(error)
        return

    try:
        profile = rasterio_image_list[0].profile
        profile.update({"count":len(rasterio_image_list)})

        for band, tif_file in enumerate(tif_band_name_list, start=1):
            with rasterio.open((stacked_image_path / tif_file), "w", **profile) as dest:
                for i, image in enumerate(rasterio_image_list, start=1):
                    dest.write(image.read(band), i)

    except IndexError as error:
        print (f"No images for stacking were found: {location}")
        print(error)
        return


def standardize_stacked_image(image_dir, target_dir, crop, standardization_procedure="layerwise", scaler_type="StandardScaler"):
    """Standardizes tif either layerwise or stackwise with the scikit-learn StandardScaler or RobustScaler.

    Parameters
    ----------
    image_dir: Path
        Path of stacked images which shall be standardized
    target_dir: Path
        Path where the standardized image shall be stored
    crop: String
        Location of the crop in latitude and longitude format as a string (e.g. "-68.148781_44.77383")
    standardization_procedure: String, optional
        Set the standardization precedure (default is "layerwise")
        "stackwise" = calculate mean and standard deviation based on the whole stack (10x400x400)
        "layerwise" = calculate mean and standard deviation based on each layer (400x400)
    scaler_type: String, optional
        Set desired scaler type (default is "StandardScaler")
        "StandardScaler" = standardizes the values by subtracting the mean and then scaling to unit variance.
        "Robustscaler" = transforms the values by subtracting the median and then dividing by the interquartile range (75% value — 25% value)
    """
    if image_dir.is_dir():

        image_path_VV = image_dir / "s1_stacked_VV.tif"
        image_path_VH = image_dir / "s1_stacked_VH.tif"

        if image_path_VV.exists():
            image_VV = rasterio.open(image_path_VV)
            profile = image_VV.profile
            standardized_image_VV = standardize_rasterio_image(image_VV, standardization_procedure, scaler_type)

        if image_path_VH.exists():
            image_VH = rasterio.open(image_path_VH)
            profile = image_VH.profile
            standardized_image_VH = standardize_rasterio_image(image_VH, standardization_procedure, scaler_type)

    output_dir = target_dir / crop
                
    try:
        output_dir.mkdir(exist_ok=True, parents=True)
    except OSError as error:
        print (f"Creation of the directory {output_dir} failed")
        print(error)
        return

    for polarization_type in ['s1_standardized_VV.tif', 's1_standardized_VH.tif']:
        with rasterio.open((output_dir / polarization_type), "w", **profile) as dest:
            if polarization_type == 's1_standardized_VV.tif':
                dest.write(standardized_image_VV)
            if polarization_type == 's1_standardized_VH.tif':
                dest.write(standardized_image_VH)


def standardize_rasterio_image(rasterio_image, standardization_procedure="layerwise", scaler_type="StandardScaler"):
    """Standardizes a rasterio image with a specific standardization procedure and scaler type:
    
    Parameters
    ----------
    rasterio_image: rasterio.open()
        Opened tif file with the rasterio package (rasterio.open("/path/to/image.tif"))
    standardization_procedure: String, optional
        Set the standardization precedure (default is "layerwise")
        "stackwise" = calculate mean and standard deviation based on the whole stack (10x400x400)
        "layerwise" = calculate mean and standard deviation based on each layer (400x400)
    scaler_type: String, optional
        Set desired scaler type (default is "StandardScaler")
        "StandardScaler" = standardizes the values by subtracting the mean and then scaling to unit variance.
        "Robustscaler" = transforms the values by subtracting the median and then dividing by the interquartile range (75% value — 25% value)
    """

    image_array = rasterio_image.read()
    num_layers, num_pixel_y, num_pixel_x = image_array.shape

    if standardization_procedure == "layerwise":

        standardized_image_array = None

        for band in image_array:

            if scaler_type == "StandardScaler":
                scaler = preprocessing.StandardScaler()
            elif scaler_type == "RobustScaler":
                scaler = preprocessing.RobustScaler()

            # Standardize each layer based on the mean/median and variance of each layer seperately
            if type(standardized_image_array) != numpy.ndarray:
                standardized_image_array = numpy.array(([scaler.fit_transform(band)]))
            else:
                standardized_image_array = numpy.concatenate((standardized_image_array, [scaler.fit_transform(band)]))

    if standardization_procedure == "stackwise":

        # In order to standardize the images based on the whole stack of images the dimension has to be reduced by 1
        # since the scaler only work for 2D arrays (e.g. reduce dimension from (5, 400, 400) to (400, 2000))
        image_array = numpy.reshape(image_array, newshape=(num_pixel_y, -1))

        if scaler_type == "StandardScaler":
            scaler = preprocessing.StandardScaler()
        elif scaler_type == "RobustScaler":
            scaler = preprocessing.RobustScaler()

        # Standardize the image stack based on the mean/median and variance of the whole stack
        standardized_image_array = scaler.fit_transform(image_array)

        # Reshape back to initial shape
        standardized_image_array = numpy.reshape(standardized_image_array, newshape=(num_layers, num_pixel_y, num_pixel_x))
    
    return standardized_image_array


def reduce_image_dimensionality(image_dir, image_name, target_dir, crop, dim_reduction_method):
    """Reduces the dimension of a stacked image by extracting the most relevant information of each layer:
    
    Parameters
    ----------
    image_dir: Path
        Path of sentinel 1 standardized images (VV and VH)
    image_name: String
        Name of the image which dimension shall be reduced ("s1_standardized_VV.tif" or "s1_standardized_VH.tif")
    target_dir: Path
        Default is source_dir with prefix "dim_reduced_"
    crop: String
        Location of the crop in latitude and longitude format as a string (e.g. "-68.148781_44.77383")
    dim_reduction_method: String, optional
        Set the method for dimension reduction (default is "max_values")
        "max_values" = The maximum value of each pixel from all layers is selected
        "pca" = Principal Component Analysis, simplify data with a small amount of linear components. 
            If pca is chosen as dim_reduction_method "otbApplication" has to be installed => check import otbApplication info.
    """

    output_dir = target_dir / crop
    if image_name == "s1_standardized_VV.tif":
        new_image_name = "s1_dim_reduced_VV.tif"
    elif image_name == "s1_standardized_VH.tif":
        new_image_name = "s1_dim_reduced_VH.tif"

    try:

        output_dir.mkdir(exist_ok=True, parents=True)

    except OSError as error:

        print (f"Creation of the directory {output_dir} failed")
        print(error)
        return

    else:

        if dim_reduction_method == "max_values":

            image = rasterio.open((image_dir / image_name))
            profile = image.profile
            # Update the profile from x layers to 1
            profile.update({"count":1})

            # Read image as numpy array
            image_array = image.read()

            # Select the maximum value of each pixel from all layers
            # The dimension is kept but reduced to one (1, 400, 400)
            max_values_image_array = numpy.amax(image_array, axis=0, keepdims=True)

            with rasterio.open((output_dir / new_image_name), "w", **profile) as dest:
                dest.write(max_values_image_array)

        elif dim_reduction_method == "pca":

            # check if otbApplication is installed since this is required for the PCA
            if "otbApplication" not in sys.modules:
                print("'pca' was chosen as dimensionality reduction method but otbApplication is not installed!")
                print("Please, install otbApplication on your machine or use 'max_values' as dimensionality reduction method!")
                return

            app = otbApplication.Registry.CreateApplication("DimensionalityReduction")

            app.SetParameterString("in", str(image_dir / image_name))
            app.SetParameterString("out", str(output_dir / new_image_name))
            app.SetParameterString("method", dim_reduction_method)
            # nbcomp = Number of components, therefore it will be reduced to one component (from (x, 400, 400) to (1, 400, 400))
            app.SetParameterInt("nbcomp", 1)

            try:
                app.ExecuteAndWriteOutput()
            except:
                logger.error("Error while creating pca image.")
                logger.error(sys.exc_info()[0])        

        else:

            print("Please, choose a valid dim_reduction_method ('pca' or 'max_values')!")
            return


def shift_images_reference_based(image_dict, target_dir, crop, target_pixel_size, reference_dir):

    try:
        lon, lat = crop.split("_")
        reference_crop = list(reference_dir.glob(f"*_{lon}*_{lat}*"))[0]
        shift_info = reference_crop.name.rsplit("_", maxsplit=1)[1]
    except:
        print(f"Could not find reference crop. Skipping crop: {crop}!")
        return

    # example for shift_info: x+6y-4
    shift_x = int(shift_info.split("y")[0][1:])
    shift_y = int(shift_info.split("y")[1])

    original_image_shape = image_dict[list(image_dict.keys())[0]].shape

    # Calculate the center point of the original image
    center_point_y, center_point_x = [int(round(index/2)) for index in original_image_shape]

    # Calculate the x- and y-indices for the crop with the randomly generated shift in x- and y-direction
    top_left_x = int((center_point_x - (target_pixel_size/2)) + shift_x)
    top_left_y = int((center_point_y - (target_pixel_size/2)) + shift_y)
    bottom_right_x = int((center_point_x + (target_pixel_size/2)) + shift_x)
    bottom_right_y = int((center_point_y + (target_pixel_size/2)) + shift_y)

    cropped_image_dict = {}
    # Crop the shifted image from the original image and save it to cropped_image_dict
    for image_name, image in image_dict.items():
        cropped_image_dict[image_name] = image[top_left_y:bottom_right_y, top_left_x:bottom_right_x]

    # Add + in front of positive shifts for the folder name
    if shift_x >= 0:
        shift_x = "+" + str(shift_x)
    if shift_y >= 0:
        shift_y = "+" + str(shift_y)
    
    output_dir = target_dir / (crop + "_" + str(target_pixel_size) + f"_x{shift_x}y{shift_y}")

    try:
        output_dir.mkdir(exist_ok=True, parents=True)

    except OSError as error:
        print (f"Creation of the directory {output_dir} failed")
        print(error)
        return
    
    for image_name, image in cropped_image_dict.items():
        imsave((output_dir / image_name), image)    



def shift_images_randomly(image_dict, target_dir, crop, target_pixel_size, shifting_limit):
    """Creates randomly shifted crops around the center of the original images in image_dict.
    
    Parameters
    ----------
    image_dict: Dictionary
        Contains image names as keys and the associated images opened with rasterio as values
    target_dir: String
        Default is the parent of source_dir and the folder is named "source-dir_shifting-limit_target-pixel-size"
    crop: String
        Location of the crop in latitude and longitude format as a string (e.g. "-68.148781_44.77383")
    target_pixel_size: Int
        Set the pixel size of the crops which shall be extracted from the original image in source_dir
        (Default is 50)
    shifting_limit: Float
        Set the limit on how much the image shall be shifted (Choose a value between 0 and 1).
        0: Image won't be shifted
        1: Image is shifted up to 100%. Therefore, the initial center of the original image appears at the edge of the cropped image.
    """

    original_image_shape = image_dict[list(image_dict.keys())[0]].shape

    # Calculate the center point of the original image
    center_point_y, center_point_x = [int(round(index/2)) for index in original_image_shape]

    # Define the maximum permitted shift and calculate a random number for the x- and y-axis inside the defined maximum.
    max_shift = int(round((target_pixel_size/2)*shifting_limit))

    if (target_pixel_size + max_shift) > original_image_shape[0]:
        print(f"Your chosen target_pixel_size ({target_pixel_size}) combined with the shifting_limit ({shifting_limit}) would exceed the original image {original_image_shape}.")
        print("Please choose a target_pixel_size and shifting_limit that won't exceed the original image size!")
        return

    random_shift_x = random.randint(-max_shift,max_shift)
    random_shift_y = random.randint(-max_shift,max_shift)
    
    # Calculate the x- and y-indices for the crop with the randomly generated shift in x- and y-direction
    top_left_x = int((center_point_x - (target_pixel_size/2)) + random_shift_x)
    top_left_y = int((center_point_y - (target_pixel_size/2)) + random_shift_y)
    bottom_right_x = int((center_point_x + (target_pixel_size/2)) + random_shift_x)
    bottom_right_y = int((center_point_y + (target_pixel_size/2)) + random_shift_y)

    cropped_image_dict = {}
    # Crop the shifted image from the original image and save it to cropped_image_dict
    for image_name, image in image_dict.items():
        cropped_image_dict[image_name] = image[top_left_y:bottom_right_y, top_left_x:bottom_right_x]

    # Add + in front of positive shifts for the folder name
    if random_shift_x >= 0:
        random_shift_x = "+" + str(random_shift_x)
    if random_shift_y >= 0:
        random_shift_y = "+" + str(random_shift_y)
    
    output_dir = target_dir / (crop + "_" + str(target_pixel_size) + f"_x{random_shift_x}y{random_shift_y}")

    try:
        output_dir.mkdir(exist_ok=True, parents=True)

    except OSError as error:
        print (f"Creation of the directory {output_dir} failed")
        print(error)
        return
    
    for image_name, image in cropped_image_dict.items():
        imsave((output_dir / image_name), image)


def compare_stacked_and_reduced_images(target_dir, channels, rows_per_preview, source_dir_stacked=None, source_dir_pca=None, source_dir_max=None, \
                                       image_height_limit=None, image_width_limit=None):

    target_dir = pathlib.Path(target_dir)
    try:
        target_dir.mkdir(exist_ok=True, parents=True)
    except OSError as error:
        print (f"Creation of the directory {target_dir} failed")
        print(error)
        return    

    if not isinstance(source_dir_stacked, type(None)):
        source_dir_stacked = pathlib.Path(source_dir_stacked)
    if not isinstance(source_dir_pca, type(None)):
        source_dir_pca = pathlib.Path(source_dir_pca)
    if not isinstance(source_dir_max, type(None)):
        source_dir_max = pathlib.Path(source_dir_max)

    # determine max number of layers for stacked images
    max_layers = 0
    if not isinstance(source_dir_stacked, type(None)):
        print("Determine max. number of layers for stacked images...")
        for stack in tqdm(source_dir_stacked.glob("*"), desc="Reading stacked images: "):
            stack_file = stack / "s1_stacked_VV.tif"
            if stack_file.exists():
                image = rasterio.open(stack_file)
                layers = image.profile["count"]
                if layers > max_layers:
                    max_layers = layers
        print(f"Max. number of layers: {max_layers}")

    # determine image size (read first image of every source dir and take the max size)
    max_width = 0
    max_height = 0
    print("Determine max. image size...")
    for folder in [source_dir_stacked, source_dir_pca, source_dir_max]:
        if not isinstance(folder, type(None)):    
            items = list(folder.glob("*"))
            item = items[0]
            for file in ["s1_stacked_VV.tif", "s1_dim_reduced_VV.tif"]:
                image_file = item / file
                if image_file.exists():
                    image = rasterio.open(stack_file)
                    width = image.profile["width"]
                    height = image.profile["height"]
                    if width > max_width:
                        max_width = width        
                    if height > max_height:
                        max_height = height

    if not isinstance(image_width_limit, type(None)) and image_width_limit < max_width:
        max_width = image_width_limit
    if not isinstance(image_height_limit, type(None)) and image_height_limit < max_height:
        max_height = image_height_limit

    print(f"Max width: {max_width}")
    print(f"Max height: {max_height}")

    combined_preview_width = config.previewBorder + ( max_width + config.previewBorder ) * max_layers
    if not isinstance(source_dir_pca, type(None)):
        combined_preview_width = combined_preview_width + max_width + config.previewBorder
    if not isinstance(source_dir_max, type(None)):
        combined_preview_width = combined_preview_width + max_width + config.previewBorder

    combined_preview_height = config.previewBorder + ( max_height + config.previewBorder ) * rows_per_preview

    # create unique lon lat set
    lon_lat_set = set()
    for folder in [source_dir_stacked, source_dir_pca, source_dir_max]:
        if not isinstance(folder, type(None)):    
            old_count = len(lon_lat_set)
            lon_lat_set = get_unique_lon_lat_set(source_dir=folder, has_subdir=False, lon_lat_set=lon_lat_set, lon_position=0, lat_position=1)
            new_count = len(lon_lat_set)
            print(f"Total positions: {new_count} New positions: {new_count - old_count}")

    # create empty image
    combined_image = numpy.full((combined_preview_height, combined_preview_width, 3), config.previewBackground)
    combined_image_id = 1
    written_images = 0
    text = []

    # loop through lon lat set
    for location in tqdm(lon_lat_set, desc="Fetching previews and writing images: "):

        lon, lat = location
        left_upper_corner_h = config.previewBorder + written_images * ( max_height + config.previewBorder )

        text.append(f"lon:{lon} lat:{lat}")

        # write images into array

        # write stack images
        if not isinstance(source_dir_stacked, type(None)) and max_layers > 0:

            image_path_list = list(source_dir_stacked.glob(f"*{lon}*_{lat}*"))

            # should only be one path...
            if len(image_path_list) > 0:
            
                image_path = image_path_list[0]
                layer_values = None

                for channel in channels:

                    stack_file = image_path / f"s1_stacked_{channel}.tif"
                    if stack_file.exists():

                        image = rasterio.open(stack_file)
                        if image.profile["width"] > max_width:
                            image_width_start = math.floor(image.profile["width"] / 2) - math.floor(max_width / 2)
                            image_width_end = image_width_start + max_width
                        else:
                            image_width_start = 0
                            image_width_end = image.profile["width"]
                        image_width = image_width_end - image_width_start
                        if image.profile["height"] > max_height:                            
                            image_height_start = math.floor(image.profile["height"] / 2) - math.floor(max_height / 2)
                            image_height_end = image_height_start + max_height
                        else:
                            image_height_start = 0
                            image_height_end = image.profile["height"]
                        image_height = image_height_end - image_height_start

                        # loop through layers
                        layers = image.profile["count"]
                        for layer in range(1,layers+1):

                            left_upper_corner_w = config.previewBorder + ( layer - 1 ) * ( max_width + config.previewBorder )

                            layer_values = image.read(layer)

                            # write array to preview image (RGB if channels len is 1, otherwise R for VV and G for VH)
                            if len(channels) == 1:

                                combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                               left_upper_corner_w:left_upper_corner_w+image_width, \
                                               0] = layer_values[image_height_start:image_height_end, image_width_start:image_width_end]
                                combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                               left_upper_corner_w:left_upper_corner_w+image_width, \
                                               1] = layer_values[image_height_start:image_height_end, image_width_start:image_width_end]
                                combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                               left_upper_corner_w:left_upper_corner_w+image_width, \
                                               2] = layer_values[image_height_start:image_height_end, image_width_start:image_width_end]

                            else:

                                if channel == "VV":
                                    combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                                   left_upper_corner_w:left_upper_corner_w+image_width, \
                                                   0] = layer_values[image_height_start:image_height_end, image_width_start:image_width_end]
                                elif channel == "VH":
                                    combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                                   left_upper_corner_w:left_upper_corner_w+image_width, \
                                                   1] = layer_values[image_height_start:image_height_end, image_width_start:image_width_end]

                # stretch images to 8-bit linearly
                for layer in range(1,max_layers+1):

                    # use the shape of the last layer_values to get the appropriate image size
                    if not isinstance(layer_values, type(None)):
                        if len(channels) == 1:
                            colors = 3
                        else:
                            colors = 2
                        left_upper_corner_w = config.previewBorder + ( layer - 1 ) * ( max_width + config.previewBorder )
                        subimage = combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                                  left_upper_corner_w:left_upper_corner_w+image_width, \
                                                  :colors]
                        stretched_subimage = stretch_image_values_linearly_8bit(subimage)
                        combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                       left_upper_corner_w:left_upper_corner_w+image_width, \
                                       :colors] = stretched_subimage

        # write pca image
        if not isinstance(source_dir_pca, type(None)):

            image_path_list = list(source_dir_pca.glob(f"*{lon}*_{lat}*"))

            # should only be one path...
            if len(image_path_list) > 0:
            
                image_path = image_path_list[0]

                left_upper_corner_w = config.previewBorder + max_layers * ( max_width + config.previewBorder )
                image_values = None

                for channel in channels:

                    if (image_path / f"s1_dim_reduced_{channel}.tif").exists():

                        image = rasterio.open((image_path / f"s1_dim_reduced_{channel}.tif").absolute())

                        if image.profile["width"] > max_width:
                            image_width_start = math.floor(image.profile["width"] / 2) - math.floor(max_width / 2)
                            image_width_end = image_width_start + max_width
                        else:
                            image_width_start = 0
                            image_width_end = image.profile["width"]
                        image_width = image_width_end - image_width_start
                        if image.profile["height"] > max_height:                            
                            image_height_start = math.floor(image.profile["height"] / 2) - math.floor(max_height / 2)
                            image_height_end = image_height_start + max_height
                        else:
                            image_height_start = 0
                            image_height_end = image.profile["height"]
                        image_height = image_height_end - image_height_start

                        image_values = image.read(1)

                        # write array to preview image (RGB if channels len is 1, otherwise R for VV and G for VH)
                        if len(channels) == 1:

                            combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                           left_upper_corner_w:left_upper_corner_w+image_width, \
                                           0] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]
                            combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                           left_upper_corner_w:left_upper_corner_w+image_width, \
                                           1] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]
                            combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                           left_upper_corner_w:left_upper_corner_w+image_width, \
                                           2] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]

                        else:

                            if channel == "VV":
                                combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                               left_upper_corner_w:left_upper_corner_w+image_width, \
                                               0] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]
                            elif channel == "VH":
                                combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                               left_upper_corner_w:left_upper_corner_w+image_width, \
                                               1] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]                                               

                # stretch images to 8-bit linearly
                # use the shape of the last image_values to get the appropriate image size
                if not isinstance(image_values, type(None)):
                    if len(channels) == 1:
                        colors = 3
                    else:
                        colors = 2                
                    subimage = combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                              left_upper_corner_w:left_upper_corner_w+image_width, \
                                              :colors]
                    stretched_subimage = stretch_image_values_linearly_8bit(subimage)
                    combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                   left_upper_corner_w:left_upper_corner_w+image_width, \
                                   :colors] = stretched_subimage                                          

        # write max image        
        if not isinstance(source_dir_max, type(None)):

            image_path_list = list(source_dir_max.glob(f"*{lon}*_{lat}*"))

            # should only be one path...
            if len(image_path_list) > 0:
            
                image_path = image_path_list[0]

                # TODO: this is not a nice solution... 
                if not isinstance(source_dir_pca, type(None)):
                    left_upper_corner_w = config.previewBorder + ( max_layers + 1 ) * ( max_width + config.previewBorder )
                else:
                    left_upper_corner_w = config.previewBorder + max_layers * ( max_width + config.previewBorder )
                image_values = None

                for channel in channels:

                    if (image_path / f"s1_dim_reduced_{channel}.tif").exists():

                        image = rasterio.open((image_path / f"s1_dim_reduced_{channel}.tif").absolute())

                        if image.profile["width"] > max_width:
                            image_width_start = math.floor(image.profile["width"] / 2) - math.floor(max_width / 2)
                            image_width_end = image_width_start + max_width
                        else:
                            image_width_start = 0
                            image_width_end = image.profile["width"]
                        image_width = image_width_end - image_width_start
                        if image.profile["height"] > max_height:                            
                            image_height_start = math.floor(image.profile["height"] / 2) - math.floor(max_height / 2)
                            image_height_end = image_height_start + max_height
                        else:
                            image_height_start = 0
                            image_height_end = image.profile["height"]
                        image_height = image_height_end - image_height_start

                        image_values = image.read(1)

                        # write array to preview image (RGB if channels len is 1, otherwise R for VV and G for VH)
                        if len(channels) == 1:

                            combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                           left_upper_corner_w:left_upper_corner_w+image_width, \
                                           0] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]
                            combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                           left_upper_corner_w:left_upper_corner_w+image_width, \
                                           1] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]
                            combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                           left_upper_corner_w:left_upper_corner_w+image_width, \
                                           2] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]

                        else:

                            if channel == "VV":
                                combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                               left_upper_corner_w:left_upper_corner_w+image_width, \
                                               0] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]
                            elif channel == "VH":
                                combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                               left_upper_corner_w:left_upper_corner_w+image_width, \
                                               1] = image_values[image_height_start:image_height_end, image_width_start:image_width_end]

                # stretch images to 8-bit linearly
                # use the shape of the last image_values to get the appropriate image size
                if not isinstance(image_values, type(None)):
                    if len(channels) == 1:
                        colors = 3
                    else:
                        colors = 2                
                    subimage = combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                              left_upper_corner_w:left_upper_corner_w+image_width, \
                                              :colors]
                    stretched_subimage = stretch_image_values_linearly_8bit(subimage)
                    combined_image[left_upper_corner_h:left_upper_corner_h+image_height, \
                                   left_upper_corner_w:left_upper_corner_w+image_width, \
                                   :colors] = stretched_subimage

        written_images = written_images + 1

        if written_images == rows_per_preview:
            
            # save image

            target_file = target_dir / f"comparison_{combined_image_id}.tif"
            image = Image.fromarray(numpy.uint8(combined_image))

            # write text
            font = ImageFont.truetype(str(
                pathlib.Path(os.environ["CONDA_PREFIX"]) / "fonts" / "open-fonts" / "IBMPlexMono-Regular.otf"), config.previewImageFontSize)
            draw = ImageDraw.Draw(image)
            for row in range(rows_per_preview):
                position_x = config.previewBorder
                position_y = config.previewBorder + row * ( max_height + config.previewBorder )
                draw.text((position_x, position_y), text[row], font=font, fill=(255, 0, 0))
                position_x = position_x + max_layers * ( max_width + config.previewBorder )
                draw.text((position_x, position_y), "PCA", font=font, fill=(255, 0, 0))
                position_x = position_x + max_width + config.previewBorder
                draw.text((position_x, position_y), "MAX", font=font, fill=(255, 0, 0))

            image.save(target_file)

            combined_image = numpy.full((combined_preview_height, combined_preview_width, 3), config.previewBackground)
            combined_image_id = combined_image_id + 1
            written_images = 0
            text = []


def stretch_image_values_linearly_8bit(arr):

    if not (len(arr.shape) == 3) or arr.shape[0] == 0:
        return None

    min_value = numpy.amin(arr)
    max_value = numpy.amax(arr)

    target_min = 0
    target_max = 255
    factor = target_max / ( max_value - min_value )

    arr = (arr - min_value) * factor

    # Round to nearest integer towards zero.
    arr = numpy.fix(arr)

    return arr


def copy_and_number_previews(source_dir, target_dir):

    source_dir = pathlib.Path(source_dir)
    target_dir = pathlib.Path(target_dir)

    counter = 0

    for crop in source_dir.glob("*"):

        if not crop.name.startswith("0_") and ( crop / "preview.tif" ).exists():

            counter = counter + 1

            old_file = crop / "preview.tif"
            new_file = target_dir / f"{counter}.tif"

            shutil.copy(old_file, new_file)

    print(f"{counter} previews copied.")
