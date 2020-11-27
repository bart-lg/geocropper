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
import random
from shapely.geometry import Point
from shapely.geometry import Polygon
from shapely.ops import transform
from dateutil.parser import *
from functools import partial
import zipfile
import tarfile
from tqdm import tqdm
import subprocess
import sys
from datetime import datetime

import geocropper.config as config
from geocropper.database import database
import geocropper.sentinelWrapper as sentinelWrapper
import geocropper.asfWrapper as asfWrapper

import logging

from osgeo import gdal
# gdal library distributed by conda destroys PATH environment variable
# see -> https://github.com/OSGeo/gdal/issues/1231
# workaround: remove first entry...
os.environ["PATH"] = os.environ["PATH"].split(';')[1]

# get logger object
logger = logging.getLogger('root')
db = database()


def convertDate(date, newFormat="%Y-%m-%d"):
    temp = parse(date)
    return temp.strftime(newFormat)


def dateOlderThan24h(date):
    then = datetime.fromisoformat(date)
    now = datetime.now()
    duration = now - then 
    duration_in_s = duration.total_seconds()
    hours = divmod(duration_in_s, 3600)[0]
    if hours >= 24:
        return True
    else:
        return False


def minutesSinceLastDownloadRequest():
    now = datetime.now()
    then = db.getLatestDownloadRequest()
    if then != None:
        then = datetime.fromisoformat(str(then))
        duration = now - then
        duration_in_s = duration.total_seconds()
        minutes = divmod(duration_in_s, 60)[0]
        return int(minutes)
    else:
        return None        


def getXYCornerCoordinates(path, lat, lon, width, height):

    poi_transformed = transformPointLatLonToXY(path, Point(lon, lat))
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


def transformPointLatLonToXY(path, point):

    # open raster image file
    img = rasterio.open(str(path))

    # prepare parameters for coordinate system transform function
    toTargetCRS = partial(pyproj.transform,
        pyproj.Proj('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs '), pyproj.Proj(img.crs))

    # transform corner coordinates for cropping
    point = transform(toTargetCRS, point)

    return(point)


def transformPointXYToLatLon(path, point):
    img = rasterio.open(str(path))
    inProj = pyproj.Proj(img.crs)
    lat, lon = inProj(point.x, point.y, inverse=True)
    return({"lat": lat, "lon": lon})


def cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat, isLatLon = True):

    if (isLatLon):
        topLeft = transformPointLatLonToXY(path, topLeft)
        bottomRight = transformPointLatLonToXY(path, bottomRight)

    # open image with GDAL
    ds = gdal.Open(str(path))

    # make sure that target directory exists
    if not os.path.isdir(str(targetDir)):
        os.makedirs(str(targetDir))

    # CROP IMAGE
    ds = gdal.Translate(str(targetDir / item), ds, format=fileFormat,
                        projWin=[topLeft.x, topLeft.y,
                                 bottomRight.x, bottomRight.y])

    ds = None


def createPreviewRGBImage(r_band_search_pattern, g_band_search_pattern, b_band_search_pattern, source_dir,
                          target_dir, max_scale=4095, exponential_scale=0.5):

    search_result = list(source_dir.glob(r_band_search_pattern))
    if len(search_result) == 0:
        return
    r_band = search_result[0]

    search_result = list(source_dir.glob(g_band_search_pattern))
    if len(search_result) == 0:
        return
    g_band = search_result[0]

    search_result = list(source_dir.glob(b_band_search_pattern))
    if len(search_result) == 0:
        return
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
    (target_dir / "r-scaled.tif").unlink()
    (target_dir / "g-scaled.tif").unlink()
    (target_dir / "b-scaled.tif").unlink()

    if config.resizePreviewImage:
        image = Image.open(str(target_dir / preview_file))
        small_image = image.resize((config.widthPreviewImageSmall, config.heightPreviewImageSmall), Image.ANTIALIAS)
        small_image.save(str(target_dir / preview_file_small))


def createPreviewRGImage(file, target_dir, min_scale=-30, max_scale=30, exponential_scale=0.5):
    """Creates a RGB preview file out of an db scaled image with only 2 bands.
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
              f"{str(exp_option)} {os.path.realpath(str(file))} {os.path.realpath(str(target_dir / 'r-scaled.tif'))}"        
    subprocess.call(command, shell=True)    

    command = f"gdal_translate -b 2 -q -ot Byte -scale {min_scale} {max_scale} 0 255 " + \
              f"{str(exp_option)} {os.path.realpath(str(file))} {os.path.realpath(str(target_dir / 'g-scaled.tif'))}"            
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
    (target_dir / "r-scaled.tif").unlink()
    (target_dir / "g-scaled.tif").unlink()
    (target_dir / "b-empty.tif").unlink()

    if config.resizePreviewImage:
        image = Image.open(str(target_dir / preview_file))
        small_image = image.resize((config.widthPreviewImageSmall, config.heightPreviewImageSmall), Image.ANTIALIAS)
        small_image.save(str(target_dir / preview_file_small))      


def concat_images(image_path_list, output_file, gap=3, bcolor=(0, 0, 0), paths_to_file=None,
                  upper_label_list=None, lower_label_list=None, write_image_text=True, center_point=False):
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
    upper_label_list : list, optional
        if defined the contained labels will be written on the image
        upper label will be written in first line
        instead of the paths of the individual images
    lower_label_list : list, optional
        if defined the contained labels will be written on the image 
        lower label will be written in second line
        instead of the paths of the individual images
    write_image_text : boolean, optional
        write paths or labels on image
        default is True
    center_point : boolean, optional
        marks the center of the individual preview images with a red dot
        default is False

    """

    # determine needed raster size
    raster_size = math.ceil(math.sqrt(len(image_path_list)))

    # determine max heigth and max width of all images
    max_height = 0
    max_width = 0

    for image_path in image_path_list:
        image = pyplot.imread(image_path)[:, :, :3]
        height, width = image.shape[:2]
        if height > max_height:
            max_height = height
        if width > max_width:
            max_width = width

    # add gap to width and height
    total_height = max_height * raster_size + gap * (raster_size - 1)
    total_width = max_width * raster_size + gap * (raster_size - 1)

    # assign positions to images
    # positions = [row, column, height_start, width_start]
    positions = numpy.zeros((len(list(image_path_list)), 4), dtype=int)
    for i, image_path in enumerate(image_path_list, 1):
        # determine position
        row = math.ceil(i / raster_size)
        column = i % raster_size
        if column == 0:
            column = raster_size

        # determine starting width and height
        height_start = (row - 1) * (max_height + gap)
        width_start = (column - 1) * (max_width + gap)

        positions[i-1][0] = int(row)
        positions[i-1][1] = int(column)
        positions[i-1][2] = int(height_start)
        positions[i-1][3] = int(width_start)

    # create empty image
    combined_image = numpy.full((total_height, total_width, 3), bcolor)

    # paste images to combined image
    for i, image_path in enumerate(image_path_list):

        # read image
        image = pyplot.imread(image_path)[:, :, :3]

        # determine width and height
        height, width = image.shape[:2]

        # paste image
        combined_image[positions[i][2]:(positions[i][2]+height), positions[i][3]:(positions[i][3]+width)] = image

        # center point
        if center_point:
            combined_image[positions[i][2] + round(height / 2), positions[i][3] + round(width / 2)] = (255, 0, 0)

    # write file
    image = Image.fromarray(numpy.uint8(combined_image))

    # write paths on image
    if write_image_text:
        font = ImageFont.truetype(str(
            pathlib.Path(os.environ["CONDA_PREFIX"]) / "fonts" / "open-fonts" / "IBMPlexMono-Regular.otf"), config.previewImageFontSize)
        draw = ImageDraw.Draw(image)
        if upper_label_list == None:
            upper_label_list = image_path_list
        for i, upper_label in enumerate(upper_label_list):
            # draw.text requires coordinates in following order: width, height
            draw.text((positions[i][3] + 5, positions[i][2] + 5), upper_label, font=font, fill=(255, 0, 0))
        if lower_label_list != None:
            for i, lower_label in enumerate(lower_label_list):
                # draw.text requires coordinates in following order: width, height
                draw.text((positions[i][3] + 5, positions[i][2] + 15), lower_label, font=font, fill=(255, 0, 0))

    image.save(output_file)

    # create file list
    if paths_to_file != None:
        file = open(paths_to_file, "w+")
        for i, image_path in enumerate(image_path_list, 1):
            position = i % raster_size
            if position != 1:
                file.write("\t")
            file.write(str(image_path))
            if position == 0:
                file.write("\r\n")


def createCombinedImages(source_folder):

    counter = 0
    image_path_list = []
    upper_label_list = []
    lower_label_list = []

    item_list = list(source_folder.glob("*"))

    combined_preview_folder = source_folder / "0_combined-preview"
    combined_preview_folder.mkdir(exist_ok=True)

    for i, item in enumerate(source_folder.glob("*"), 1):

        preview_file = item / "preview.tif"

        if preview_file.exists():

            image_path_list.append(preview_file)
            upper_label_list.append(item.name.split("_")[0])
            # lower_label_list.append(item.parent.name)

        if i % config.previewImagesCombined == 0 or i == len(item_list):

            counter = counter + 1

            output_file = combined_preview_folder / ("combined-preview-" + str(counter) + ".tif")
            summary_file = combined_preview_folder / ("combined-preview-" + str(counter) + "-paths.txt")

            concat_images(image_path_list, output_file, gap=config.previewBorder,
                          bcolor=config.previewBackground, paths_to_file=summary_file,
                          upper_label_list=upper_label_list, write_image_text=config.previewTextOnImage, 
                          center_point=config.previewCenterDot)

            image_path_list = []
            upper_label_list = []
            # lower_label_list = []


def combineImages(folder="", has_subdir=True):

    for group in config.croppedTilesDir.glob("*"):

        if len(folder) == 0 or (len(folder) > 0 and folder == str(group.name)):

            if has_subdir:
                for request in group.glob("*"):
                    createCombinedImages(request)
            else:
                createCombinedImages(group)


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
        fileFormat = "JP2OpenJPEG"

        # go through "SAFE"-directory structure of Sentinel-2

        pathGranule = big_tile / "GRANULE"
        for mainFolder in os.listdir(pathGranule):

            pathImgData = pathGranule / mainFolder / "IMG_DATA"

            HQ_dir = pathImgData / "R10m"
            # Level-1 currently not supported
            # HQ_dir only exists on Level-2 data
            if HQ_dir.exists():

                # open image with GDAL
                file = list(HQ_dir.glob("*_B02_10m.jp2"))[0]
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

                    for file in HQ_dir.glob("*_B*_10m.jp2"):
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
                        topLeft = Point(top_left_x, top_left_y)
                        bottomRight = Point(bottom_right_x, bottom_right_y)

                        file = list(HQ_dir.glob("*_B02_10m.jp2"))[0]
                        point = transformPointXYToLatLon(file, Point(random_x, random_y))

                        mainTargetFolder = output_path / ("%s_%s_%s" % (j, point["lat"], point["lon"]))

                        # target directory for cropped image
                        targetDir = mainTargetFolder / "sensordata"
                        targetDir.mkdir(parents=True, exist_ok=True)

                        # target directory for meta information
                        metaDir = mainTargetFolder / "original-metadata"

                        # target directory for preview image
                        previewDir = mainTargetFolder

                        for imgDataItem in os.listdir(pathImgData):

                            pathImgDataItem = pathImgData / imgDataItem

                            targetSubDir = targetDir / imgDataItem

                            if os.path.isdir(pathImgDataItem):

                                for item in os.listdir(pathImgDataItem):

                                    # set path of img file
                                    path = pathImgDataItem / item

                                    # CROP IMAGE
                                    cropImg(path, item, topLeft, bottomRight, targetSubDir, fileFormat, isLatLon=False)

                        createPreviewRGBImage("*B04_10m.jp2", "*B03_10m.jp2", "*B02_10m.jp2", targetSubDir, previewDir)

                        if config.copyMetadata:
                            metaDir.mkdir(parents=True)
                            tileDir = big_tile
                            for item in tileDir.rglob('*'):
                                if item.is_file() and item.suffix.lower() != ".jp2":
                                    targetDir = metaDir / item.parent.relative_to(tileDir)
                                    if not targetDir.exists():
                                        targetDir.mkdir(parents=True)
                                    shutil.copy(item, targetDir)

                        if config.createSymlink:
                            tileDir = big_tile
                            # TODO: set config parameter for realpath or relpath for symlink
                            metaDir.symlink_to(os.path.realpath(str(tileDir.resolve())), str(metaDir.parent.resolve()))

                        print(f"random crop {j} done.\n")

    print("### Generate combined preview images...")
    combineImages(str(output_path.name), has_subdir=False)
    print("done.")


def trim_crops(source_dir, target_dir, width, height):
    """Trim crops to smaller size. Reference point is the center of the image. 
    
    All directories in source_dir will be copied to target_dir 
    and all .jp2 files within that directories will be trimmed to new size. 
    The preview images will be deleted and new preview images will be created.
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

    # check if source_dir exists, exit if no
    if not os.path.isdir(str(source_dir)):
        sys.exit("ERROR trim_crops: source_dir is not a directory!")

    # check if target_dir exists, exit if yes
    if os.path.exists(str(target_dir)):
        sys.exit("ERROR trim_crops: target_dir already exists!")

    # create target_dir
    os.makedirs(str(target_dir))

    # convert to pathlib objects
    source_dir = pathlib.Path(source_dir)
    target_dir = pathlib.Path(target_dir)

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
            shutil.copytree(folder_in, folder_out, symlinks = True, copy_function=shutil.copy)

            # chmod to 664 for files and 775 for dirs
            dir_mod = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
            file_mod = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH
            os.chmod(folder_out, dir_mod)
            for item in folder_out.rglob("*"):
                if item.is_dir():
                    os.chmod(item, dir_mod)                    
                else:
                    os.chmod(item, file_mod)                    

            # get all files of subfolder
            for file in folder_out.rglob("*"):

                # trim files with matching suffix
                if file.suffix in file_types:

                    if file.suffix == ".jp2":

                        # Sentinel-2 img data are in jp2-format
                        # set appropriate format for GDAL lib
                        fileFormat = "JP2OpenJPEG"                        

                        # open image with GDAL
                        img = gdal.Open(str(file))

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
                        img = gdal.Translate(str(file) + "_new", img, format=fileFormat,
                                            projWin=[top_left_x, top_left_y,
                                                     bottom_right_x, bottom_right_y])                      

                        # save and replace

                        img = None

                        file.unlink()
                        file_new = pathlib.Path(str(file) + "_new")
                        file_new.rename(file)


                    # remove old preview files
                    if file.suffix == ".tif":

                        if file.name == "preview.tif":
                            # remove file
                            file.unlink()  

            # create preview image
            createPreviewRGBImage("*B04_10m.jp2", "*B03_10m.jp2", "*B02_10m.jp2", folder_out / "sensordata" / "R10m", folder_out)   


    # create combined previews
    createCombinedImages(target_dir)


def moveLatLon(lat, lon, azimuth, distance):
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


def getLatLonCornerCoordinates(lat, lon, width, height):
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
    lat_temp, lon_temp = moveLatLon(lat, lon, 270, (width / 2))
    top_left_lat, top_left_lon = moveLatLon(lat_temp, lon_temp, 0, (height / 2))

    # determine top right corner
    lat_temp, lon_temp = moveLatLon(lat, lon, 90, (width / 2))
    top_right_lat, top_right_lon = moveLatLon(lat_temp, lon_temp, 0, (height / 2))    

    # determine bottom right corner
    lat_temp, lon_temp = moveLatLon(lat, lon, 90, (width / 2))
    bottom_right_lat, bottom_right_lon = moveLatLon(lat_temp, lon_temp, 180, (height / 2))

    # determine bottom right corner
    lat_temp, lon_temp = moveLatLon(lat, lon, 270, (width / 2))
    bottom_left_lat, bottom_left_lon = moveLatLon(lat_temp, lon_temp, 180, (height / 2))    

    return ([ Point(top_left_lon, top_left_lat), 
              Point(top_right_lon, top_right_lat),
              Point(bottom_right_lon, bottom_right_lat),
              Point(bottom_left_lon, bottom_left_lat),
              Point(top_left_lon, top_left_lat)
              ])


def unpackBigTiles():

    logger.info("start of unpacking tile zip/tar files")
    
    print("\nUnpack big tiles:")
    print("-----------------\n")

    # determine number of zip files        
    filesNumZip = len([f for f in os.listdir(config.bigTilesDir) 
         if f.endswith('.zip') and os.path.isfile(os.path.join(config.bigTilesDir, f))])

    # determine number of tar files
    filesNumTar = len([f for f in os.listdir(config.bigTilesDir) 
         if f.endswith('.tar.gz') and os.path.isfile(os.path.join(config.bigTilesDir, f))])

    # calculate number of total packed files
    filesNum = filesNumZip + filesNumTar

    # index i serves as a counter
    i = 1
    

    # start unpacking

    for item in os.listdir(config.bigTilesDir):

        if item.endswith(".zip") or item.endswith(".tar.gz"):
        
            print("[%d/%d] %s:" % (i, filesNum, item))

            # get path of the packed file
            filePath = config.bigTilesDir / item

            # unpack zip file if zip
            if item.endswith(".zip"):

                # TODO: dirty... (is maybe first entry of zipRef)
                # get tile by folder name
                newFolderName = item[:-4] + ".SAFE"
                tile = db.getTile(folderName = newFolderName)              

                # unzip
                with zipfile.ZipFile(file=filePath) as zipRef:
                    
                    # show progress bar based on number of files in archive
                    for file in tqdm(iterable=zipRef.namelist(), total=len(zipRef.namelist())):
                        zipRef.extract(member=file, path=config.bigTilesDir)

                zipRef.close()


            # unpack tar file if tar
            if item.endswith(".tar.gz"):

                # get tile by folder name
                tile = db.getTile(folderName = item[:-7])

                # create target directory, since there is no root dir in tar package
                targetDir = config.bigTilesDir / tile["folderName"]
                if not os.path.isdir(targetDir):
                    os.makedirs(targetDir)                    

                # untar
                with tarfile.open(name=filePath, mode="r:gz") as tarRef:

                    # show progress bar based on number of files in archive
                    for file in tqdm(iterable=tarRef.getmembers(), total=len(tarRef.getmembers())):
                        tarRef.extract(member=file, path=targetDir)

                tarRef.close()


            # remove packed file
            os.remove(filePath)

            # set unpacked date in database
            db.setUnzippedForTile(tile["rowid"])

            i += 1


    logger.info("tile zip/tar files extracted")


def startAndCropRequestedDownloads():

    print("\nStart requested downloads:")
    print("--------------------------------")

    tiles = db.getRequestedTiles()

    if not tiles == None:

        sentinel = sentinelWrapper.sentinelWrapper()
        asf = asfWrapper.asfWrapper()

        for tile in tiles:

            if tile["platform"].startswith("Sentinel"):

                print(f"\nSentinel tile: {tile['folderName']}")
                print(f"Product ID: {tile['productId']}")
                print(f"First download request: {convertDate(tile['firstDownloadRequest'], newFormat='%Y-%m-%d %H:%M:%S')}")
                if tile['lastDownloadRequest'] == None:
                    print(f"Last download request: None\n")
                else:
                    print(f"Last download request: {convertDate(tile['lastDownloadRequest'], newFormat='%Y-%m-%d %H:%M:%S')}\n")

                granule = tile['folderName'].split(".")[0]

                if sentinel.readyForDownload(tile['productId']):

                    print("Product ready for download!\n")

                    # download sentinel product
                    # sentinel wrapper has a resume function for incomplete downloads
                    logger.info("Download started.")
                    download_complete = sentinel.downloadSentinelProduct(tile['productId'])

                    if download_complete:

                        # if downloaded zip-file could be detected set download complete date in database
                        if pathlib.Path(config.bigTilesDir / (granule + ".zip") ).is_file():

                            unpackBigTiles()
                            db.setDownloadCompleteForTile(tile['rowid'])

                else:
                    
                    print("Product not available for download yet.")

                    if granule.startswith("S1") and asf.downloadS1Tile(granule, config.bigTilesDir):

                        unpackBigTiles()
                        db.setDownloadCompleteForTile(tile['rowid'])
                        print(f"Tile {granule} downloaded from Alaska Satellite Facility")

                    else:                    

                        if tile['lastDownloadRequest'] == None or dateOlderThan24h(tile['lastDownloadRequest']):

                            print("Last successful download request older than 24h or non-existing.")
                            print("Repeat download request...")

                            lastRequest = minutesSinceLastDownloadRequest()

                            if lastRequest == None or lastRequest > config.copernicusRequestDelay:

                                if sentinel.requestOfflineTile(tile['productId']) == True:

                                    # Request successful
                                    db.setLastDownloadRequestForTile(tile["rowid"])
                                    print("Download of archived tile triggered. Please try again between 24 hours and 3 days later.")

                                else:

                                    # Request error
                                    db.clearLastDownloadRequestForTile(tile["rowid"])
                                    print("Download request failed! Please try again later.")                        

                            else:

                                print(f"There has been already a download requested in the last {config.copernicusRequestDelay} minutes! Please try later.")

                        else:

                            print("Last successful download request not older than 24h. Please try again later.")


    # unpack new big tiles
    utils.unpackBigTiles()
    logger.info("Big tiles unpacked.")

    # get projections of new downloaded tiles
    saveMissingTileProjections()

    # crop outstanding points                    

    pois = db.getUncroppedPoisForDownloadedTiles()

    if not pois == None:

        print("\nCrop outstanding points:")
        print("------------------------------")

        for poi in pois:

            if poi['tileCropped'] == None and poi['cancelled'] == None:

                print(f"Crop outstanding point: lat:{poi['lat']} lon:{poi['lon']} \
                        groupname:{poi['groupname']} width:{poi['width']} height:{poi['height']}")
                cropTiles(poi['rowid'])
    
        print("\nCropped all outstanding points!")


def cropTiles(poiId):
    
    print("\nCrop tiles:")
    print("-----------------")

    poi = db.getPoiFromId(poiId)

    print("(w: %d, h: %d)\n" % (poi["width"], poi["height"]))


    # crop tile if point of interest (POI) exists and width and height bigger than 0
    if not poi == None and poi["width"] > 0 and poi["height"] > 0:

        # get tiles that need to be cropped
        tiles = db.getTilesForPoi(poiId)
        
        # go through the tiles
        for tile in tiles:
        
            # crop if tile is not cropped yet (with the parameters of POI)
            if tile["tileCropped"] == None and not (tile["downloadComplete"] == None):

                print("Cropping %s ..." % tile["folderName"])

                if poi["platform"] == "Sentinel-1" or poi["platform"] == "Sentinel-2":
                    beginposition = convertDate(tile["beginposition"], newFormat="%Y%m%d-%H%M")
                else:
                    beginposition = convertDate(tile["beginposition"], newFormat="%Y%m%d")

                poiParameters = getPoiParametersForOutputFolder(poi)
                connectionId = db.getTilePoiConnectionId(poiId, tile["rowid"])
                mainTargetFolder = config.croppedTilesDir / poi["groupname"] / poiParameters / ( "%s_%s_%s_%s" % (connectionId, poi["lon"], poi["lat"], beginposition) )

                # target directory for cropped image
                targetDir = mainTargetFolder / "sensordata"
                targetDir.mkdir(parents = True, exist_ok = True)               

                # target directory for meta information
                metaDir = mainTargetFolder / "original-metadata"

                # target directory for preview image
                previewDir = mainTargetFolder 
                # previewDir.mkdir(parents = True, exist_ok = True)   


                # SENTINEL 1 CROPPING
                
                if poi["platform"] == "Sentinel-1":

                    if config.gptSnap.exists():

                        corner_coordinates = getLatLonCornerCoordinates(poi["lat"], poi["lon"], poi["width"], poi["height"])

                        # convert S1 crops to UTM projection or leave it in WGS84
                        if config.covertS1CropsToUTM == True:
                            projection = "AUTO:42001"
                        else:
                            projection = "EPSG:4326"

                        targetFile = targetDir / "s1_cropped.tif"

                        # preprocess and crop using SNAP GPT
                        poly = Polygon([[p.x, p.y] for p in corner_coordinates])
                        command = [str(config.gptSnap), os.path.realpath(str(config.xmlSnap)), 
                                   ("-PinDir=" + os.path.realpath(str(config.bigTilesDir / tile["folderName"]))),
                                   ("-Psubset=" + poly.wkt),
                                   ("-PoutFile=" + os.path.realpath(str(targetFile))),
                                   ("-PmapProjection=" + projection)]
                        subprocess.call(command)

                        if targetFile.exists():

                            print("done.\n")

                            # create preview image
                            createPreviewRGImage(str(targetFile), mainTargetFolder, exponential_scale=None)

                            # set date for tile cropped 
                            db.setTileCropped(poiId, tile["rowid"], mainTargetFolder)

                        else:

                            print("Sentinel-1 crop could not be created!")

                            # cancel crop
                            db.setCancelledTileForPoi(poiId, tile["rowid"])


                        # copy or link metadata
                        if config.copyMetadata:                            
                            print("Copy metadata...")
                            metaDir.mkdir(parents = True)
                            tileDir = config.bigTilesDir / tile["folderName"]
                            for item in tileDir.rglob('*'):
                                if item.is_file() and item.suffix.lower() != ".tiff" and item.suffix.lower() != ".safe":
                                    targetDir = metaDir / item.parent.relative_to(tileDir)
                                    if not targetDir.exists():
                                        targetDir.mkdir(parents = True)
                                    shutil.copy(item, targetDir)
                            print("done.\n")    

                        if config.createSymlink:
                            tileDir = config.bigTilesDir / tile["folderName"]
                            if not metaDir.exists():
                                # TODO: set config parameter for realpath or relpath for symlinks
                                metaDir.symlink_to(os.path.realpath(str(tileDir.resolve())), str(metaDir.parent.resolve()))
                                print("Symlink created.")                        

                    else:
                        print("SNAP GPT not configured. Sentinel-1 tiles cannot be cropped.\n")
                        db.setCancelledTileForPoi(poiId, tile["rowid"])  


                # SENTINEL 2 CROPPING

                if poi["platform"] == "Sentinel-2":

                    corner_coordinates = None

                    # Sentinel-2 img data are in jp2-format
                    # set appropriate format for GDAL lib
                    fileFormat="JP2OpenJPEG"

                    # go through "SAFE"-directory structure of Sentinel-2

                    is_S2L1 = True

                    pathGranule = config.bigTilesDir / tile["folderName"] / "GRANULE"
                    for mainFolder in os.listdir(pathGranule):

                        pathImgData = pathGranule / mainFolder / "IMG_DATA"
                        for imgDataItem in os.listdir(pathImgData):

                            pathImgDataItem = pathImgData / imgDataItem

                            # if Level-1 data pathImgDataItem is already an image file
                            # if Level-2 data pathImgDataItem is a directory with image files

                            if os.path.isdir(pathImgDataItem):

                                # Level-2 data

                                is_S2L1 = False

                                targetSubDir = targetDir / imgDataItem
                            
                                for item in os.listdir(pathImgDataItem):

                                    # set path of img file
                                    path = pathImgDataItem / item

                                    if corner_coordinates == None:
                                        corner_coordinates = getXYCornerCoordinates(path, poi["lat"], poi["lon"], poi["width"], poi["height"])

                                    # CROP IMAGE
                                    cropImg(path, item, corner_coordinates["top_left"], corner_coordinates["bottom_right"], 
                                                  targetSubDir, fileFormat, isLatLon=False)

                                createPreviewRGBImage("*B04_10m.jp2", "*B03_10m.jp2", "*B02_10m.jp2", targetSubDir, previewDir)
                            
                            else:

                                # Level-1 data

                                # set path of image file
                                path = pathImgDataItem

                                if corner_coordinates == None:
                                    corner_coordinates = getXYCornerCoordinates(path, poi["lat"], poi["lon"], poi["width"], poi["height"])

                                # CROP IMAGE
                                cropImg(path, imgDataItem, corner_coordinates["top_left"], corner_coordinates["bottom_right"], 
                                              targetDir, fileFormat, isLatLon=False)

                        if is_S2L1:
                            createPreviewRGBImage("*B04.jp2", "*B03.jp2", "*B02.jp2", targetDir, previewDir)                                

                    print("done.\n")        

                    if config.copyMetadata:                            
                        print("Copy metadata...")
                        metaDir.mkdir(parents = True)
                        tileDir = config.bigTilesDir / tile["folderName"]
                        for item in tileDir.rglob('*'):
                            if item.is_file() and item.suffix.lower() != ".jp2":
                                targetDir = metaDir / item.parent.relative_to(tileDir)
                                if not targetDir.exists():
                                    targetDir.mkdir(parents = True)
                                shutil.copy(item, targetDir)
                        print("done.\n")

                    if config.createSymlink:
                        tileDir = config.bigTilesDir / tile["folderName"]
                        if not metaDir.exists():
                            # TODO: set config parameter for realpath or relpath for symlinks
                            metaDir.symlink_to(os.path.realpath(str(tileDir.resolve())), str(metaDir.parent.resolve()))
                            print("Symlink created.")

                    # set date for tile cropped 
                    db.setTileCropped(poiId, tile["rowid"], mainTargetFolder)


                # LANDSAT CROPPING

                if poi["platform"].startswith("LANDSAT"):
                
                    print("Cropping of Landsat data not yet supported.\n")
                    db.setCancelledTileForPoi(poiId, tile["rowid"])

                    # # Landsat img data are in GeoTiff-format
                    # # set appropriate format for GDAL lib
                    # fileFormat="GTiff"

                    # # all images are in root dir of tile

                    # # set path of root dir of tile
                    # pathImgData = config.bigTilesDir / tile["folderName"]

                    # # TODO: switch to pathlib (for item in pathImgData)
                    # # go through all files in root dir of tile
                    # for item in os.listdir(pathImgData):

                    #     # if file ends with tif then crop
                    #     if item.lower().endswith(".tif"):

                    #         # set path of image file
                    #         path = pathImgData / item

                    #         # CROP IMAGE
                    #         cropImg(path, item, topLeft, bottomRight, targetDir, fileFormat)

                    # if poi["platform"] == "LANDSAT_8_C1":
                    #     r_band_search_pattern = "*B4.TIF"
                    #     g_band_search_pattern = "*B3.TIF"
                    #     b_band_search_pattern = "*B2.TIF"
                    # else:
                    #     r_band_search_pattern = "*B3.TIF"
                    #     g_band_search_pattern = "*B2.TIF"
                    #     b_band_search_pattern = "*B1.TIF"                           
                    # createPreviewRGBImage(r_band_search_pattern, g_band_search_pattern, b_band_search_pattern, targetDir, previewDir)                         

                    # print("done.")

                    # if config.copyMetadata:
                    #     print("Copy metadata...")
                    #     metaDir.mkdir(parents = True)
                    #     for item in pathImgData.glob('*'):
                    #         if item.is_file():
                    #             if item.suffix.lower() != ".tif":
                    #                 shutil.copy(item, metaDir)
                    #         if item.is_dir():
                    #             shutil.copytree(item, (metaDir / item.name))
                    #     print("done.\n")

                    # if config.createSymlink:
                    #     tileDir = pathImgData
                    #     # TODO: set config parameter for realpath or relpath for symlink
                    #     metaDir.symlink_to(os.path.realpath(str(tileDir.resolve())), str(metaDir.parent.resolve()))
                    #     print("Symlink created.")                            

                    # # set date for tile cropped 
                    # db.setTileCropped(poiId, tile["rowid"], mainTargetFolder) 


def getPoiParametersForOutputFolder(poi):
    
    folderElements = []
    folderName = ""

    try:
        folderElements.append("df" + convertDate(poi["dateFrom"], "%Y%m%d"))
    except:
        pass

    try:
        folderElements.append("dt" + convertDate(poi["dateTo"], "%Y%m%d"))
    except:
        pass
        
    try:
        if poi["platform"] == "Sentinel-1":
            folderElements.append("pfS1")
        if poi["platform"] == "Sentinel-2":
            folderElements.append("pfS2")
        if poi["platform"] == "LANDSAT_TM_C1":
            folderElements.append("pfLTM")
        if poi["platform"] == "LANDSAT_ETM_C1":
            folderElements.append("pfLETM")
        if poi["platform"] == "LANDSAT_8_C1":
            folderElements.append("pfL8")
    except:
        pass
        
    try:
        folderElements.append("tl" + str(poi["tileLimit"]))
    except:
        pass
        
    try:
        folderElements.append("cc" + str(poi["cloudcoverpercentage"]))
    except:
        pass
        
    try:
        folderElements.append("pm" + str(poi["polarisatiomode"]))
    except:
        pass
        
    try:
        folderElements.append("pt" + str(poi["producttype"]))
    except:
        pass
        
    try:
        folderElements.append("som" + str(poi["sensoroperationalmode"]))
    except:
        pass
        
    try:
        folderElements.append("si" + str(poi["swathidentifier"]))
    except:
        pass
        
    try:
        folderElements.append("tls" + str(poi["timeliness"]))
    except:
        pass
        
    try:
        folderElements.append("w" + str(poi["width"]))
    except:
        pass
        
    try:
        folderElements.append("h" + str(poi["height"]))
    except:
        pass

    for item in folderElements:
        if not item.endswith("None"):            
            if len(folderName) > 0:
                folderName = folderName + "_"
            folderName = folderName + item

    return folderName    


def saveMissingTileProjections():

    tiles = db.getTilesWithoutProjectionInfo()
    for tile in tiles:
        saveTileProjection(tile["productId"])


def saveTileProjection(productId):

    tile = db.getTile(productId)

    if tile != None and tile["downloadComplete"] != None:

        mainFolder = config.bigTilesDir / tile["folderName"]
        projection = None

        if tile["platform"] == "Sentinel-1":

            imageFolder = mainFolder / "measurement"

            image = list(imageFolder.glob("*.tiff"))[0]

            projection = getProjectionFromFile(image, tile["platform"])

        if tile["platform"] == "Sentinel-2":

            imageFolder = mainFolder / "GRANULE"

            image = list(imageFolder.rglob("*_B02*.jp2"))[0]

            projection = getProjectionFromFile(image, tile["platform"])

        # save projection to database
        if projection != None:
            db.updateTileProjection(tile["rowid"], projection)


def getProjectionFromFile(path, platform):

    if path != None and path.exists():

        img = rasterio.open(str(path))

        if platform == "Sentinel-1":

            # Sentinel-1 uses ground control points (GCPs)
            gcps, gcp_crs = img.gcps
            projection = gcp_crs

        if platform == "Sentinel-2":

            projection = img.crs
    
        return str(projection)
