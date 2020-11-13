import numpy
from PIL import Image, ImageDraw, ImageFont
import math
import matplotlib.pyplot as pyplot
import os
import pathlib
import rasterio
import shutil
import pyproj
import random
from shapely.geometry import Point
from shapely.ops import transform
from dateutil.parser import *
from functools import partial
import subprocess
import sys

import geocropper.config as config

import logging

from osgeo import gdal
# gdal library distributed by conda destroys PATH environment variable
# see -> https://github.com/OSGeo/gdal/issues/1231
# workaround: remove first entry...
os.environ["PATH"] = os.environ["PATH"].split(';')[1]

# get logger object
logger = logging.getLogger('root')


def convertDate(date, newFormat="%Y-%m-%d"):
    temp = parse(date)
    return temp.strftime(newFormat)

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

    logger.info("Create preview image.")

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
                          upper_label_list=upper_label_list, center_point=True)

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
            shutil.copytree(folder_in, folder_out, symlinks = True)

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
