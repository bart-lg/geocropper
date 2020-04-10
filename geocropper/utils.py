import numpy
from PIL import Image, ImageDraw, ImageFont
import math
import matplotlib.pyplot as pyplot
import os
import pathlib

import geocropper.config as config


def concat_images(image_path_list, output_file, gap = 3, bcolor = (0,0,0), paths_to_file = None, \
    upper_label_list = None, lower_label_list = None, write_image_text = True):
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

    """

    # determine needed raster size
    raster_size = math.ceil(math.sqrt(len(image_path_list)))

    # determine max heigth and max width of all images
    max_height = 0
    max_width = 0

    for image_path in image_path_list:
        image = pyplot.imread(image_path)[:,:,:3]
        height, width = image.shape[:2]
        if height > max_height:
            max_height = height
        if width > max_width:
            max_width = width

    # add gap to width and height
    total_height = max_height * raster_size + gap * ( raster_size - 1 )
    total_width = max_width * raster_size + gap * ( raster_size - 1 )

    # assign positions to images
    # positions = [row, column, height_start, width_start]
    positions = numpy.zeros((len(list(image_path_list)), 4), dtype = int)
    for i, image_path in enumerate(image_path_list, 1):
        # determine position
        row = math.ceil(i / raster_size)
        column = i % raster_size
        if column == 0:
            column = raster_size        

        # determine starting width and height
        height_start = ( row - 1 ) * ( max_height + gap )   
        width_start = ( column - 1 ) * ( max_width + gap )

        positions[i-1][0] = int(row)
        positions[i-1][1] = int(column)
        positions[i-1][2] = int(height_start)
        positions[i-1][3] = int(width_start)

    # create empty image
    combined_image = numpy.full((total_height, total_width, 3), bcolor)

    # paste images to combined image
    for i, image_path in enumerate(image_path_list):
        
        # read image
        image = pyplot.imread(image_path)[:,:,:3]

        # determine width and height
        height, width = image.shape[:2]

        # paste image
        combined_image[positions[i][2]:(positions[i][2]+height), positions[i][3]:(positions[i][3]+width)] = image

    # write file
    image = Image.fromarray(numpy.uint8(combined_image))

    # write paths on image
    if write_image_text:
        font = ImageFont.truetype(str( \
            pathlib.Path(os.environ["CONDA_PREFIX"]) / "fonts" / "open-fonts" / "IBMPlexMono-Regular.otf"), 12)
        draw = ImageDraw.Draw(image)
        if upper_label_list == None:
            upper_label_list = image_path_list
        for i, upper_label in enumerate(upper_label_list):
            # draw.text requires coordinates in following order: width, height
            draw.text((positions[i][3] + 5, positions[i][2] + 5), upper_label, font=font, fill=(255,0,0))
        if lower_label_list != None:
            for i, lower_label in enumerate(lower_label_list):
                # draw.text requires coordinates in following order: width, height
                draw.text((positions[i][3] + 5, positions[i][2] + 15), lower_label, font=font, fill=(255,0,0))            

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


def createCombinedImages():

    for group in config.croppedTilesDir.glob("*"):

        for request in group.glob("*"):

            counter = 0
            image_path_list = []
            upper_label_list = []
            lower_label_list = []

            item_list = list(request.glob("*"))

            for i, item in enumerate(request.glob("*"), 1):

                preview_file = item / "preview.tif"
                if preview_file.exists():

                    image_path_list.append(preview_file)
                    upper_label_list.append(item.name)
                    # lower_label_list.append(item.parent.name)

                    if i % config.previewImagesCombined == 0 or i == len(item_list):

                        counter = counter + 1 

                        output_file = request / ("combined-preview-" + str(counter) + ".tif")
                        summary_file = request / ("combined-preview-" + str(counter) + "-paths.txt")

                        concat_images(image_path_list, output_file, gap = config.previewBorder, \
                            bcolor = config.previewBackground, paths_to_file = summary_file, \
                            upper_label_list = upper_label_list)

                        image_path_list = []
                        upper_label_list = []
                        # lower_label_list = []  
