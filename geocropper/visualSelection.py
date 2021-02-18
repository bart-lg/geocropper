# import numpy
import pathlib
import math
import csv
import cv2

import geocropper.config as config
import logging

# get logger object
logger = logging.getLogger('root')


# PreviewImage keeps path of the preview image
class PreviewImage:
    def __init__(self, path):
        self.img = cv2.imread(str(path.absolute()))
        self.path = path


def get_combined_preview_numbers(folder):
    """Returns a list of the combined-preview files in the given folder
    """

    preview_numbers = []

    if folder.is_dir():

        for image_file in folder.glob("combined-preview-*.tif"):

            preview_number = image_file.name.replace(".tif", "").replace("combined-preview-", "")
            preview_numbers.append(int(preview_number))

    preview_numbers.sort()
    return preview_numbers


def get_image_ids(file_path):
    """Returns 2D array of IDs of preview images in given combined-preview file.
    """

    folder = file_path.parents[0]
    meta_file_name = file_path.name.replace(".tif", "") + "-paths.txt"
    meta_file = folder / meta_file_name

    if meta_file.exists():
        
        id_list = []

        with open(str(meta_file.absolute()), "r", newline="") as f:
            reader = csv.reader(f, delimiter="\t")
            for line in reader:
                row = []
                for item in line:
                    splitted_crop_path = item.split("/")
                    crop_folder = splitted_crop_path[len(splitted_crop_path) - 2]
                    crop_id = crop_folder.split("_")[0]
                    row.append(crop_id)
                id_list.append(row)

        return id_list

    else:
        return False


def load_csv(folder, marker):
    """Load saved markers.
    """

    folder = pathlib.Path(folder)

    ids = set()

    csv_file = folder / ( "marker_" + str(marker) + ".csv" ) 

    if csv_file.exists():
        with open(str(csv_file.absolute()), "r", newline="") as f:
            reader = csv.reader(f)
            for line in reader:
                if len(line) > 0 and len(line[0]) > 0 and line[0].isdigit():
                    ids.add(int(line[0]))

    return ids


def save_csv(folder, marker, ids):
    """Save markers.
    """

    folder = pathlib.Path(folder)

    csv_file = folder / ( "marker_" + str(marker) + ".csv" ) 

    ids_sorted = list(ids)
    ids_sorted.sort()

    with open(str(csv_file.absolute()), "w", newline="") as f:
        spamwriter = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        if ids_sorted != None:
            for item in ids_sorted:
                spamwriter.writerow([str(item)])


def get_image_text(image_ids, marker_lists):
    """Creates 2D array with image texts for marked images.
    """

    result = [["" for i in range(len(image_ids[0]))] for j in range(len(image_ids))]

    for i, marker_list in enumerate(marker_lists):

        marker = i + 1

        for row in range(len(image_ids)):
            for col in range(len(image_ids[row])):
                if image_ids[row][col].isdigit() and int(image_ids[row][col]) in marker_list:
                    result[row][col] = "M" + str(marker) + " [" + str(image_ids[row][col]) + "]"

    return result


def show_image(preview_image_path, gap):

    # set font for markers
    font = cv2.FONT_HERSHEY_SIMPLEX

    preview_image = PreviewImage(preview_image_path)

    # load markers 1 to 3
    markers = []
    for marker in range(1,4):
        markers.append(load_csv(preview_image.path.parents[0], marker))    

    # make sure image fills window
    cv2.namedWindow('ImageSelection', cv2.WND_PROP_FULLSCREEN)
    # cv2.setWindowProperty('ImageSelection',cv2.WND_PROP_FULLSCREEN,cv2.WINDOW_FULLSCREEN)
    
    # get IDs of images displayed
    image_ids = get_image_ids(preview_image.path)

    # get marker texts
    text_arr = get_image_text(image_ids, markers)

    # get shape of preview image
    height, width, bands = preview_image.img.shape

    # determine rows and cols
    grid_rows = len(image_ids)
    grid_cols = len(image_ids[0])

    # determine height and width of images
    item_height = math.ceil((height - ((grid_rows - 1) * gap)) / grid_rows)
    item_width = math.ceil((width - ((grid_cols - 1) *  gap)) / grid_cols)

    # write marker text on preview image
    for row in range(len(text_arr)):
        for col in range(len(text_arr[row])):

            if len(text_arr[row][col]) > 0:

                text_x = col * (item_width + gap) + config.textOffsetX
                text_y = row * (item_height + gap) + config.textOffsetY

                cv2.putText(preview_image.img, text_arr[row][col], (text_x,text_y), font, config.fontScale, \
                    (config.fontColorR, config.fontColorG, config.fontColorB), config.lineType)

    # show image to screen
    cv2.imshow('ImageSelection', preview_image.img)

    # define mouse callback
    cv2.setMouseCallback('ImageSelection', mouse_event, \
        {
            "preview_image": preview_image, 
            "image_shape": (item_height, item_width), 
            "gap": gap, 
            "markers": markers, 
            "image_ids": image_ids
        })  


def mouse_event(event, x, y, flags, params):
    
    write_csvs = False

    preview_image = params["preview_image"]
    image_shape = params["image_shape"]
    gap = params["gap"]
    markers = params["markers"]
    image_ids = params["image_ids"]

    if event == cv2.EVENT_LBUTTONDOWN:

        row, col = get_grid_cell(preview_image.img.shape, image_shape, gap, (x,y))

        if row < len(image_ids) and col < len(image_ids[row]) and image_ids[row][col].isdigit():
        
            image_id = int(image_ids[row][col])

            if flags & cv2.EVENT_FLAG_SHIFTKEY:

                # remove marker 2 and 3
                markers[1].discard(image_id)
                markers[2].discard(image_id)
                
                if image_id in markers[0]:
                    # remove marker 1
                    markers[0].discard(image_id)
                else:
                    # add marker 1
                    markers[0].add(image_id)

                write_csvs = True


            elif flags & cv2.EVENT_FLAG_CTRLKEY:

                # remove marker 1 and 3
                markers[0].discard(image_id)
                markers[2].discard(image_id)
                
                if image_id in markers[1]:
                    # remove marker 2
                    markers[1].discard(image_id)
                else:
                    # add marker 2
                    markers[1].add(image_id)

                write_csvs = True

            elif flags & cv2.EVENT_FLAG_ALTKEY:

                # remove marker 1 and 2
                markers[0].discard(image_id)
                markers[1].discard(image_id)
                
                if image_id in markers[2]:
                    # remove marker 3
                    markers[2].discard(image_id)
                else:
                    # add marker 3
                    markers[2].add(image_id)

                write_csvs = True

    # save markers and refresh image
    if write_csvs:
        for marker in range(len(markers)):
            save_csv(preview_image.path.parents[0], marker+1, markers[marker])
        show_image(preview_image.path, gap)


def get_grid_cell(preview_image_shape, image_shape, gap, position):

    preview_image_height, preview_image_width, preview_image_bands = preview_image_shape
    image_height, image_width = image_shape
    image_height = image_height + gap
    image_width = image_width + gap
    x, y = position

    # determine row
    row = 0
    if y > image_height:
        row = math.floor(y / image_height)

    # determine col
    col = 0
    if x > image_width:
        col = math.floor(x / image_width)

    return row, col        


def get_preview_image_filename(image_number):
    return f"combined-preview-{str(image_number)}.tif"


def start_visual_selection(path, gap=config.previewBorder, image_start=1):
    """Opens GUI window with visual selection of images.
    """

    path = pathlib.Path(path)

    file_numbers = get_combined_preview_numbers(path)

    if image_start in file_numbers:
        preview_image_index = file_numbers.index(image_start)
    else:
        preview_image_index = 0
    
    show_image(path / get_preview_image_filename(file_numbers[preview_image_index]), gap)  

    while True:

        # Needs a value greater than 0 otherwise it would wait until a key is pressed
        # and then it would not be possible to catch closed windows
        pressedkey=cv2.waitKey(100)    

        # Wait for ESC key to exit
        if pressedkey==27:
            cv2.destroyAllWindows()
            break

        # next image (forward)
        if pressedkey==ord("f"):

            if (preview_image_index + 1) < len(file_list):

                preview_image_index = preview_image_index + 1
                show_image(path / get_preview_image_filename(file_numbers[preview_image_index]), gap)

            else:

                print(f"Current image is the last image: {get_preview_image_filename(file_numbers[preview_image_index])}")
            
        # previous image (back)
        if pressedkey==ord("b"):

            if preview_image_index > 0:

                preview_image_index = preview_image_index - 1
                show_image(path / get_preview_image_filename(file_numbers[preview_image_index]), gap)

            else:

                print(f"Current image is the first image: {get_preview_image_filename(file_numbers[preview_image_index])}")

        # show preview image path
        if pressedkey==ord("i"):
            print(f"Current preview image: {get_preview_image_filename(file_numbers[preview_image_index])}")

        try:
            if cv2.getWindowProperty('ImageSelection', cv2.WND_PROP_VISIBLE) == 0:
                break
        except:
            break
            