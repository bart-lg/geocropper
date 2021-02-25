import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('../')
sys.path.append(os.getcwd())

from geocropper import *

# check settings in user-config.ini: [Combined Preview Images]

path = "/home/bart/21_windturbine/random_crops_w2000_h2000_filtered"

geocropper.combine_preview_images(path, outside_cropped_tiles_dir=True, has_subdir=False)
