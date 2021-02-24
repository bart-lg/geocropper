import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('../')
sys.path.append(os.getcwd())

from geocropper import *

in_path = "/home/bart/21_windturbine/random_crops_w4000_h4000_filtered"
out_path = "/home/bart/21_windturbine/random_crops_w2000_h2000_filtered"

geocropper.trim_crops(in_path, out_path, 2000, 2000, has_subdir=False)
