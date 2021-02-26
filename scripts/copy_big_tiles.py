import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('../')
sys.path.append(os.getcwd())

from geocropper import *

out_path = "/media/sm-ssd-1/bigTiles_S1_03"

geocropper.copy_big_tiles(out_path)
