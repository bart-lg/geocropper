import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('../')
sys.path.append(os.getcwd())

from geocropper import *

geocropper.utils.start_and_crop_requested_downloads()
