import sys
sys.path.append('../geocropper')

from geocropper import *


#geocropper.importAllCSVs()

#download_and_crop(48, 16, "20190601", "20190630", "Sentinel-2", 2000, 2000, tile_limit=2, cloudcoverpercentage=10, producttype="S2MSI2A")

download_and_crop(48, 16, "2019-08-01", "2019-08-31", "Sentinel-2", 1500, 1500, tile_limit=1, cloudcoverpercentage=10, producttype="S2MSI2A")
#download_and_crop(48, 16, "20190101", "20191231", "Sentinel-1", 1000, 1000, tile_limit=1, sensoroperationalmode="IW")
#download_and_crop(48, 16, "20190101", "20191231", "Sentinel-1", 1000, 1000, tile_limit=1, swathidentifier="IW2")


#download_and_crop(48, 16, "2019-07-01", "2019-08-31", "LANDSAT_8_C1", 1000, 1000, tile_limit=1, cloudcoverpercentage=10)
# download_and_crop(48, 16, "2011-07-01", "2011-08-31", "LANDSAT_TM_C1", 1000, 1000, tile_limit=1, cloudcoverpercentage=10)
# download_and_crop(48, 16, "2011-07-01", "2011-08-31", "LANDSAT_ETM_C1", 2000, 2000, tile_limit=1, cloudcoverpercentage=10)

