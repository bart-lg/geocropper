import sys
sys.path.append('../geocropper')

from geocropper import *


#geocropper.importAllCSVs()


geoc = geocropper.init(48, 16)
#geoc.downloadAndCrop("20190601", "20190630", "Sentinel-2", 2000, 2000, tileLimit=2, cloudcoverpercentage=10, producttype="S2MSI2A")

geoc.downloadAndCrop("2019-08-01", "2019-08-31", "Sentinel-2", 1500, 1500, tileLimit=1, cloudcoverpercentage=10, producttype="S2MSI2A")
#geoc.downloadAndCrop("20190101", "20191231", "Sentinel-1", 1000, 1000, tileLimit=1, sensoroperationalmode="IW")
#geoc.downloadAndCrop("20190101", "20191231", "Sentinel-1", 1000, 1000, tileLimit=1, swathidentifier="IW2")


#geoc.downloadAndCrop("2019-07-01", "2019-08-31", "LANDSAT_8_C1", 1000, 1000, tileLimit=1, cloudcoverpercentage=10)
# geoc.downloadAndCrop("2011-07-01", "2011-08-31", "LANDSAT_TM_C1", 1000, 1000, tileLimit=1, cloudcoverpercentage=10)
# geoc.downloadAndCrop("2011-07-01", "2011-08-31", "LANDSAT_ETM_C1", 2000, 2000, tileLimit=1, cloudcoverpercentage=10)

