import geocropper

geoc = geocropper.init(45, 16)
#geoc.downloadAndCrop("20190701", "20190731", "Sentinel-2", 2000, 2000, tileLimit=1, cloudcoverpercentage=10, producttype="S2MSI2A")
#geoc.downloadAndCrop("20190701", "20190715", "Sentinel-1", 1000, 1000, tileLimit=1)
# geoc.downloadAndCrop("2019-07-01", "2019-08-31", "LANDSAT_8_C1", 1000, 1000, tileLimit=1, cloudcoverpercentage=10)
# geoc.downloadAndCrop("2011-07-01", "2011-08-31", "LANDSAT_TM_C1", 1000, 1000, tileLimit=1, cloudcoverpercentage=10)
geoc.downloadAndCrop("2011-07-01", "2011-08-31", "LANDSAT_ETM_C1", 2000, 2000, tileLimit=1, cloudcoverpercentage=10)

