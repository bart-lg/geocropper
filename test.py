import geocropper

geoc = geocropper.Geocropper(48.2, 16.3)
#geoc.downloadAndCrop("20190701", "20190831", "Sentinel-2", 1000, 1000, tileLimit = 0,  producttype="S2MSI2A")
geoc.downloadAndCrop("20190701", "20190831", "Sentinel-2", 1000, 1000, cloudcoverpercentage=10, producttype="S2MSI2A")

