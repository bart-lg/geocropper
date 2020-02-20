import geocropper

geoc = geocropper.Geocropper(25, 4)
geoc.downloadAndCrop("20190701", "20190715", "Sentinel-2", 2000, 2000, cloudcoverpercentage=10, producttype="S2MSI2A")
#geoc.downloadAndCrop("20190701", "20190715", "Sentinel-1", 1000, 1000)

