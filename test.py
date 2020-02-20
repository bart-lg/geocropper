import geocropper

geoc = geocropper.Geocropper(20, 10)
geoc.downloadAndCrop("20190701", "20190731", "Sentinel-2", 1000, 1000, cloudcoverpercentage=10, producttype="S2MSI2A")
#geoc.downloadAndCrop("20190701", "20190715", "Sentinel-1", 1000, 1000)

