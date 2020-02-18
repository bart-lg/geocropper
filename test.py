import geocropper

geoc = geocropper.Geocropper(40.58497, -12.75013)
geoc.downloadSentinelData("20190801", "20190831", "Sentinel-2", 10)

