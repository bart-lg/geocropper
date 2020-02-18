import sentinelWrapper
import zipfile
import tqdm
import os
import config

class Geocropper:

	def __init__(self, lat , lon):
		self.lat = lat
		self.lon = lon
		self.sentinel = sentinelWrapper.sentinelWrapper()

	def printPosition(self):
		print("lat: " + str(self.lat))
		print("lon: " + str(self.lon))

	def downloadSentinelData(self, fromDate, toDate, platforms, maxCloudCoverage):
		print("\nDownload Sentinel data for:")
		self.printPosition()
		print("From: " + fromDate)
		print("To: " + toDate)
		print("Platforms: " + platforms)
		print("maxCloudCoverage: " + str(maxCloudCoverage))
		print("=========================================================\n")
		products = self.sentinel.getSentinelProducts(self.lat, self.lon, fromDate, toDate, platforms, maxCloudCoverage)
		for key in products:
			if not os.path.isdir(config.bigTilesDir + "/" + products[key]["title"] + ".SAFE") and \
			  not os.path.isfile(config.bigTilesDir + "/" + products[key]["title"] + ".zip"):
				print("Download " + products[key]["title"])
				self.sentinel.downloadSentinelProduct(key)
			else:
				print(products[key]["title"] + " already exists.")
		self.unpackBigTiles()

	def unpackBigTiles(self):
		print("\nUnpack big tiles:\n")
		for item in os.listdir(config.bigTilesDir):
			if item.endswith(".zip"):
				filePath = config.bigTilesDir + "/" + item
				print(item + ":")
				with zipfile.ZipFile(file=filePath) as zipRef:
					for file in tqdm.tqdm(iterable=zipRef.namelist(), total=len(zipRef.namelist())):
						zipRef.extract(member=file, path=config.bigTilesDir)
				zipRef.close()
				os.remove(filePath)


	