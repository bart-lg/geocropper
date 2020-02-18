import sys
import config
from geojson import Point

# from sentinelsat.sentinel import SentinelAPI, geojson_to_wkt, read_geojson
from sentinelsat import SentinelAPI, geojson_to_wkt

class sentinelWrapper:

	def __init__(self):
		self.api = SentinelAPI(config.copernicusUser, config.copernicusPW, 'https://scihub.copernicus.eu/dhus')

	def getSentinelProducts(self, lat, lon, fromDate, toDate, platforms, maxCloudCoverage):
		footprint = geojson_to_wkt(Point((lat, lon)))
		return self.api.query(footprint,
		        date=(fromDate, toDate),
		        platformname=platforms,
		        cloudcoverpercentage=(0, maxCloudCoverage))

	def downloadSentinelProducts(self, products):
		self.api.download_all(products, config.bigTilesDir)

	def downloadSentinelProduct(self, productID):
		self.api.download(productID, config.bigTilesDir)

