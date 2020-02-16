
class Geocropper:

	def __init__(self, lat , lon):
		self.lat = lat
		self.lon = lon

	def printPosition(self):
		print("lat: " + str(self.lat))
		print("lon: " + str(self.lon))

	