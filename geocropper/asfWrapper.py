import subprocess

import geocropper.config as config
import logging

# get logger object
logger = logging.getLogger('root')

class asfWrapper:

	def downloadS1Tile(self, granule, outPath):

		if config.asfUser != None and config.asfUser != "":

			command = ["wget", "-c", f"--http-user={config.asfUser}", f"--http-password={config.asfPW}",
					   f"https://datapool.asf.alaska.edu/GRD_HD/SA/{granule}.zip", "-P", str(outPath)]
			subprocess.call(command)

			path = outPath / (granule + ".zip")

			if path.is_file():
				logger.info(f"Tile {granule} downloaded from Alaska Satellite Facility.")
				return True
			else:
				return False

		else:
			return None
