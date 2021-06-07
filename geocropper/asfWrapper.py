import subprocess

import geocropper.config as config
import logging

# get logger object
logger = logging.getLogger('root')

class AsfWrapper:

    def download_S1_tile(self, granule, out_path):

        if config.asfUser != None and config.asfUser != "":

            if granule.endswith(".zip"):
                download_file = granule
            else:
                download_file = f"{granule}.zip"

            command = ["wget", "-c", f"--http-user={config.asfUser}", f"--http-password={config.asfPW}",
                       f"https://datapool.asf.alaska.edu/GRD_HD/SA/{download_file}", "-P", str(out_path)]
            subprocess.call(command)

            path = out_path / download_file

            if path.is_file():
                logger.info(f"Tile {granule} downloaded from Alaska Satellite Facility.")
                return True
            else:
                return False

        else:
            return None
