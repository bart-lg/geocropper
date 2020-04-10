import userconfig
import pathlib

# TODO: change format of config variable names to UPPER_CASE names with underscore as separator

# used for download of Sentinel data
copernicusUser = userconfig.copernicusUser
copernicusPW = userconfig.copernicusPW
copernicusURL = 'https://scihub.copernicus.eu/dhus'

# used for download of Landsat data
usgsUser = userconfig.usgsUser
usgsPW = userconfig.usgsPW

# various data paths
dataDir = pathlib.Path(__file__).parent.parent / "data"
bigTilesDir = dataDir / "bigTiles"
croppedTilesDir = dataDir / "croppedTiles"
csvInputDir = dataDir / "csvInput"
csvArchiveDir = dataDir / "csvArchive"
logFile = dataDir / "geocropper.log"

# shape file used to determine country for geolocation
worldBordersShapeFile = str(dataDir / "worldBorders" / "TM_WORLD_BORDERS-0.3.shp")

# path and filename of sqlite database file
dbFile = dataDir / "geocropper.db"

# ATTENTION: Changes in optionalSentinelParameters requires manual removal of existing database before starting script!!!
# optional landsat parameters => max_cloud_cover (value in db stored as cloudcoverpercentage)
optionalSentinelParameters = ["polarisationmode", "producttype", "sensoroperationalmode", "swathidentifier", "cloudcoverpercentage", "timeliness"]

# logging modes: DEBUG, INFO, WARNING, ERROR, CRITICAL
loggingMode = "WARNING"

# combined preview images
previewBorder = 5
previewBackground = (100,0,0)
previewTextOnImage = True
previewImagesCombined = 25
