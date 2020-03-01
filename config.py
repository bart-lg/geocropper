import userconfig

# path and filename of log file
logFile = "geocropper.log"

# used for download of Sentinel data
copernicusUser = userconfig.copernicusUser
copernicusPW = userconfig.copernicusPW
copernicusURL = 'https://scihub.copernicus.eu/dhus'

# used for download of Landsat data
usgsUser = userconfig.usgsUser
usgsPW = userconfig.usgsPW

# various data paths
dataDir = "./data"
bigTilesDir = dataDir + "/bigTiles"
croppedTilesDir = dataDir + "/croppedTiles"
csvInputDir = dataDir + "/csvInput"
csvArchiveDir = dataDir + "/csvArchive"

# shape file used to determine country for geolocation
worldBordersShapeFile = dataDir + "/worldBorders/TM_WORLD_BORDERS-0.3.shp"

# path and filename of sqlite database file
dbFile = "geocropper.db"

# ATTENTION: Changes in optionalSentinelParameters requires manual removal of existing database before starting script!!!
# optional landsat parameters => max_cloud_cover (value in db stored as cloudcoverpercentage)
optionalSentinelParameters = ["polarisationmode", "producttype", "sensoroperationalmode", "swathidentifier", "cloudcoverpercentage", "timeliness"]
