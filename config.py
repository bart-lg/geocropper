import userconfig

logFile = "geocropper.log"

copernicusUser = userconfig.copernicusUser
copernicusPW = userconfig.copernicusPW
copernicusURL = 'https://scihub.copernicus.eu/dhus'

usgsUser = userconfig.usgsUser
usgsPW = userconfig.usgsPW

dataDir = "./data"
bigTilesDir = dataDir + "/bigTiles"
croppedTilesDir = dataDir + "/croppedTiles"
csvInputDir = dataDir + "/csvInput"
csvArchiveDir = dataDir + "/csvArchive"

dbFile = "geocropper.db"

# ATTENTION: Changes in optionalSentinelParameters requires manual removal of existing database before starting script!!!
optionalSentinelParameters = ["polarisationmode", "producttype", "sensoroperationalmode", "swathidentifier", "cloudcoverpercentage", "timeliness"]
# optional landsat parameters => max_cloud_cover (value in db stored in cloudcoverpercentage)

worldBordersShapeFile = dataDir + "/worldBorders/TM_WORLD_BORDERS-0.3.shp"
