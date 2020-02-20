import userconfig

logFile = "geocropper.log"

copernicusUser = userconfig.copernicusUser
copernicusPW = userconfig.copernicusPW
copernicusURL = 'https://scihub.copernicus.eu/dhus'

dataDir = "./data"
bigTilesDir = dataDir + "/bigTiles"
croppedTilesDir = dataDir + "/croppedTiles"

dbFile = "geocropper.db"

# ATTENTION: Changes in optionalSentinelParameters requires manual removal of existing database before starting script!!!
optionalSentinelParameters = ["polarisationmode", "producttype", "sensoroperationalmode", "swathidentifier", "cloudcoverpercentage", "timeliness"]

worldBordersShapeFile = dataDir + "/worldBorders/TM_WORLD_BORDERS-0.3.shp"