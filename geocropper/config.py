import configparser
import pathlib

defaultConfig = configparser.ConfigParser()
defaultConfig.read(pathlib.Path(__file__).parent.parent / "default-config.ini")

if (pathlib.Path(__file__).parent.parent / "user-config.ini").exists():
	userConfig = configparser.ConfigParser()
	userConfig.read(pathlib.Path(__file__).parent.parent / "user-config.ini")	
else:
	userConfig = None

# used for download of Sentinel data
try:
	copernicusUser = userConfig["Credentials"]["copernicusUser"]
except:
	copernicusUser = defaultConfig["Credentials"]["copernicusUser"]
try:
	copernicusPW = userConfig["Credentials"]["copernicusPW"]
except:
	copernicusPW = defaultConfig["Credentials"]["copernicusPW"]	

try:
	copernicusURL = userConfig["URLs"]["copernicusURL"]
except:
	copernicusURL = defaultConfig["URLs"]["copernicusURL"]	

# used for download of Landsat data
try:
	usgsUser = userConfig["Credentials"]["usgsUser"]
except:
	usgsUser = defaultConfig["Credentials"]["usgsUser"]	
try:
	usgsPW = userConfig["Credentials"]["usgsPW"]
except:
	usgsPW = defaultConfig["Credentials"]["usgsPW"]	

# various data paths
try:
	dataDir = pathlib.Path(userConfig["Paths"]["data"])
except:
	dataDir = pathlib.Path(defaultConfig["Paths"]["data"])
try:
	bigTilesDir = pathlib.Path(userConfig["Paths"]["bigTiles"])
except:
	bigTilesDir = pathlib.Path(defaultConfig["Paths"]["bigTiles"])
try:
	croppedTilesDir = pathlib.Path(userConfig["Paths"]["croppedTiles"])
except:
	croppedTilesDir = pathlib.Path(defaultConfig["Paths"]["croppedTiles"])
try:
	csvInputDir = pathlib.Path(userConfig["Paths"]["csvInput"])
except:
	csvInputDir = pathlib.Path(defaultConfig["Paths"]["csvInput"])
try:
	csvArchiveDir = pathlib.Path(userConfig["Paths"]["csvArchive"])
except:
	csvArchiveDir = pathlib.Path(defaultConfig["Paths"]["csvArchive"])
try:
	logFile = pathlib.Path(userConfig["Paths"]["logFile"])
except:
	logFile = pathlib.Path(defaultConfig["Paths"]["logFile"])	

# shape file used to determine country for geolocation
try:
	worldBordersShapeFile = str(pathlib.Path(userConfig["Paths"]["worldBordersShapeFile"]))
except:
	worldBordersShapeFile = str(pathlib.Path(defaultConfig["Paths"]["worldBordersShapeFile"]))

# path and filename of sqlite database file
try:
	dbFile = pathlib.Path(userConfig["Paths"]["dbFile"])
except:
	dbFile = pathlib.Path(defaultConfig["Paths"]["dbFile"])	

# logging modes: DEBUG, INFO, WARNING, ERROR, CRITICAL
try:
	loggingMode = userConfig["Logging"]["loggingMode"]
except:
	loggingMode = defaultConfig["Logging"]["loggingMode"]


# copy metadata from bigTiles to croppedTiles
try:
	copyMetadata = userConfig["Meta Data"].getboolean("copyMetadata")
except:
	copyMetadata = defaultConfig["Meta Data"].getboolean("copyMetadata")

# creates symlink within croppedTiles to bigTiles
try:
	createSymlink = userConfig["Meta Data"].getboolean("createSymlink")
except:
	createSymlink = defaultConfig["Meta Data"].getboolean("createSymlink")

# ATTENTION: Changes in optionalSentinelParameters requires manual removal of existing database before starting script!!!
# optional landsat parameters => max_cloud_cover (value in db stored as cloudcoverpercentage)
optionalSentinelParameters = ["polarisationmode", "producttype", "sensoroperationalmode", "swathidentifier", "cloudcoverpercentage", "timeliness"]

# small preview image
try:
	resizePreviewImage = userConfig["Small Preview Image"].getboolean("resizePreviewImage")
except:
	resizePreviewImage = defaultConfig["Small Preview Image"].getboolean("resizePreviewImage")
try:
	widthPreviewImageSmall = userConfig["Small Preview Image"].getint("widthPreviewImageSmall")
except:
	widthPreviewImageSmall = defaultConfig["Small Preview Image"].getint("widthPreviewImageSmall")	
try:
	heightPreviewImageSmall = userConfig["Small Preview Image"].getint("heightPreviewImageSmall")
except:
	heightPreviewImageSmall = defaultConfig["Small Preview Image"].getint("heightPreviewImageSmall")

# combined preview images
try:
	combinedPreview = userConfig["Combined Preview Images"].getboolean("combinedPreview")
except:
	combinedPreview = defaultConfig["Combined Preview Images"].getboolean("combinedPreview")
try:
	previewBorder = userConfig["Combined Preview Images"].getint("previewBorder")
except:
	previewBorder = defaultConfig["Combined Preview Images"].getint("previewBorder")
try:
	red = userConfig["Combined Preview Images"].getint("previewBackgroundR")
except:
	red = defaultConfig["Combined Preview Images"].getint("previewBackgroundR")
try:
	green = userConfig["Combined Preview Images"].getint("previewBackgroundG")
except:
	green = defaultConfig["Combined Preview Images"].getint("previewBackgroundG")
try:
	blue = userConfig["Combined Preview Images"].getint("previewBackgroundB")
except:
	blue = defaultConfig["Combined Preview Images"].getint("previewBackgroundB")	
previewBackground = (red,green,blue)
try:
	previewTextOnImage = userConfig["Combined Preview Images"].getboolean("previewTextOnImage")
except:
	previewTextOnImage = defaultConfig["Combined Preview Images"].getboolean("previewTextOnImage")
try:
	previewImageFontSize = userConfig["Combined Preview Images"].getint("previewImageFontSize")
except:
	previewImageFontSize = defaultConfig["Combined Preview Images"].getint("previewImageFontSize")	
try:
	previewImagesCombined = userConfig["Combined Preview Images"].getint("previewImagesCombined")
except:
	previewImagesCombined = defaultConfig["Combined Preview Images"].getint("previewImagesCombined")		
