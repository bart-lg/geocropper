from configparser import ConfigParser
import pathlib
import logging

# get logger object
logger = logging.getLogger('root')

try: 

	logger.debug("Start loading config")

	config = ConfigParser()
	config.read([
		pathlib.Path(__file__).parent.parent / "default-config.ini",
		pathlib.Path(__file__).parent.parent / "user-config.ini"
	])

	# used for download of Sentinel data
	copernicusUser = config["Credentials"]["copernicusUser"]
	copernicusPW = config["Credentials"]["copernicusPW"]
	copernicusURL = config["URLs"]["copernicusURL"]

	usgsUser = config["Credentials"]["usgsUser"]
	usgsPW = config["Credentials"]["usgsPW"]

	# used for download of Sentinel-1 data from the Alaska Satellite Facility
	asfUser = config["Credentials"]["asfUser"]
	asfPW = config["Credentials"]["asfPW"]

	# miscellaneous variables
	copernicusRequestDelay = config["Misc"].getint("copernicusRequestDelay")
	copernicusRepeatRequestAfterMin = config["Misc"].getint("copernicusRepeatRequestAfterMin")
	covertS1CropsToUTM = config["Misc"].getboolean("covertS1CropsToUTM")
	databaseTimeout = config["Misc"].getint("databaseTimeout")
	databaseRetryQueries = config["Misc"].getint("databaseRetryQueries")

	# various data paths
	dataDir = pathlib.Path(config["Paths"]["data"])
	bigTilesDir = pathlib.Path(config["Paths"]["bigTiles"])
	croppedTilesDir = pathlib.Path(config["Paths"]["croppedTiles"])
	csvInputDir = pathlib.Path(config["Paths"]["csvInput"])
	csvArchiveDir = pathlib.Path(config["Paths"]["csvArchive"])
	logFile = pathlib.Path(config["Paths"]["logFile"])

	# shape file used to determine country for geolocation
	worldBordersShapeFile = str(pathlib.Path(config["Paths"]["worldBordersShapeFile"]))

	# path and filename of sqlite database file
	dbFile = pathlib.Path(config["Paths"]["dbFile"])

	# path and filename of SNAP Graph Processing Tool (GPT)
	gptSnap = pathlib.Path(config["Paths"]["gptSnap"])

	# path and filename of XML file for SNAP Graph Processing Tool (GPT)
	xmlSnap = pathlib.Path(config["Paths"]["xmlSnap"])


	# logging modes: DEBUG, INFO, WARNING, ERROR, CRITICAL
	loggingMode = config["Logging"]["loggingMode"]


	# copy metadata from bigTiles to croppedTiles
	copyMetadata = config["Meta Data"].getboolean("copyMetadata")

	# creates symlink within croppedTiles to bigTiles
	createSymlink = config["Meta Data"].getboolean("createSymlink")

	# optional landsat parameters => max_cloud_cover (value in db stored as cloudcoverpercentage)
	optionalSentinelParameters = ["polarisationmode", "producttype", "sensoroperationalmode", 
		"swathidentifier", "cloudcoverpercentage", "timeliness", "orbitdirection", "filename"]

	# small preview image
	resizePreviewImage = config["Small Preview Image"].getboolean("resizePreviewImage")
	widthPreviewImageSmall = config["Small Preview Image"].getint("widthPreviewImageSmall")
	heightPreviewImageSmall = config["Small Preview Image"].getint("heightPreviewImageSmall")

	# combined preview images
	combinedPreview = config["Combined Preview Images"].getboolean("combinedPreview")
	previewBorder = config["Combined Preview Images"].getint("previewBorder")
	red = config["Combined Preview Images"].getint("previewBackgroundR")
	green = config["Combined Preview Images"].getint("previewBackgroundG")
	blue = config["Combined Preview Images"].getint("previewBackgroundB")
	previewBackground = (red,green,blue)
	previewTextOnImage = config["Combined Preview Images"].getboolean("previewTextOnImage")
	previewImageFontSize = config["Combined Preview Images"].getint("previewImageFontSize")
	previewImagesCombined = config["Combined Preview Images"].getint("previewImagesCombined")
	previewCenterDot = config["Combined Preview Images"].getboolean("previewCenterDot")

	logger.info("Config loaded")

except Exception as e:

	print(str(e))
	logger.critical(f"Error in loading config: {repr(e)}")
	raise SystemExit  
