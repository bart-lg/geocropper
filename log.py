import logging
import logging.handlers
import config

def setupCustomLogger(name):

    # logger settings
    logFileMaxSize = 1024 * 1024 * 100 # megabytes
    logNumBackups = 3
    logFormat = "%(asctime)s [%(levelname)s]: %(filename)s(%(funcName)s:%(lineno)s) >> %(message)s"
    logFilemode = "a" # w: overwrite; a: append

    # setup logger
    logging.basicConfig(filename=config.logFile, format=logFormat, filemode=logFilemode ,level=logging.DEBUG)
    rotateFile = logging.handlers.RotatingFileHandler(
        config.logFile, maxBytes=logFileMaxSize, backupCount=logNumBackups
    )
    logger = logging.getLogger(name)
    logger.addHandler(rotateFile)

    # print log messages to console
    #consoleHandler = logging.StreamHandler()
    #logFormatter = logging.Formatter(log_format)
    #consoleHandler.setFormatter(logFormatter)
    #logger.addHandler(consoleHandler)

    return logger

# source: https://docs.python.org/2/howto/logging.html
# logger.debug("")      // Detailed information, typically of interest only when diagnosing problems.
# logger.info("")       // Confirmation that things are working as expected.
# logger.warning("")    // An indication that something unexpected happened, or indicative of some problem in the near future
# logger.error("")      // Due to a more serious problem, the software has not been able to perform some function.
# logger.critical("")   // A serious error, indicating that the program itself may be unable to continue running.