import logging
import logging.handlers
import geocropper.config as config

def setup_custom_logger(name):

    # logger settings
    # 100 MB
    log_file_max_size = 1024 * 1024 * 100 
    log_num_backups = 3
    log_format = "%(asctime)s [%(levelname)s]: %(filename)s(%(funcName)s:%(lineno)s) >> %(message)s"
    # w: overwrite; a: append
    log_filemode = "a" 

    # setup logger
    if config.loggingMode == "DEBUG":
        level = logging.DEBUG
    if config.loggingMode == "INFO":
        level = logging.INFO
    if config.loggingMode == "WARNING":
        level = logging.WARNING
    if config.loggingMode == "ERROR":
        level = logging.ERROR
    if config.loggingMode == "CRITICAL":
        level = logging.CRITICAL

    logging.basicConfig(filename=config.logFile, format=log_format, filemode=log_filemode ,level=level)
    rotate_file = logging.handlers.RotatingFileHandler(
        config.logFile, maxBytes=log_file_max_size, backupCount=log_num_backups
    )
    logger = logging.getLogger(name)
    logger.addHandler(rotate_file)

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