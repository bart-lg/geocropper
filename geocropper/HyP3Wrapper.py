# import geocropper.config as config
# import logging

# (not working with python 3.7)
# from hyp3_sdk import HyP3 

# # get logger object
# logger = logging.getLogger('root')

# class HyP3Wrapper:

# 	def __init__(self):

#         logger.info("connect to ASF HyP3 API")
        
#         self.api = HyP3(username=config.asfUser, password=config.asfPW)

#         logger.info("ASF HyP3 API connected")


#     def requestTile(self, granule, jobname):

#     	# jobname must be less than 20 characters
#     	return self.api.submit_rtc_job(granule=granule, name=jobname)

#     def readyForDownload(self, job):

#     	# self.api.refresh(job)
#     	# find out how to determine if download is ready or not
