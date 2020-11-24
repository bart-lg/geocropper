#!/bin/bash
#
# Use flock to prevent parallel runnings if using crontab!
# Running script every 35 minutes is recommended, since ESA allows only one archive download request every 30 minutes.
#
# */35 * * * * /usr/bin/flock -n /tmp/geocropper.cron.lock path_to_this_script

# change working directory to the directory of the script
cd "$(dirname "${BASH_SOURCE[0]}")"

# read env name out of user-config.ini
source <(grep "conda_env" ../user-config.ini  | tr -d " ")

if [[ ! -z "$conda_env" ]]
then

	# activate conda environment
	source /opt/miniconda3/bin/activate "${conda_env}"

	# defining output file for console output
	OUTPUT_FILE="console-outputs/console-output_$(date +%Y-%m-%d_%H-%M-%S).txt"

	# run script
	python3 check_requested_downloads_and_crop.py &> "$OUTPUT_FILE"

else

	echo "ERROR: No conda environment specified in user-config.ini!"

fi