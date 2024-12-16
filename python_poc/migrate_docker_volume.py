import argparse
import subprocess
import json
import sys
import pprint

from loguru import logger
from datetime import datetime

# Generates the docker command to generate the backup temp file on the root server

parser = argparse.ArgumentParser()
parser.add_argument("machine_user", help="The user that will be used to ssh'd into the external machine")
parser.add_argument("machine_host", help="The ip address or url of the machine that will be ssh'ed into")
parser.add_argument("volume_name", help="The name/id of the docker volume to be coppied")
parser.add_argument("backup_path", help="The full path accessable to the local machine where the data will be backed up")

args = parser.parse_args()

docker_volume_info_output = subprocess.Popen(f"ssh {args.machine_user}@{args.machine_host} docker inspect {args.volume_name}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

logger.info(f"Result from ssh docker volume inspect: \n {docker_volume_info_output}")

docker_vol_info = json.loads(docker_volume_info_output[0].decode("utf-8"))

logger.info("Decoded docker volume info json: ")
pprint.pprint(docker_vol_info[0])

logger.info(f"Trying to back up volume: {docker_vol_info[0]['Mountpoint']}")

# Actual data transfer:
backup_name = f"{datetime.today().strftime('%Y-%m-%d')}_{docker_vol_info[0]['Name']}"

docker_backup_cmd = f'''
docker run --rm \
  -v {docker_vol_info[0]['Name']}:/volume \
  -v {args.backup_path}:/backup \
  busybox \
  tar cvf /backup/{backup_name}.tar /volume
'''

print(docker_backup_cmd)

# Copy volume through SSH:
# ssh user@remote tar czf - /my/directory/ > /my/local/destination/archive.tgz