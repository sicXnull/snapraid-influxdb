import os
import re
import subprocess
import logging
from influxdb import InfluxDBClient
from datetime import datetime

script_log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'snapraid-influxdb.log')
logging.basicConfig(filename=script_log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w')

def log_exception(message):
    logging.exception(message)

try:
    # InfluxDB connection
    host = 'localhost'  # InfluxDB host
    port = 8086         # InfluxDB HTTP API port
    database = 'snapraid_influxdb'  # InfluxDB database name
    username = 'user'  # InfluxDB username
    password = 'password'  # InfluxDB password

    client = InfluxDBClient(host, port, username, password, database=database)

    # Check if the database exists, and create it if not
    if database not in client.get_list_database():
        client.create_database(database)
        logging.info(f'Database "{database}" created')

    log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'snapraid-smart.log')

    # Generate the log file using the 'snapraid smart' command
    snapraid_command = f"snapraid smart > {log_file_path}"
    subprocess.run(snapraid_command, shell=True, check=True)
    logging.info('Log file generated successfully')

    with open(log_file_path, 'r') as log_file:
        lines = log_file.readlines()

    data_points = []

    # Regex
    pattern = r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)%\s+(\d+\.\d+)\s+(\S+)\s+(\S+)\s+(\S+)'

    header_found = False
    for line in lines:
        if "Temp  Power   Error   FP Size" in line:
            header_found = True
            continue

        if header_found:
            match = re.match(pattern, line.strip())
            if match:
                temp, power, error, fp, size, serial, device, disk = match.groups()
                failure_percentage = int(fp)

                data_point = {
                    "measurement": "snapraid_smart",
                    "tags": {
                        "disk": disk
                    },
                    "time": datetime.utcnow().isoformat(),
                    "fields": {
                        "temp": int(temp),
                        "power": int(power),
                        "error": int(error),
                        "fail_percentage": failure_percentage,
                        "size": float(size)
                    }
                }
                data_points.append(data_point)

                logging.debug(
                    f'Added data point for disk {disk}: temp={temp}, power={power}, error={error}, fail_percentage={failure_percentage}, size={size}')

            else:
                logging.warning(f"No match for line: {line.strip()}")

    client.write_points(data_points)
    logging.info(f'{len(data_points)} data points written to InfluxDB')

except Exception as e:
    log_exception(f'Error: {str(e)}')

finally:
    client.close()

logging.info('Script finished')
