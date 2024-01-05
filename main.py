"""SQS Listener"""
# pylint: disable = W0702
import json
import logging
import os
import platform
import subprocess
import traceback
from signal import SIGINT, SIGTERM, signal

import boto3
import docker
import psutil
import requests
import watchtower
from docker.errors import ContainerError
from docker.types import LogConfig, Mount

MEM_LIMIT = os.getenv("MEM_LIMIT", "8g")
MEMSWAP_LIMIT = os.getenv("MEMSWAP_LIMIT", "8g")
AWS_ACCOUNT = os.getenv("AWS_ACCOUNT", "910371487650")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
QUEUE_NAME = os.getenv("SQS_QUEUE", "atomQueueStandard")
WAIT_TIME = int(os.getenv("WAIT_TIME", "10"))
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "3600"))
LOGS_GROUP = os.getenv("LOGS_GROUP", "atom_workers")
ROOT_FOLDER = "/tmp"
STANDARD_MOUNT_PATH = "/data"
MODELS_MOUNT_PATH = "/models"
MODELS_FOLDER = "/"


class SignalHandler:
    """Handle signals"""

    def __init__(self):
        self.received_signal = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_signal = True


def get_system_info():
    """Retrieve system information"""
    try:
        info = {}
        info["platform"] = platform.system()
        info["platform-release"] = platform.release()
        info["platform-version"] = platform.version()
        info["architecture"] = platform.machine()
        info["processor"] = platform.processor()
        info["cpu_count"] = psutil.cpu_count()
        info["cpu_percent"] = psutil.cpu_percent()
        info["mem"] = str(round(psutil.virtual_memory().total / (1024.0**3))) + " GB"
        info["mem_percent"] = psutil.virtual_memory().percent
        info["disk"] = str(round(psutil.disk_usage('/').total / (1024.0**3))) + " GB"
        info["disk_percent"] = psutil.disk_usage('/').percent

        return info
    except Exception as e:
        logging.exception(e)
        return {}


def get_instance_data():
    """Retreive instance metadata"""
    info = get_system_info()
    url = "http://169.254.169.254/latest/api/token"
    header = {"X-aws-ec2-metadata-token-ttl-seconds": "21600"}
    res = requests.put(url, headers=header, timeout=5)  # Add timeout argument
    if res.status_code != 200:
        print("*Warning* Could not retrieve token")
        return info

    token = res.content.decode("utf-8")
    header = {"X-aws-ec2-metadata-token": token}
    keys = [
        "instance-id",
        "hostname",
        "placement/region",
        "placement/availability-zone",
        "public-ipv4",
        "system" "ami-id",
    ]

    for key in keys:
        url = f"http://169.254.169.254/latest/meta-data/{key}"
        res = requests.get(url, headers=header, timeout=5)
        if res.status_code == 200:
            info[key] = res.content.decode("utf-8")

    return info


def set_logger(task_id, level='info'):
    """Setup logging to cloudwatch"""
    logging_levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }
    logging.basicConfig(level=logging_levels[level])
    handler = watchtower.CloudWatchLogHandler(
        log_group_name=LOGS_GROUP, log_stream_name=task_id
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        "%Y-%m-%d %H:%M:%S %Z",
    )
    handler.setFormatter(formatter)
    logging.getLogger(task_id).addHandler(handler)


def status_update(task_id, message):
    """Update status in DB"""
    logger = logging.getLogger(task_id)
    logger.info("TODO: Updating status %s:%s", task_id, message)


def run_container(dkr, task_id):
    """Pull the image and run the container"""
    logger = logging.getLogger(task_id)
    try:
        # Create the mounts
        task_folder = os.path.join(ROOT_FOLDER, task_id)
        mounts = [
            Mount(
                source=f"{task_folder}", target=f"{STANDARD_MOUNT_PATH}", type="bind"
            ),
            Mount(
                source=f"{MODELS_FOLDER}", target=f"{MODELS_MOUNT_PATH}", type="bind"
            ),
        ]
        env = dict(os.environ)
        lc = LogConfig(type=LogConfig.types.JSON, config={"max-file": "100"})

        device_requests = []
        try:
            _ = subprocess.check_output("nvidia-smi")
            device_requests = [
                docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])
            ]
            logger.info("GPU Found. Adding nvidia runtime")
        except:
            logger.info("GPU not found.")

        # Create client
        client = docker.from_env()
        logger.info("Created docker client")

        # Pull Image
        image = dkr["image"]
        tag = dkr["tag"]
        client.images.pull(image, tag)
        logger.info("Pulled Image: %s:%s", image, tag)

        # Run the container
        logger.info("Container running...")
        container = client.containers.run(
            f"{image}:{tag}",
            name=f"{task_id}",
            mounts=mounts,
            environment=env,
            network_mode="host",
            log_config=lc,
            mem_limit=MEM_LIMIT,
            memswap_limit=MEMSWAP_LIMIT,
            device_requests=device_requests,
        )

        if isinstance(container, bytes):
            logger.info("=============================================")
            logs = container.decode("utf-8")
            if len(logs):
                logger.info(logs)
            logger.info("=============================================")

            return {"success": True}

    except ContainerError as cerr:
        logger.error(cerr.container.logs())
        return {"success": False, "function": "<container>", "err": cerr}

    except Exception as ex:
        logger.exception(ex)
        logger.error(traceback.format_exc())
        return {"success": False, "function": "run_container", "err": ex}


def process_message(msg):
    """Process message"""
    task_id = msg.message_id
    event = json.loads(msg.body)
    dkr = event["docker"]
    config = event.get("config", {})

    # Setup cloudwatch logger
    set_logger(task_id, event.get('log_level', "info"))
    logger = logging.getLogger(task_id)

    # Log system info
    logger.info(get_instance_data())

    # Update status
    status_update(task_id, "running")

    # Get input
    logger.debug("Parsed Message: \nID: %s \nCONFIG: %s", task_id, config)

    # Create all task folders
    task_folder = os.path.join(ROOT_FOLDER, task_id)
    input_folder = os.path.join(task_folder, "input")
    logs_folder = os.path.join(task_folder, "logs")
    output_folder = os.path.join(task_folder, "output")
    for folder in [input_folder, logs_folder, output_folder]:
        logger.debug("Creating folder: %s", folder)
        os.makedirs(folder)

    # Write the task config
    config_file = os.path.join(input_folder, "task_config.json")
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    logger.info("Wrote %s", config_file)

    # Run the docker processor
    result = run_container(dkr, task_id)

    # Update success/failure status
    if result["success"]:
        status_update(task_id, "completed")
    else:
        status_update(task_id, "failed")

    # return result
    return result


#############################################################################
# MAIN ENTRY POINT
#############################################################################
def main():
    """Main fn: SQS Listener and message processor"""
    try:
        print("ATOM Worker listening for messages")
        signal_handler = SignalHandler()
        queue_url = f"https://sqs.{AWS_REGION}.amazonaws.com/{AWS_ACCOUNT}/{QUEUE_NAME}"
        sqs = boto3.resource("sqs")
        queue = sqs.Queue(queue_url)

        # Continuously poll (long polling) for messages until SIGTERM/SIGKILL
        while not signal_handler.received_signal:
            messages = queue.receive_messages(
                MaxNumberOfMessages=1,
                WaitTimeSeconds=WAIT_TIME,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
            )
            for message in messages:
                process_message(message)
                message.delete()

    except Exception as err:
        print("**ERROR in Worker Main Function**", err)
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
