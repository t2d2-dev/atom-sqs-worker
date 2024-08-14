"""SQS Listener"""

# pylint: disable = W0702
import json
import logging
import os
import platform
import subprocess
import traceback
from datetime import datetime
from signal import SIGINT, SIGTERM, signal
from time import sleep

import boto3
import docker
import psutil
import requests
import watchtower
from docker.errors import ContainerError
from docker.types import Mount
from dotenv import load_dotenv
from pymongo import MongoClient

from update_env import APPDATA_PATH, disk_cleanup

load_dotenv()  # take environment variables from .env.

# AWS parameters (for SQS and CloudWatch)
AWS_ACCOUNT = os.getenv("AWS_ACCOUNT", "910371487650")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
QUEUE_NAME = os.getenv("AWS_SQS_QUEUE", "atomQueueStandard")
SECRETS_ARN = os.getenv("AWS_SECRETS_ARN", None)
LOGS_GROUP = os.getenv("AWS_CW_LOGGROUP", "atom_workers")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
AUTOSCALING_GROUP_NAME = os.getenv("AUTOSCALING_GROUP_NAME", "ATOM worker asg")

# Limits to docker container
MEM_LIMIT = os.getenv("MEM_LIMIT", "8g")
MEMSWAP_LIMIT = os.getenv("MEMSWAP_LIMIT", "8g")

# Queue listener params
WAIT_TIME = int(os.getenv("WAIT_TIME", "10"))
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "7200"))

# Paths and mounts
APPDATA_FOLDER = os.getenv("APPDATA_PATH", "/opt/tasks")
MODELS_FOLDER = os.getenv("MODELS_PATH", "/opt/models")
APPDATA_MOUNT_PATH = "/data"
MODELS_MOUNT_PATH = "/models"


class SignalHandler:
    """Handle signals"""

    def __init__(self):
        self.received_signal = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_signal = True


def get_secrets(secrets_arn=SECRETS_ARN):
    """Get secrets for environment"""
    if not secrets_arn:
        return {}
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secrets_arn)
    return json.loads(response["SecretString"])


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
        info["disk"] = str(round(psutil.disk_usage("/").total / (1024.0**3))) + " GB"
        info["disk_percent"] = psutil.disk_usage("/").percent

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


def set_instance_protection(protect=True):
    """Set instance protection"""
    try:
        client = boto3.client("autoscaling")
        instance_id = get_instance_data()["instance-id"]
        client.set_instance_protection(
            InstanceIds=[instance_id],
            AutoScalingGroupName=AUTOSCALING_GROUP_NAME,
            ProtectedFromScaleIn=protect,
        )
        return True
    except Exception as err:
        print("*WARNING* Could not set instance protection ", err)
        print(traceback.format_exc())
        return False


def set_logger(task_id, level="info"):
    """Setup logging to cloudwatch"""
    logging_levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
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


def status_update(
    task_id, status, project_id=0, sysinfo=None, env="dev", task_envvars=None
):
    """Update status in DB
    TODO:
    - Cleanup arguments: env, task_envvars, project_id, ENVIRONMENT
    - Remove dependency on MongoDB
    """
    logger = logging.getLogger(task_id)

    try:
        logger.info("Updating status %s:%s", task_id, status)
        mongo = MongoClient(MONGO_URL)
        db = mongo[f"t2d2-v3-{env}-db"]
        collection = db["tasks"]
        now = datetime.now()

        # Insert if not found
        task = collection.find_one({"task_id": task_id})

        payload = {"status": status}
        if sysinfo:
            payload["agent"] = sysinfo

        if status == "running":
            delta = now - task.get("created_on", now)
            payload["duration_queue"] = delta.days * 86400 + delta.seconds

        elif status == "completed":
            delta = now - task.get("updated_on", now)
            payload["duration"] = delta.days * 86400 + delta.seconds

        # result = collection.update_one({"task_id": task_id}, {"$set": update})
        if task_envvars is None:
            task_envvars = {}
        base_url = task_envvars.get("T2D2_API_URL", "https://api-v3-dev.t2d2.ai/api/")
        task_owner_token = task_envvars.get("TASK_OWNER_TOKEN", None)
        url = base_url + f"{project_id}/task/update/{task_id}"
        headers = {"Content-Type": "application/json", "x-api-key": task_owner_token}
        res = requests.put(url, json=payload, headers=headers, timeout=30)
        logger.info(res.json())

        return

    except Exception as err:
        logger.warning("*WARNING* Could not update task %s", err)
        logger.warning(traceback.format_exc())


def check_message_status(task_id, env="dev"):
    """Check to see if message is stopped / cancelled"""
    try:
        logger = logging.getLogger(task_id)
        logger.info("Checking task status %s : env %s", task_id, env)
        mongo = MongoClient(MONGO_URL)
        db = mongo[f"t2d2-v3-{env}-db"]
        collection = db["tasks"]

        now = datetime.now()

        # Insert if not found
        task = collection.find_one({"task_id": task_id})
        if not task:
            collection.insert_one(
                {
                    "project_id": 0,
                    "created_by": "unknown",
                    "task_name": "unknown",
                    "task_id": task_id,
                    "status": "running",
                    "created_at": now,
                    "updated_at": now,
                    "duration_queue": 0,
                    "duration": 0,
                    "agent": {},
                }
            )
            return False

        task_status = task.get("status", "running")
        if task_status in ("cancelled", "stopped", "killed"):
            return True

        return False
    except Exception as err:
        print("*WARNING* Could not check task status ", err)
        print(traceback.format_exc())
        return False


def run_container(dkr, task_id, envvars=None):
    """Pull the image and run the container"""
    logger = logging.getLogger(task_id)
    try:
        # Create the mounts
        task_folder = os.path.join(APPDATA_FOLDER, task_id)
        logger.info("Creating task_dir %s", task_folder)

        # Mounts
        mounts = [
            Mount(source=f"{task_folder}", target=f"{APPDATA_MOUNT_PATH}", type="bind"),
            Mount(
                source=f"{MODELS_FOLDER}", target=f"{MODELS_MOUNT_PATH}", type="bind"
            ),
        ]
        logger.info("Created mounts")

        # Environment variables from secrets
        env = get_secrets()
        if envvars:
            env.update(envvars)
        logger.info("Updated env vars")
        logger.debug("Env = %s", env)

        # Check for GPU
        device_requests = []
        try:
            _ = subprocess.check_output("nvidia-smi")
            device_requests = [
                docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])
            ]
            logger.info("GPU Found. Adding nvidia runtime")
        except:
            logger.info("GPU not found.")

        # Create client (Sometimes docker takes time to get started. So wait and try)
        max_attempts = 5
        for i in range(max_attempts):
            try:
                client = docker.from_env()
                logger.info("Created docker client")
                break
            except Exception as ex:
                logger.warning(
                    "Docker startup failed. Attempt %d of %d: %s", i, max_attempts, ex
                )
                sleep(5)
                if i == max_attempts:
                    logger.error("Could not initialize Docker")
                    logger.error(ex)
                    return {
                        "success": False,
                        "function": "docker_initialize",
                        "err": ex,
                    }

        # Pull Image
        image = dkr["image"]
        tag = dkr["tag"]
        try:
            client.images.pull(image, tag)
            logger.info("Pulled Image: %s:%s", image, tag)
        except Exception as ex:
            logger.error("Could not pull image %s:%s", image, tag)
            logger.error(ex)
            return {"success": False, "function": "pull_image", "err": ex}

        # Run the container
        logger.info("Container running...")
        try:
            container = client.containers.run(
                f"{image}:{tag}",
                name=f"{task_id}",
                mounts=mounts,
                environment=env,
                network_mode="host",
                mem_limit=MEM_LIMIT,
                memswap_limit=MEMSWAP_LIMIT,
                device_requests=device_requests,
                detach=True,
            )

            # Wait for container to be done
            result = container.wait()
            logger.info("=============================================")
            logs = container.logs().decode("utf-8")
            if len(logs):
                logger.info(logs)
            logger.info("=============================================")

            if result["StatusCode"] != 0:
                logger.error(
                    "Container exited with error code %s", result["StatusCode"]
                )
                return {"success": False, "function": "run_container", "err": result}

            return {"success": True}

        except Exception as ex:
            logger.error(traceback.format_exc())
            logger.error("Could not run container %s:%s", image, tag)
            logger.error(ex)
            return {"success": False, "function": "run_container", "err": ex}

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
    project_id = config.get("project_id", 0)
    task_env = event.get("env", "dev")
    task_envvars = event.get("env_vars", None)

    try:
        # Setup cloudwatch logger
        set_logger(task_id, event.get("log_level", "info"))
        logger = logging.getLogger(task_id)

        # Log system info
        sysinfo = get_instance_data()
        logger.info(sysinfo)

        # Log task info
        logger.info("Task ID: %s", task_id)
        logger.info("Project ID: %s", project_id)
        logger.info("Docker: %s", dkr)
        logger.info("Config: ")
        logger.info(config)

        # Check to see if message is cancelled or stopped
        if task_env.startswith("dev"):
            task_env = "dev"
        elif task_env.startswith("prod"):
            task_env = "prod"
        if check_message_status(task_id, env=task_env):
            return {
                "success": False,
                "function": "process_message",
                "err": "Task is cancelled",
            }

        # Update status
        status_update(
            task_id,
            status="running",
            project_id=project_id,
            sysinfo=sysinfo,
            env=task_env,
            task_envvars=task_envvars,
        )

        # Get input
        logger.debug("Parsed Message: \nID: %s \nCONFIG: %s", task_id, config)

        # Create all task folders
        task_folder = os.path.join(APPDATA_FOLDER, task_id)
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
        if not task_envvars:
            task_envvars = {"ENVIRONMENT": task_env}
        else:
            task_envvars.update({"ENVIRONMENT": task_env})

        result = run_container(dkr, task_id, task_envvars)

        # Update success/failure status
        if result["success"]:
            status_update(
                task_id,
                "completed",
                project_id=project_id,
                env=task_env,
                task_envvars=task_envvars,
            )
        else:
            status_update(
                task_id,
                "failed",
                project_id=project_id,
                env=task_env,
                task_envvars=task_envvars,
            )

        # return result
        return result

    except Exception as err:
        print("**ERROR in Worker Main Function**", err)
        print(traceback.format_exc())
        status_update(
            task_id,
            "failed",
            project_id=project_id,
            env=task_env,
            task_envvars=task_envvars,
        )
        return {"success": False, "function": "process_message", "err": err}


#############################################################################
# MAIN QUEUE LISTENER
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
        count, heartbeat = 0, ["_", "-"]
        while not signal_handler.received_signal:
            count += 1
            print(heartbeat[count % 2], end="")
            if count % 80 == 0:
                print("")
                disk_cleanup(APPDATA_PATH)

            messages = queue.receive_messages(
                MaxNumberOfMessages=1,
                WaitTimeSeconds=WAIT_TIME,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
            )
            for message in messages:
                print("\nRECEIVED: ", message.message_id, "\nINSTANCE PROTECTED")
                set_instance_protection(protect=True)
                result = process_message(message)
                message.delete()  # Delete message regardless of success/failure
                set_instance_protection(protect=False)
                print("COMPLETED: ", result, "\nINSTANCE UNPROTECTED")

    except Exception as err:
        print("**ERROR in Worker Main Function**", err)
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
