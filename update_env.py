"""Python script to update environment variables"""
import json
import os
import shutil

import boto3
import psutil

MODELS_BUCKET = "t2d2-ai-models"
MODELS_PATH = "/opt/atom/models"
APPDATA_PATH = "/opt/atom/tasks"
SECRETS_ARN = "arn:aws:secretsmanager:us-east-1:910371487650:secret:atom/host-bopG5K"
MIN_DISKSPACE = 20 * 1024 * 1024 * 1024


def get_secrets(secrets_arn=SECRETS_ARN):
    """Get secrets for environment"""
    if not secrets_arn:
        return {}
    print(f"Retreiving secrets from {secrets_arn}")
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secrets_arn)
    return json.loads(response["SecretString"])


def env_update():
    """Update env"""
    print("Writing .env")
    with open(".env", "w", encoding="utf-8") as f:
        # Update from Secrets
        secrets = get_secrets()
        for key, val in secrets.items():
            f.write(f"{key}={val}\n")

        # Update from psutil
        memory = psutil.virtual_memory().available / (1024 * 1024 * 1024)
        memlimit = round(memory * 0.9, 1)
        f.write(f"MEM_LIMIT={memlimit}g\n")
        f.write(f"MEMSWAP_LIMIT={memlimit}g\n")
        f.write(f"APPDATA_PATH={APPDATA_PATH}\n")
        f.write(f"MODELS_PATH={MODELS_PATH}\n")

    return


def disk_cleanup(tasksdir=APPDATA_PATH):
    """Cleanup tasks folder if space is below threshold"""
    print("Disk cleanup")
    available = psutil.disk_usage("/").free
    if available < MIN_DISKSPACE:
        for folder in os.listdir(tasksdir):
            print(f"Deleting: {os.path.join(tasksdir, folder)}")
            shutil.rmtree(os.path.join(tasksdir, folder))
    return


def get_models(bucket=MODELS_BUCKET, models=MODELS_PATH):
    """Get models from store"""
    # Get list of objects from bucket and store in MODELS_PATH
    if not bucket:
        return
    print("Getting Models")
    client = boto3.client("s3")
    response = client.list_objects(Bucket=bucket)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        savefile = os.path.join(models, key)
        if not os.path.exists(savefile):
            print(f"Bucket:{bucket} Key:{key} Savefile:{savefile}")
            os.makedirs(os.path.dirname(savefile), exist_ok=True)
            client.download_file(bucket, key, savefile)

    return


if __name__ == "__main__":
    os.makedirs(APPDATA_PATH, exist_ok=True)
    os.makedirs(MODELS_PATH, exist_ok=True)
    disk_cleanup()
    get_models()
    env_update()
