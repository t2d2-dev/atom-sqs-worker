"""SQS Listener"""
import os
from signal import SIGINT, SIGTERM, signal
import traceback
import boto3

QUEUE_URL = os.getenv("SQS_QUEUE", "https://sqs.us-east-1.amazonaws.com/910371487650/atomQueueStandard")
WAIT_TIME = int(os.getenv("WAIT_TIME", "10"))
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "3600"))

sqs = boto3.resource('sqs')
queue = sqs.Queue(QUEUE_URL)

# queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)


class SignalHandler:
    """Handle signals"""

    def __init__(self):
        self.received_signal = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_signal = True


def process_message(msg):
    """Process message"""
    print(f"Received Message: ID: {msg.message_id} Body: {msg.body}")
    # Read the config

    # Create a task folder

    # Initialize the docker processor

    # Run the docker processor

    # Update success/failure status

    # return result
    return


if __name__ == "__main__":
    signal_handler = SignalHandler()
    while not signal_handler.received_signal:
        messages = queue.receive_messages(
            MaxNumberOfMessages=1,
            WaitTimeSeconds=WAIT_TIME,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
        )
        for message in messages:
            try:
                process_message(message)
                message.delete()
            except Exception as err:
                print("**ERROR Processing Message**")
                print(traceback.format_exc())

