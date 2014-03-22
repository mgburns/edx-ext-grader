import json
import logging
import time

import pika

from .conf import settings
from .xqueue import XQueueClient

log = logging.getLogger(__name__)


class XQueueListener(object):
    SLEEP_INTERVAL = 1

    def __init__(self):
        """
        Initialize a grader queue

        Used for both producing and consuming submissions
        """
        self.xqueue = XQueueClient()

        # Setup RabbitMQ connection for this grader
        credentials = pika.PlainCredentials(settings.RABBITMQ_USER, settings.RABBITMQ_PASSWORD)

        try:
            # Establish a connection
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=settings.RABBITMQ_HOST,
                                          port=settings.RABBITMQ_PORT,
                                          virtual_host=settings.RABBITMQ_VHOST,
                                          credentials=credentials)
                )

        except pika.exceptions.AMQPConnectionError:
            log.exception("XQueue consumer could not connect to submission broker")
            raise

        # Establish a connection channel
        self.channel = self.connection.channel()

    def start(self):
        self._is_polling = True

        while self._is_polling:
            submission = self.xqueue.get_submission()

            if submission:
                self.publish(submission)
            else:
                time.sleep(self.SLEEP_INTERVAL)

    def publish(self, submission):
        """
        Add submissions to the queue for handling by workers
        """
        try:
            payload = json.loads(submission['xqueue_body']['grader_payload'])
            grader = payload['grader']
        except Exception as e:
            log.exception('Could not extract submission destination: %s', submission)
            raise

        # TODO: Validate grader key against registry

        # Ensure the submission queue exists
        queue = self.channel.queue_declare(queue=grader, durable=True)

        self.channel.basic_publish(
            exchange="",
            routing_key=grader,
            body=json.dumps(submission),
            properties=pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2)
            )

        return queue.method.message_count

    def submission_count(self, grader):
        queue = self.channel.queue_declare(queue=grader, durable=True)
        return queue.method.message_count


def start_listener():
    listener = XQueueListener()
    listener.start()
