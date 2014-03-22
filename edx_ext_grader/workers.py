"""
Grader

The grader workers pop student submissions off their configured submit queue.

Each worker is tied to a specific grader in the graders module.

"""
import json
import logging
import multiprocessing
import pika

from .conf import settings
from . import graders

log = logging.getLogger(__name__)


class GraderManager():
    def __init__(self, grader_name, worker_count=5):
        self.workers = []
        for num in range(worker_count):
            self.workers.append(GraderWorker(grader_name))

    def start(self):
        """ Starts a pool of grader worker processes to handle submissions """
        for worker in self.workers:
            worker.daemon = True
            worker.start()


class GraderWorker(multiprocessing.Process):

    # We can handle one submission at a time.
    # Next step -- use an async connection and spawn threads to handle submissions
    PREFETCH_COUNT = 1

    def __init__(self, grader_name, *args, **kwargs):
        super(GraderWorker, self).__init__(*args, **kwargs)

        self.grader_name = grader_name

        # Load the grader into our process space
        Grader = graders.get(self.grader_name)
        self.grader = Grader()

        # Setup RabbitMQ connection for this grader
        credentials = pika.PlainCredentials(settings.RABBITMQ_USER, settings.RABBITMQ_PASSWORD)

        try:
            # Establish a connection
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=settings.RABBITMQ_HOST,
                                          port=settings.RABBITMQ_PORT,
                                          virtual_host=settings.RABBITMQ_VHOST,
                                          credentials=credentials,
                                          heartbeat_interval=5)
                )

        except pika.exceptions.AMQPConnectionError:
            log.exception("Grader could not connect to submission broker: %s", self.grader_name)
            raise

        # Establish a connection channel
        self.channel = self.connection.channel()

        # Create a submission queue for this grader
        self.channel.queue_declare(queue=self.grader_name, durable=True)
        self.channel.basic_qos(prefetch_count=self.PREFETCH_COUNT)

    def run(self):
        """
        Polls queue, evaluating and generating responses.
        """
        self.wait_for_submissions()

    def wait_for_submissions(self):
        """
        Polls submission queue awaiting for submissions

        """
        # Set receive callback for exclusive queue
        self.channel.basic_consume(self.handle_submission,
                                   queue=self.grader_name)

        # Block waiting for submissions
        self.channel.start_consuming()

    def stop(self):
        self.connection.close()

    def handle_submission(self, channel, method, properties, body):
        submission = json.loads(body)
        log.info("Submission received: %s", submission)

        # Evaluate the submission
        response = self.grader.handle_submission(submission)
        log.info("Response received: %s", response)

        # Only acknowledge message handling of response is good
        if response:
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            self.channel.basic_nack(delivery_tag=method.delivery_tag)


def start_grader(grader_name, worker_count=5):
    manager = GraderManager(grader_name, worker_count)
    manager.start()
