import pika
import json
import logging

log = logging.getLogger(__name__)


class SubmissionQueue():
    def __init__(self, queue_url):
        """
        Initialize a grader queue

        Used for both producing and consuming submissions
        """
        self.queue_url = queue_url

        # Establish a connection to RabbitMQ
        try:
            self.connection = pika.BlockingConnection(
                pika.URLParameters(self.queue_url),
                )

            # Establish a connection channel
            self.channel = self.connection.channel()
        except pika.exceptions.AMQPConnectionError:
            log.exception("Grader could not connect to submission queue: %s", queue_url)
            raise

    def submit(self, submission):
        """
        Add submissions to the queue for handling by workers
        """
        try:
            grader = submission['xqueue_body']['grader_payload']['grader']
        except Exception as e:
            log.exception('Could not extract submission destination: %s', grader)

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


class GraderExchange():

    EXCHANGE = "submissions"
    EXCHANGE_TYPE = "direct"
    DELIVERY_MODE = 2

    def __init__(self, grader, host='localhost', port=5672):
        """
        Initialize a grader queue

        Used for both producing and consuming submissions
        """
        self.host = host
        self.port = port
        self.grader = grader

        # Establish a connection to RabbitMQ
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.host, self.port),
            )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE,
                                      type=self.EXCHANGE_TYPE)

    def consume(self, grader_key=''):
        """
        Poll submit queue for submissions
        """
        # Create an exclusive queue for receiving submissions
        result = self.channel.queue_declare(exclusive=True)
        self.queue_name = result.method.queue

        # Bind exchange to our exclusive queue
        self.channel.queue_bind(exchange=self.EXCHANGE,
                                queue=self.queue_name,
                                routing_key=grader_key)

        # Set receive callback for exclusive queue
        self.channel.basic_consume(self.receive_submission,
                                   queue=self.queue_name)

        # Block waiting for submissions
        self.channel.start_consuming()

    def submit(self, submission, grader_key=''):
        """
        Add submissions to the queue for handling by workers
        """
        self.channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=grader_key,
            body=json.dumps(submission),
            properties=pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2)
            )

    def receive_submission(self, channel, method, properties, body):
        grader_key = method.routing_key
        submission = json.loads(body)

        print "Grader will evaluate submission: ", grader_key, submission
        response = self.grader.grade(submission)
        print "Response: ", response

        self.channel.basic_ack(delivery_tag=method.delivery_tag)
