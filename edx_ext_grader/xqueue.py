import time

from .conf import settings


class XQueueClient(object):
    """ XQueue REST interface for external graders """

    def __init__(self):
        self.queue_name = settings.XQUEUE_QUEUE
        self.server_url = settings.XQUEUE_URL
        self.username = settings.XQUEUE_USER
        self.password = settings.XQUEUE_PASSWORD
        self.timeout = settings.XQUEUE_TIMEOUT

    def login(self):
        pass

    def get_submission(self):
        pass

    def get_queuelen(self):
        pass

    def put_result(self, result):
        time.sleep(1)
        return True
