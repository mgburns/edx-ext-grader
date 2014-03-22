import logging

from ..xqueue import XQueueClient

log = logging.getLogger(__name__)


class BaseGrader(object):
    def __init__(self):
        self.xqueue = XQueueClient()

    def handle_submission(self, submission):
        log.info("[%s] Handling submission: %s", self.__class__.__name__, submission)
        response = self.grade(submission)
        if response:
            self.post_response(response)
            return True
        return False

    def post_response(self, response):
        result = self.xqueue.put_result(response)
        log.info("[%s] Posted response to XQueue: %s -- Result: %s", self.__class__.__name__, response, result)

    def grade(self, submission):
        """
        Subclasses must define
        """
        pass
