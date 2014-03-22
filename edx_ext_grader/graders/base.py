import json
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
            self.post_response(submission, response)
            return True
        return False

    def post_response(self, submission, response):
        xresponse = {
            "xqueue_header": json.dumps(submission['xqueue_header']),
            "xqueue_body": json.dumps(response)
        }
        result = self.xqueue.put_result(xresponse)
        log.info("[%s] Posted response to XQueue: %s -- Result: %s", self.__class__.__name__, xresponse, result)

    def grade(self, submission):
        """
        Subclasses must define
        """
        pass
