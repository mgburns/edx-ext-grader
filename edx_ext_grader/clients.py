from .xqueue import XQueueClient


class XQueuePullClient(XQueueClient):
    """ External grader interface for polling XQueue for submissions """

    def __init__(self, *args, **kwargs):
        super(XQueuePullClient, args, kwargs)

    def start(self):
        """ Begin polling XQueue for submissions """
        pass

    def stop(self):
        """ Stop polling XQueue for submissions """
        pass


def pull_client(queue_name, server_url, username, password, timeout):
    """ Returns an XQueuePullClient instance """
    return XQueuePullClient(queue_name, server_url, username, password, timeout)
