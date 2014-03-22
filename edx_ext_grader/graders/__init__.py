import importlib
import logging

from ..conf import settings
from .base import BaseGrader

log = logging.getLogger(__name__)
_registry = {}


def autodiscover():
    """ Autodiscovery of grader modules.

    Looks for settings.INSTALLED_GRADERS on sys.path and attempts to import
    a module called "{{ grader }}.graders".  Loading the module should
    include registration for custom grader classes:

        edx_ext_grader.graders.register("name", GraderClass)

    """
    for grader in settings.INSTALLED_GRADERS:
        try:
            importlib.import_module('%s.graders' % grader)
        except:
            log.exception("Could not import: %s.graders", grader)
            pass


def register(grader_name, grader_class):
    """
    Register grader classes

    """
    global _registry

    if not issubclass(grader_class, BaseGrader):
        log.critical("Graders need to inherit from 'BaseGrader'")
        return False

    if grader_name not in _registry:
        _registry[grader_name] = grader_class


def get(grader_name):
    """
    Fetch grader class by type key

    """
    global _registry

    if grader_name not in _registry:
        return None

    return _registry[grader_name]


__all__ = ['BaseGrader', 'register', 'get']
