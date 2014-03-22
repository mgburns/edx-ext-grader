"""
External grader configuration

"""
import os
import importlib

from . import global_settings

ENVIRONMENT_VARIABLE = "GRADER_SETTINGS_MODULE"


class Settings(object):
    def __init__(self):
        try:
            settings_module = os.environ[ENVIRONMENT_VARIABLE]
            if not settings_module:
                raise KeyError
        except KeyError:
            raise Exception("Could not find os.environ[%s]" % ENVIRONMENT_VARIABLE)

        self.SETTINGS_MODULE = settings_module

        # Set attributes from global settings
        for setting in dir(global_settings):
            if setting != setting.upper():
                continue
            setattr(self, setting, getattr(global_settings, setting))

        # Import setting module
        try:
            mod = importlib.import_module(self.SETTINGS_MODULE)
        except ImportError as e:
            raise ImportError("Could not import settings '%s': %s" % (self.SETTINGS_MODULE, e))

        # Update settings using settings module
        for setting in dir(mod):
            if setting != setting.upper():
                continue
            setattr(self, setting, getattr(mod, setting))

settings = Settings()
