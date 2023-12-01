import yaml
import logging
import logging.handlers
import logging.config


class SizedTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler, logging.handlers.RotatingFileHandler):

    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0, when='midnight', interval=1, utc=False):
        logging.handlers.TimedRotatingFileHandler.__init__(
            self, filename, when, interval, backupCount, encoding, delay, utc)
        logging.handlers.RotatingFileHandler.__init__(
            self, filename, mode, maxBytes, backupCount, encoding, delay)


def getLogger(name) -> logging.Logger:
    try:
        with open('./logging.yml', 'r') as f_yml:
            config = yaml.load(f_yml, Loader=yaml.FullLoader)
            logging.config.dictConfig(config)
    except Exception as ex:
        print(str(ex))

    return logging.getLogger(name)
