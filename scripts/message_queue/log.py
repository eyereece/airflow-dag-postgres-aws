import logging

class Logging:
    def __init__(self):
        self.logger = logging.getLogger('mq_pubsub')
        self.logger.setLevel(logging.INFO)
        self.handler = logging.FileHandler('mq.log')
        self.handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)

    def log_msg(self, message, level='info'):
        if level == 'warning':
            self.logger.warning(message)
            return
        self.logger.info(message)

log_msg = Logging().log_msg