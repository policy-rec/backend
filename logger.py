import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

LOGS_PATH = str(os.getenv('LOG_FOLDER'))
LOGS_TEST_FILE = str(os.getenv('LOG_FILE'))

class Logger:
    def __init__(self):
        self.logs_path = LOGS_PATH
        self.__setup_logging__()

    def __setup_logging__(self):
        logging.basicConfig(
            filename=os.path.join(LOGS_PATH, LOGS_TEST_FILE),
            level=logging.INFO,
            format="%(message)s",
        )

        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("openai").setLevel(logging.WARNING)

    # def setupLogging(self):
    #     event_log_file = os.path.join(self.logs_path, LOGS_TEST_FILE)
    #     chat_log_file = os.path.join(self.logs_path, LOGS_CHAT_FILE)
    #     error_log_file = os.path.join(self.logs_path, LOGS_ERROR_FILE)

    #     self.chat_handler = logging.FileHandler(chat_log_file)
    #     self.event_handler = logging.FileHandler(event_log_file)
    #     self.error_handler = logging.FileHandler(error_log_file)

    #     formatter = logging.Formatter("%(message)s")
    #     self.chat_handler.setFormatter(formatter)
    #     self.event_handler.setFormatter(formatter)
    #     self.error_handler.setFormatter(formatter)

    #     self.chat_logger = logging.getLogger("chat_logger")
    #     self.event_logger = logging.getLogger("event_logger")
    #     self.error_logger = logging.getLogger("error_logger")

    #     self.chat_logger.addHandler(self.chat_handler)
    #     self.chat_logger.setLevel(logging.INFO)

    #     self.event_logger.addHandler(self.event_handler)
    #     self.event_logger.setLevel(logging.INFO)

    #     self.error_logger.addHandler(self.error_handler)
    #     self.error_logger.setLevel(logging.ERROR)
    
    def __getGMToffset__(self):
        offset = datetime.now().astimezone().utcoffset()

        if offset is None:
            return "GMT+00:00"
        
        offset_minutes = offset.total_seconds() / 60

        hours = int(offset_minutes // 60)
        minutes = int(offset_minutes % 60)
        return f"GMT{hours:+02}:{minutes:02}"

    def log_event(self, msg_creator, msg, uid = None, cid = None):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + self.__getGMToffset__()

        if msg_creator == "USER":
            log_message = (f"[{timestamp}] | --> [{msg_creator}:{uid}:{cid}]: {msg}")
        elif msg_creator == "RESP":
            log_message = (f"[{timestamp}] | --> [{msg_creator}:{cid}]: {msg}")
        else:
            log_message = (f"[{timestamp}] | [{msg_creator}]: {msg}")
        logging.info(log_message)