# Editor: LiuQiang

import logging
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from logging import Formatter
import os
import datetime


class ToolLogging(object):

    def __init__(self, file="log", level="info", classify="date", date_dir=False):
        self._logger = logging.getLogger(file)
        # 日志切割
        if date_dir:
            file = file.rstrip('/')
            if '/' in file:
                index = file.rfind('/')
                dir_name = file[:index]
                file_name = file[index+1:]
            else:
                file_name = file
                dir_name = ''
            today = datetime.date.today().isoformat()
            cwd = dir_name + '/' + today
            if os.path.isdir(cwd):
                pass
            else:
                os.makedirs(cwd)
            file = cwd + '/' + file_name
        if classify == "date":
            my_handler = TimedRotatingFileHandler(filename=file, when='midnight', interval=1, backupCount=30)
        elif classify == "size":
            my_handler = RotatingFileHandler(filename=file, maxBytes=1024*1024*30, backupCount=100)

        logger_formtter = Formatter('%(asctime)s-*-[line:%(lineno)d]-*-%(levelname)s-*-%(message)s')
        my_handler.setFormatter(logger_formtter)
        self._logger.addHandler(my_handler)
        # 屏幕输出
        console_hdlr = StreamHandler()
        console_formatter = Formatter('%(asctime)s-*-%(name)-12s: %(levelname)-8s %(message)s')
        console_hdlr.setFormatter(console_formatter)
        level_transfer = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "fatal": logging.FATAL,
        }
        log_level = level_transfer.get(level.lower())
        if not log_level:
            log_level = logging.INFO
        self._logger.setLevel(log_level)
        self._logger.addHandler(console_hdlr)

    def get_logger(self):
        return self._logger
