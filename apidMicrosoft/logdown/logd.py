# -*- coding: utf-8 -*-

__author__ = 'Lincoln'
from core.config import config
from core import loghandler
from redis import Redis
from rq import Queue
import logging.handlers
import datetime
import logging
import sys

LOG_FILENAME = config.get("log","LOG_PATH")+'get_log.log'
logger = logging.getLogger('logd')
exec('logger.setLevel(%s)' % config.get('log', 'log_level'))
format = "%(asctime)s %(filename)s %(lineno)d %(levelname)s %(message)s"
handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, "midnight", 1, 30)
handler.setFormatter(logging.Formatter(format))
handler.suffix = "%Y%m%d"
logger.addHandler(handler)

q = Queue(connection=Redis(config.get("redis", "host")),
          name=config.get("redis","Log"))
# q = Queue(connection=Redis(config.get("redis", "host")),
#           name=config.get("redis","MicrosoftLog"))

now = datetime.datetime.now()
try:
    if len(sys.argv) > 2:
        now = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d:%H:%M')
except:
    print '%Y-%m-%d:%H:%M'


def main():
    try:
        print "*" * 30
        data = loghandler.getData()
        while data:
            print data
            if data:
                args = loghandler.parse(data)
                if len(args) == 4:
                    cdn = args[0]
                    ts = args[1]
                    domains = args[2].split(",")
                    jd = args[3]
                    q.enqueue(loghandler.toWorker, cdn, ts, domains, jd, timeout=10000)
                else:
                    logger.debug("Data format Error: %s" % data)
            data = loghandler.getData()
    except:
        logger.error("__main__ ERROR:" + str(sys.exc_info()))
    finally:
        logger.info("Elapsed time:" + str((datetime.datetime.now() - now).total_seconds()))


if __name__ == '__main__':
    main()

