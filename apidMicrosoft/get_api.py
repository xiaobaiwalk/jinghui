# -*- coding: utf-8 -*-

from core.config import config
from core.config import initConfig
from core.handler import handle
import logging.handlers
import logging
import datetime
import argparse
import time
import sys
import requests

LOG_FILENAME = config.get("log","LOG_PATH")+'get_api.log'
logger = logging.getLogger('worker')
exec('logger.setLevel(%s)' % config.get('log', 'log_level'))
format = "%(asctime)s %(filename)s %(lineno)d %(levelname)s %(message)s"
handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, "midnight", 1, 30)
handler.setFormatter(logging.Formatter(format))
handler.suffix = "%Y%m%d"
logger.addHandler(handler)

api_list_config = initConfig()
PACKAGE_SIZE = config.getint("package", "size")

now = datetime.datetime.now()

requestt = requests.get("http://alpha.elmeast.com.cn/api/calledForlikun/getDomainByUserName.php?username=Microsoft")
domainArr = requestt.json()
domainArr = ['update1.csgo.wmsj.cn']


def default(ts=None):
    try:
        if ts is None:
            x = datetime.datetime.now() - datetime.timedelta(hours=1)
            xx = x.strftime('%Y-%m-%d %H') + ':00:00'
            timeArray = time.strptime(xx, "%Y-%m-%d %H:%M:%S")
            timestamp = int(time.mktime(timeArray))

            # timestamp = int(time.mktime(now.timetuple()))/300*300
        else:
            timestamp = ts
        domains = domainArr
        if len(domains) > 0:
            for p in xrange(0, len(domains), PACKAGE_SIZE):
                handle('aliyun', timestamp, domains[p:p + PACKAGE_SIZE])
    except:
        logger.error("__main__ ERROR:" + str(sys.exc_info()))
    finally:
        logger.info("Elapsed time:" + str((datetime.datetime.now() - now).total_seconds()))


def main():

    usage = """
        --cdn --tp --domain --starttime
    """
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("--domains", dest="domains",help="Which domains")
    parser.add_argument("--starttime", dest="starttime",help='starttime %%Y-%%m-%%d:%%H:%%M (*)')

    args = parser.parse_args()
    print args

    if args.starttime:
        try:
            now = datetime.datetime.strptime(args.starttime, '%Y-%m-%d:%H:%M')
            timestamp = int(time.mktime(now.timetuple())) / 300 * 300
        except:
            print 'starttime %%Y-%%m-%%d:%%H:%%M'
            sys.exit(-1)

        domains = domainArr

        if len(domains) > 0:
            for p in xrange(0, len(domains), PACKAGE_SIZE):
                handle('aliyun', timestamp, domains[p:p + PACKAGE_SIZE])

    else:
        default()


if __name__ == '__main__':
    main()
