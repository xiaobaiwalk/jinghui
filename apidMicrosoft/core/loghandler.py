# -*- coding: utf-8 -*-

__author__ = 'Lincoln'
from gevent import monkey
monkey.patch_all()

import requests
from rq_scheduler import Scheduler
from core.download import wget
from datetime import timedelta
from core import util
from redis import Redis
from rq import Queue
import logging.handlers
import gevent
from core.util import *

scheduler = Scheduler(connection=Redis(config.get("redis","host")), queue_name=config.get("redis","Log"))
mscheduler = Scheduler(connection=Redis(config.get("redis","host")), queue_name=config.get("redis","Merge"))

r = redis.StrictRedis(config.get("redis","store_host"), db=config.get("redis","store_db"))
rlog = redis.StrictRedis(config.get("redis","host"), db=config.get("redis","log_db"))
rCcvideo = redis.StrictRedis(config.get("redis", "host"), port=config.get("redis", "port"),db=config.get("redis", "fiveminutedomain"))
qMerge = Queue(connection=Redis(config.get("redis", "host")), name="mergelog")

DomainProviderCACHE = redis.StrictRedis(config.get("redis","host"),db=config.get("redis","DomainProvider"))
DomainProviderExpire = 3600*6
AddUpCACHE = redis.StrictRedis(config.get("redis","host"),db=config.get("redis","AddUp"))

GDP_URL = 'http://alpha.elmeast.com.cn/api/calledForlikun/getCdnBydomainV2.php'

LOG_QUEUE = 'logdownload'
LOG_TRANS_QUEUE = 'downloadlogtrans'
LOG_TRANS_CCVIDEO = 'downloadlogccvideo'
CT_KAFAK_QUEUE = 'mq:ct:kafka'
CT_KAFAK_HISTORY = 'history:ct:kafka'


LOG_FILENAME = config.get("log", "LOG_PATH")+'get_log.log'
logger = logging.getLogger('logd')
exec('logger.setLevel(%s)' % config.get('log', 'log_level'))
format = "%(asctime)s %(filename)s %(lineno)d %(levelname)s %(message)s"
handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, "midnight", 1, 30)
handler.setFormatter(logging.Formatter(format))
handler.suffix = "%Y%m%d"
logger.addHandler(handler)

def getData():
    return r.lpop(LOG_QUEUE)


def parse(data):
    return data.split("|")


def downloadLog(cdn, urls, ts, domain, retry_times=0):
    if retry_times < config.getint("retry", "log"):
        try:
            url = urls[0]
        except:
            url = ''
        username = util.getUsernameByDomain(domain)

        result = False

        if rCcvideo.sismember("fiveminutedomain", domain):
            f = getDestGzPathCc(cdn, ts, username, domain)
        elif cdn.find('jingdong') > -1:
            f = getDestPath(cdn, ts, username, domain)
        else:
            f = getDestGzPath(cdn, ts, username, domain)

        # f = 'C:\\data1\\storage\\logd\\aliyun\\2019-01-16\\wmsj\\18\\update1.csgo.wmsj.cn.gz'

        if url:
            logger.info(url)
            try:
                result = wget().download(url, f)
            except Exception as e:
                result = False
                logger.error("wget {} error".format(url), exc_info=True)
        else:
            generateEmptyFile(cdn, ts, domain)
            return

        if not result:
            retry_times += 1
            scheduler.enqueue_in(timedelta(minutes=5), downloadLog, cdn, urls,
                                 ts, domain, retry_times)
        else:
            # TODO
            endtime = ts + 3600
            # get_url = 'openapi.elmeast.com/insertaccesslogtodb?domain=www.baidu.com&accesslogname=update1.csgo.wmsj.cn.2019_01_16_08_00.gz&starttime=1234567890&endtime=1234567890&signature=fasdfaf'

            get_url = 'http://openapi.elmeast.com/insertaccesslogtodb?domain=' + domain + '&accesslogname=' + f + '&starttime=' + str(ts) + '&endtime=' + str(endtime) + '&signature=fasdfaf'

            if get_FileSize(f) != 0:
                requests.get(get_url)

                # mergeLog(domain, ts)
            # if not rlog.sismember("ccdomain", domain) and get_FileSize(f) != 0:
            #     rlog.lpush(LOG_TRANS_QUEUE, "|".join([cdn, f]))
            # if username == "ccvideo":
            #     rlog.lpush(LOG_TRANS_CCVIDEO, f)
            #
            # # china telecom log trans and send to kafka
            # if get_FileSize(f) != 0 and is_chinatelecom_domain(domain):
            #     ct_kakfa_inqueue(cdn, username, ts, domain, f)

def ct_kakfa_inqueue(cdn, username, ts, domain, path):
    # history=offload:doamin:ts task=offload|cp|domain|ts|fpath
    history = "{}|{}|{}".format(cdn, domain, ts)
    task = "{}|{}|{}|{}|{}".format(cdn, username, domain, ts, path)
    if not rlog.sismember(CT_KAFAK_HISTORY, history):
        rlog.zadd(CT_KAFAK_QUEUE, time.time(), task)


def generateEmptyFile(cdn, ts ,domain):
    pass


def toWorker(cdn, ts, domains, jd):
    try:
        jobs = []
        urldic = {}
        data = json.loads(jd).get("data", [])
        if len(data) == 0:
            return
        rdomains = [dm.get("domain") for dm in data]
        [urldic.setdefault(dm.get("domain"), dm.get("urls", [])) for dm in data]

        if len(rdomains) > 0:
            logger.info((rdomains, domains))

        for edomain in list(set(domains).difference(set(rdomains))):
            generateEmptyFile(cdn, ts, edomain)

        # for domain in list(set(domains).intersection(set(rdomains))):
        for domain in rdomains:
            downloadLog(cdn, urldic.get(domain), ts, domain)
            # jobs.append(gevent.spawn(downloadLog, cdn, urldic.get(domain), ts, domain))

        # gevent.joinall(jobs)
    except:
        logger.error(str(sys.exc_info()))


def toWorkerConversantHandler(cdn, ts, jd):
    try:
        for data in jd:
            jobs = []
            try:
                domain = data.get('domain')
                for logs in data.get('logs'):
                    for seq, item in enumerate(logs.get('values', [])):
                        url = item.get('url')
                        jobs.append(gevent.spawn(ConversantHandler,
                                        url, str(ts), domain, cdn, str(seq)))
                        #ConversantHandler(url, str(ts), domain, cdn, str(seq))
            except:
                logger.error(str(sys.exc_info()))
            gevent.joinall(jobs)
    except:
        logger.error(str(sys.exc_info()))


def ConversantHandler(url, ts, domain, cdn, seq):
    """seq: index of log, because same ts has many files"""
    try:
        downloader = wget()
        local_path = getConversantGzPath(cdn, ts, domain, seq)

        if downloader.download(url, local_path):
            dest_path = os.path.join(config.get("Conversant", "flume"),
                                     "tmp", "_".join([domain, ts, seq]))
            logger.info((local_path, dest_path))

            if gunzipFile(local_path, dest_path):
                flume_path = os.path.join(config.get("Conversant", "flume"),
                                          "complete", "_".join([domain, ts, seq]))
                logger.info(moveFile(dest_path, flume_path))
    except:
        logger.error(str(sys.exc_info()))


def tsToDate(ts):
    x = time.localtime(int(ts))
    return time.strftime('%Y-%m-%d', x)


def tsToHour(ts):
    x = time.localtime(int(ts))
    return time.strftime('%H', x)


def tsToMinu(ts):
    x = time.localtime(int(ts))
    return time.strftime("%M", x)

# def tsToDateUDN(ts):
#     return (datetime.datetime.fromtimestamp(ts) - datetime.timedelta(hours=1)).strftime("%Y-%m-%d")
#
# def tsToHourUDN(ts):
#     return (datetime.datetime.fromtimestamp(ts) - datetime.timedelta(hours=1)).strftime("%H")

def getDestPath(cdn,ts,username,domain):
    return os.path.join(config.get("store","dest"),cdn,tsToDate(ts),username,tsToHour(ts),domain)

def getDestGzPath(cdn,ts,username,domain):
    return os.path.join(config.get("store","dest"),cdn,tsToDate(ts),username,tsToHour(ts),domain) + ".gz"

def getDestGzPathCc(cdn,ts,username,domain):
    return os.path.join(config.get("store","dest"),cdn,tsToDate(ts),username,tsToHour(ts),tsToMinu(ts) + "_" + domain) + ".gz"

def getConversantGzPath(cdn, ts, domain, seq):
    return os.path.join(config.get("Conversant", "storage"),tsToDate(ts),tsToHour(ts),"_".join([domain, ts, seq])) + ".gz"

def getMergeLssDestPath(cdn,ts,username,domain):
    return os.path.join(config.get("store","dest"),config.get("kafkatopic",cdn),tsToDate(ts),username,tsToHour(ts),domain + ".gz")

def getMergedianxin(cdn, ts, username, domain):
    return os.path.join(config.get("store", "merge"), "kafkatmp", tsToDate(ts),cdn,tsToHour(ts), domain)

def getMergeCcvideo(cdn,ts,username,domain):
    return os.path.join(config.get("store","dest"),config.get("kafkatopic",cdn),tsToDate(ts),username,tsToHour(ts),tsToMinu(ts)+"_"+domain + ".gz")

def getMergeDestPath(cdn,ts,username,domain):
    try:
        return os.path.join(config.get("store", "merge"), "kafkatmp", tsToDate(ts), config.get("kafkatopic", cdn), tsToHour(ts), domain)
    except:
        logger.error("getMergeDestPath Error: %s" % str(sys.exc_info()))
        return "-"

def getMergePath(ts,username,domain):
    return os.path.join(config.get("store","merge"),tsToDate(ts),username,tsToHour(ts) + "_" + domain) + ".gz"

def getMergeCcvideoPath(ts,username,domain):
    return os.path.join(config.get("store", "merge"), tsToDate(ts), username, tsToHour(ts) + "_" + tsToMinu(ts) + "_" + domain) + ".gz"

def getMergePath2(ts,username,domain):
    return os.path.join(config.get("store","merge"),tsToDate(ts),username,tsToHour(ts) + "_" + domain)

def getMergeTmpPath(ts,username,domain):
    return os.path.join(config.get("store","mergetmp"),tsToDate(ts),username,tsToHour(ts) + "_" + domain) + ".gz"

def getMergeTmpCcvideoPath(ts,username,domain):
    return os.path.join(config.get("store","mergetmp"),tsToDate(ts), username, tsToHour(ts) + "_" + tsToMinu(ts) + "_" + domain) + ".gz"

def getMergeTmpPath2(ts,username,domain):
    return os.path.join(config.get("store","mergetmp"),tsToDate(ts),username,tsToHour(ts) + "_" + domain)

def getDestPathUDN(ts, account):
    return os.path.join(config.get("store", "dest"), "_".join([str(ts),account]))

def getMergePathUDN(ts,username,domain):
    return os.path.join(config.get("store","merge"),tsToDate(ts),username,tsToHour(ts) + "_" + domain) + ".gz"

def addUp(cdn,date,hour,domain):
    key = "_".join([date,hour,domain])
    logger.debug((key,cdn,AddUpCACHE.sadd(key, cdn)))
    if AddUpCACHE.scard(key) == len(getDomainProvider(domain)):
        return True
    else:
        return False

def getDomainProvider(domain):
    try:
        dp = DomainProviderCACHE.get("_".join([domain,"elmeast"]))
        if dp:
            return json.loads(dp)
        else:
            dps = http.getter(GDP_URL, {"domain":domain,"sp":"elmeast"})
            DomainProviderCACHE.set("_".join([domain,"elmeast"]),dps)
            DomainProviderCACHE.expire("_".join([domain,"elmeast"]),DomainProviderExpire)
            return json.loads(dps)
    except:
        logger.debug(sys.exc_info())
        return []

def mergeLogCCvideo(domain,ts):
    merge = False
    zero = False
    try:
        username = util.getUsernameByDomain(domain)
        # f = getMergePath(ts,username,domain)
        if rCcvideo.sismember("fiveminutedomain", domain) == 1:
            f = getMergeTmpCcvideoPath(ts, username, domain)
        else:
            f = getMergeTmpPath(ts, username, domain)
        outnewpath = os.path.dirname(f)
        if not os.path.exists(outnewpath):
            os.makedirs(outnewpath)
        logger.debug("mergeLog Path: %s" % f)
        gz = gzip.GzipFile(filename=f, mode="w")
        # for cdn in getDomainProvider(domain):
        for cdn in ['ccvideo']:
            logger.debug(cdn)
            if rCcvideo.sismember("fiveminutedomain", domain) == 1:
                dpath = getMergeCcvideo(cdn, ts, username, domain)
            else:
                dpath = getMergeLssDestPath(cdn, ts, username, domain)
            logger.debug(dpath)
            if os.path.exists(dpath):
                zero = True
                if dpath.endswith("gz"):
                    with gzip.open(dpath) as g:
                        for line in g:
                            gz.write(line)
                else:
                    with open(dpath) as g:
                        for line in g:
                            gz.write(line)
        gz.close()
        if zero:
            merge = True
        else:
            logger.debug("mergeLog Path: %s don't have logs." % f)
            os.remove(f)
    except:
        logger.debug("mergeLog Error: %s" % str(sys.exc_info()))
        mscheduler.enqueue_in(timedelta(minutes=5), mergeLog, domain, ts)
    finally:
        try:
            if merge:
                username = util.getUsernameByDomain(domain)
                pddomain = rCcvideo.sismember("fiveminutedomain", domain)
                if pddomain:
                    f = getMergeCcvideoPath(ts, username, domain)
                else:
                    f = getMergePath(ts, username, domain)
                outnewpath = os.path.dirname(f)
                if not os.path.exists(outnewpath):
                    os.makedirs(outnewpath)
                # os.rename(getMergeTmpPath(ts,username,domain),f)
                if pddomain:
                    shutil.move(getMergeTmpCcvideoPath(ts, username, domain), f)
                else:
                    shutil.move(getMergeTmpPath(ts, username, domain), f)

                if pddomain:
                    r.sadd("ACCESSFILE_READY_LIST_" + username, "_".join([username, tsToDate(ts), tsToHour(ts),tsToMinu(ts) ,domain]))
                else:
                    r.sadd("ACCESSFILE_READY_LIST_" + username, "_".join([username, tsToDate(ts), tsToHour(ts), domain]))
                # AddUpCACHE.delete("_".join([tsToDate(ts),tsToHour(ts),domain]))
        except:
            logger.debug("mergeLog Error: %s" % str(sys.exc_info()))

def mergeLogBaiduLss(domain,ts):
    merge = False
    zero = False
    try:
        username = util.getUsernameByDomain(domain)
        # f = getMergePath(ts,username,domain)
        f = getMergeTmpPath(ts, username, domain)
        outnewpath = os.path.dirname(f)
        if not os.path.exists(outnewpath):
            os.makedirs(outnewpath)
        logger.debug("mergeLog Path: %s" % f)
        gz = gzip.GzipFile(filename=f, mode="w")
        # for cdn in getDomainProvider(domain):
        for cdn in ['baiduLss','baidulss2nd','Exclouds_baidu']:
            logger.debug(cdn)
            dpath = getMergeLssDestPath(cdn, ts, username, domain)
            logger.debug(dpath)
            if os.path.exists(dpath):
                zero = True
                if dpath.endswith("gz"):
                    with gzip.open(dpath) as g:
                        for line in g:
                            gz.write(line)
                else:
                    with open(dpath) as g:
                        for line in g:
                            gz.write(line)
        gz.close()
        if zero:
            merge = True
        else:
            logger.debug("mergeLog Path: %s don't have logs." % f)
            os.remove(f)
    except:
        logger.debug("mergeLog Error: %s" % str(sys.exc_info()))
        mscheduler.enqueue_in(timedelta(minutes=5), mergeLog, domain, ts)
    finally:
        try:
            if merge:
                username = util.getUsernameByDomain(domain)

                f = getMergePath(ts, username, domain)
                outnewpath = os.path.dirname(f)
                if not os.path.exists(outnewpath):
                    os.makedirs(outnewpath)
                # os.rename(getMergeTmpPath(ts,username,domain),f)
                shutil.move(getMergeTmpPath(ts, username, domain), f)

                r.sadd("ACCESSFILE_READY_LIST_" + username, "_".join([username, tsToDate(ts), tsToHour(ts), domain]))
                # AddUpCACHE.delete("_".join([tsToDate(ts),tsToHour(ts),domain]))
        except:
            logger.debug("mergeLog Error: %s" % str(sys.exc_info()))

def mergeLogmd(domain,ts):
    merge = False
    zero = False
    try:
        username = util.getUsernameByDomain(domain)
        # f = getMergePath(ts,username,domain)
        f = getMergeTmpPath(ts, username, domain)
        outnewpath = os.path.dirname(f)
        if not os.path.exists(outnewpath):
            os.makedirs(outnewpath)
        logger.debug("mergeLog Path: %s" % f)
        gz = gzip.GzipFile(filename=f, mode="w")
        # for cdn in getDomainProvider(domain):
        for cdn in ["dianxin"]:
            logger.debug(cdn)
            dpath = getMergedianxin(cdn, ts, username, domain)
            logger.debug(dpath)
            if os.path.exists(dpath):
                zero = True
                if dpath.endswith("gz"):
                    with gzip.open(dpath) as g:
                        for line in g:
                            gz.write(line)
                else:
                    with open(dpath) as g:
                        for line in g:
                            gz.write(line)
        gz.close()
        if zero:
            merge = True
        else:
            logger.debug("mergeLog Path: %s don't have logs." % f)
            os.remove(f)
    except:
        logger.debug("mergeLog Error: %s" % str(sys.exc_info()))
        mscheduler.enqueue_in(timedelta(minutes=5), mergeLog, domain, ts)
    finally:
        try:
            if merge:
                username = util.getUsernameByDomain(domain)

                f = getMergePath(ts, username, domain)
                outnewpath = os.path.dirname(f)
                if not os.path.exists(outnewpath):
                    os.makedirs(outnewpath)
                # os.rename(getMergeTmpPath(ts,username,domain),f)
                shutil.move(getMergeTmpPath(ts, username, domain), f)

                r.sadd("ACCESSFILE_READY_LIST_" + username, "_".join([username, tsToDate(ts), tsToHour(ts), domain]))
                # AddUpCACHE.delete("_".join([tsToDate(ts),tsToHour(ts),domain]))
        except:
            logger.debug("mergeLog Error: %s" % str(sys.exc_info()))


def mergeLog(domain, ts):
    merge = False
    zero = False
    try:
        username = util.getUsernameByDomain(domain)
        # f = getMergePath(ts,username,domain)
        f = getMergeTmpPath(ts, username, domain)
        outnewpath = os.path.dirname(f)
        if not os.path.exists(outnewpath):
            os.makedirs(outnewpath)
        logger.info("mergeLog Path: %s" % f)
        gz = gzip.GzipFile(filename=f, mode="w")
        for cdn in getDomainProvider(domain):
            dpath = getMergeDestPath(cdn, ts, username, domain)
            logger.info("cdn:{} dpath:{}".format(cdn, dpath))
            if os.path.exists(dpath):
                zero = True
                if dpath.endswith("gz"):
                    with gzip.open(dpath) as g:
                        for line in g:
                            gz.write(line)
                else:
                    with open(dpath) as g:
                        for line in g:
                            gz.write(line)
        gz.close()
        if zero:
            merge = True
        else:
            logger.info("mergeLog Path: %s don't have logs." % f)
            os.remove(f)
    except:
        logger.error("mergeLog Error:", exc_info=True)
        mscheduler.enqueue_in(timedelta(minutes=5), mergeLog, domain,ts)
    finally:
        try:
            if merge:
                username = util.getUsernameByDomain(domain)
                f = getMergePath(ts, username, domain)
                outnewpath = os.path.dirname(f)
                if not os.path.exists(outnewpath):
                    os.makedirs(outnewpath)

                shutil.move(getMergeTmpPath(ts, username, domain), f)
                r.sadd("ACCESSFILE_READY_LIST_" + username, "_".join([username, tsToDate(ts), tsToHour(ts), domain]))
                # AddUpCACHE.delete("_".join([tsToDate(ts),tsToHour(ts),domain]))
        except Exception as e:
            logger.error("mergeLog Error:", exc_info=True)


def mergeLog2(domain, ts ):
    merge = False
    zero = False
    try:
        username = util.getUsernameByDomain(domain)
        # f = getMergePath(ts,username,domain)
        # 新的路径
        f = getMergeTmpPath2(ts,username,domain)
        outnewpath = os.path.dirname(f)
        if not os.path.exists(outnewpath):
            os.makedirs(outnewpath)
        logger.debug("mergeLog Path: %s" % f)
        # gz = gzip.GzipFile( filename=f , mode="w" )
        with open(f, "w") as xie:
            for cdn in getDomainProvider(domain):
                logger.debug(cdn)
                # 旧的路径
                dpath = getMergeDestPath(cdn,ts,username,domain)
                logger.debug(dpath)
                if os.path.exists(dpath):
                    zero = True
                    # if dpath.endswith("gz"):
                    with open(dpath) as g:
                        for line in g:
                            xie.write(line)

                # else:
                #     with open(dpath) as g:
                #         for line in g:
                #             file.write(line)
        # gzip.close()
        if zero:
            merge = True
        else:
            logger.debug("mergeLog Path: %s don't have logs." % f)
            os.remove(f)
    except:
        logger.debug("mergeLog Error: %s" % str(sys.exc_info()))
        mscheduler.enqueue_in(timedelta(minutes=5), mergeLog, domain,ts)
    finally:
        try:
            if merge:
                username = util.getUsernameByDomain(domain)

                f = getMergePath2(ts, username, domain)
                outnewpath = os.path.dirname(f)
                if not os.path.exists(outnewpath):
                    os.makedirs(outnewpath)
                shutil.move(getMergeTmpPath2(ts,username,domain),f)

                rlog.lpush(config.get("redis","ThreeLogKey"),f)
        except:
            logger.debug("mergeLog Error: %s" % str(sys.exc_info()))

def chinanetLogHandler(url,tyh,tm,domain):
    timeArray = time.strptime("_".join([tyh,tm]), "%Y-%m-%d_%H_%M")
    ts = int(time.mktime(timeArray))
    username = util.getUsernameByDomain(domain)
    RESULT = False
    if url:
        RESULT = wget().download(url, getDestGzPath("ChinaNet", ts, username, domain))
    else:
        return
