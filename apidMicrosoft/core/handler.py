# -*- coding: utf-8 -*-
from gevent import monkey;
from redis import Redis
from core import loghandler
monkey.patch_all()
import logging.handlers
import simplejson as json
from rq import Queue
from config import initConfig
from rq_scheduler import Scheduler
from config import config
from datetime import timedelta
import http
import util
import gevent
import redis
import aliyun
import sys
reload(sys)
sys.setdefaultencoding('utf8')

api_list_config = initConfig()
r = redis.StrictRedis(config.get("redis","store_host"), db=config.get("redis","store_db"))
scheduler = Scheduler(connection=Redis(config.get("redis","host")),queue_name=config.get("redis","Queue"))

q = Queue(connection=Redis(config.get("redis", "host")),
          name=config.get("redis","Log"))

logger = logging.getLogger('worker')
LOG_FILENAME = config.get("log","LOG_PATH")+'get_api.log'
exec('logger.setLevel(%s)' % config.get('log', 'log_level'))
format = "%(asctime)s %(filename)s %(lineno)d %(levelname)s %(message)s"
handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, "midnight", 1, 30)
handler.setFormatter(logging.Formatter(format))
handler.suffix = "%Y%m%d"
logger.addHandler(handler)


def handle(cdn, ts, domains):

    for l in api_list_config.items(cdn):
        try:
            tp = l[0]
            url = l[1].split("|")[0]
            delay = l[1].split("|")[1]
            period = l[1].split("|")[2]

            if ts % int(period) == 0:
                # 延迟等必须在这做
                for domain in domains:
                    jd = aliyun.get_logs(tp, cdn, url, ts - int(delay) * 300, domain)
                    loghandler.toWorker(cdn, ts, domains, jd)

        except Exception as e:
            logger.error(sys.exc_info())
            logger.error("spawn Error:cdn %s,ts %s,domains:%s" % (cdn, str(ts), ",".join(domains)),
                         exc_info=True)

def route_aliyun(cdn, tp, url, ts, domains):
    dispatch = {
        "flow": aliyun.get_flow,
        "bandwith": aliyun.get_bandwith,
        "originflow": aliyun.get_origin_flow,
        "httpcode": aliyun.get_httpcode,
        "pv": aliyun.get_pv,
        "flowfenbu": aliyun.get_flowfenbu,
        "logdownload": aliyun.get_logs
    }

    with gevent.Timeout(config.getint("gevent", "timeout"), False):
        func = dispatch.get(tp)
        if func is not None:
            for domain in domains:
                data = func(tp, cdn, url, ts, domain)
                if tp != "logdownload":
                    enQueue(cdn + tp, cdn, ts, [domain], data)
                else:
                    enQueue(tp, cdn, ts, ["null"], data)


def route(cdn, tp, url, ts, domains):
    dispatch = {
        'bandwidth': lambda: _get(tp, cdn, url, ts, domains),
        'requestandhitpv': lambda: _get(tp, cdn, url, ts, domains),
        'hitflow': lambda: _get(tp, cdn, url, ts, domains),
        'httpcode': lambda: _get(tp, cdn, url, ts, domains),
        'back': lambda: _process(tp, cdn, url, ts, domains),
        'regionisp': lambda: _process(tp, cdn, url, ts, domains),
        'logdownload': lambda: _get(tp, cdn, url, ts, domains),
        'nodeip': lambda: _post(tp, cdn, url, ts, domains),
        'areaispip': lambda: _post(tp, cdn, url, ts, domains)
    }
    data = None
    with gevent.Timeout(config.getint("gevent","timeout"), False):
        data = dispatch.get(tp, _default)()
    #logger.debug(data)
    if tp in ['back','regionisp']:
        for d in data:
            if d[1] is None:
                logger.debug("%s Get %s tp:%s None,enqueue Scheduler" % (str(ts),cdn,tp))
                scheduler.enqueue_in(timedelta(minutes=5), retryRoute,tp, cdn, url ,ts , [d[0]])
            else:
                enQueue(tp, cdn, ts, [d[0]], d[1])
    else:
        if data is None:
            logger.debug("%s Get %s tp:%s None,enqueue Scheduler" % (str(ts),cdn,tp))
            scheduler.enqueue_in(timedelta(minutes=5), retryRoute,tp, cdn, url ,ts , domains)
        else:
            enQueue(tp, cdn, ts, domains,data)




def retryRoute(tp, cdn, url ,ts , domains,retry_times=0):
    dispatch = {
        'bandwidth': lambda: _get(tp, cdn, url, ts, domains),
        'requestandhitpv': lambda: _get(tp, cdn, url, ts, domains),
        'hitflow': lambda: _get(tp, cdn, url, ts, domains),
        'httpcode': lambda: _get(tp, cdn, url, ts, domains),
        'back': lambda: _process(tp, cdn, url, ts, domains),
        'regionisp': lambda: _process(tp, cdn, url, ts, domains),
        'logdownload': lambda: _get(tp, cdn, url, ts, domains),
        'nodeip': lambda: _post(tp, cdn, url, ts, domains),
        'areaispip': lambda: _post(tp, cdn, url, ts, domains)
    }
    if retry_times < config.getint("retry","times"):
        retry_times += 1
        data = dispatch.get(tp, _default)()
        if tp in ['back','regionisp']:
            for d in data:
                if d[1] is None:
                    logger.debug("%s Get %s tp:%s None,enqueue Scheduler" % (str(ts),cdn,tp))
                    scheduler.enqueue_in(timedelta(minutes=5), retryRoute,tp, cdn, url ,ts , [d[0]],retry_times)
                else:
                    enQueue(tp, cdn, ts, [d[0]], d[1])
        else:
            if data is None:
                scheduler.enqueue_in(timedelta(minutes=5), retryRoute,tp, cdn, url ,ts , domains,retry_times)
            else:
                enQueue(tp, cdn, ts, domains,data)
    else:
        scheduler.enqueue_in(timedelta(hours=1), retryRouteBy1hour,tp, cdn, url ,ts , domains)

def retryRouteBy1hour(tp, cdn, url ,ts , domains,retry_times=0):
    dispatch = {
        'bandwidth': lambda: _get(tp, cdn, url, ts, domains),
        'requestandhitpv': lambda: _get(tp, cdn, url, ts, domains),
        'hitflow': lambda: _get(tp, cdn, url, ts, domains),
        'httpcode': lambda: _get(tp, cdn, url, ts, domains),
        'back': lambda: _process(tp, cdn, url, ts, domains),
        'regionisp': lambda: _process(tp, cdn, url, ts, domains),
        'logdownload': lambda: _get(tp, cdn, url, ts, domains),
        'nodeip': lambda: _post(tp, cdn, url, ts, domains),
        'areaispip': lambda: _post(tp, cdn, url, ts, domains)
    }
    if retry_times < config.getint("retry","times"):
        retry_times += 1
        data = dispatch.get(tp, _default)()
        if tp in ['back','regionisp']:
            for d in data:
                if d[1] is None:
                    logger.debug("%s Get %s tp:%s None,enqueue Scheduler" % (str(ts),cdn,tp))
                    scheduler.enqueue_in(timedelta(hours=1), retryRoute,tp, cdn, url ,ts , [d[0]],retry_times)
                else:
                    enQueue(tp, cdn, ts, [d[0]], d[1])
        else:
            if data is None:
                scheduler.enqueue_in(timedelta(hours=1), retryRouteBy1hour,tp, cdn, url ,ts , domains,retry_times)
            else:
                enQueue(tp, cdn, ts, domains,data)


def enQueue(tp, cdn, ts, domains,data):
    value = "|".join([cdn,str(ts),",".join(domains),data])
    toRedis(tp,value)
    # toMongo(tp,cdn,ts,domains,data)


def toRedis(tp,value,retry_times=0):
    try:
        if retry_times < config.getint("retry","redis"):
             r.lpush(tp,value)
        else:
            logger.debug(("toRedis Error:", tp, value))
    except:
        import sys
        logger.debug(sys.exc_info())
        retry_times += 1
        scheduler.enqueue_in(timedelta(minutes=5), toRedis,tp,value,retry_times)


def _get(tp, cdn, url ,ts , domains):
    text = http.getter(url, {"username":config.get(cdn, "username") , "signature": util.getSignature(cdn, ts), "domain": ",".join(domains), "start_time": util.getStartTime(ts), "end_time": util.getEndTime(ts)})
    if text is not None:
        try:
            t = json.loads(text)
        except json.JSONDecodeError:
            t = {}
        if not t.get("status",1):
            return text
        else:
            logger.debug("%s Get %s tp:%s Status 1 Text %s" % (str(ts),cdn,url,text))
            return None
    else:
        return None


def _post(tp, cdn, url ,ts , domains):
    data = {"username":config.get(cdn,"username") ,"signature": util.getSignature(cdn, ts), "domain": ",".join(domains), "start_time": util.getStartTime(ts), "end_time": util.getEndTime(ts)} if tp not in ['nodeip', 'areaispip'] else {"username":config.get(cdn, "username") , "signature": util.getSignature(cdn, ts), "domain": ",".join(domains)}
    text =  http.poster(url, data)
    if text is not None:
        try:
            t = json.loads(text)
        except json.JSONDecodeError:
            t = {}
        if not t.get("status",1):
            return text
        else:
            logger.debug("%s Get %s tp:%s Status 1 Text %s" % (str(ts),cdn,url,text))
            return None
    else:
        return None


def _process(tp, cdn, url ,ts , domains):
    ret = []
    for d in domains:
        data = _post(tp, cdn, url ,ts , [d])
        ret.append((d, data))
    return ret


def _default():
    return None
