# -*- coding: utf-8 -*-

__author__ = 'Lincoln'
from core.config import config
from core import http
import simplejson as json
import subprocess
import datetime
import time
import hashlib
import logging
import logging.handlers
import redis
import shutil
import gzip
import sys
import os
import re
import urllib2

LOG_QUEUE = 'logdownload'
logger = logging.getLogger('logd')

r_cache = redis.StrictRedis(config.get("redis", "host"), db=config.get("redis", "DomainProvider"))


def getSignature(cdn, ts):
    timeStr = datetime.datetime.now().strftime('%Y%m%d')
    return hashlib.md5(timeStr + config.get(cdn, "username") + config.get(cdn, "password")).hexdigest()


def getDilianSignature():
    timeStr = datetime.datetime.now().strftime('%Y%m%d')
    return hashlib.md5(config.get("dnion", "username") + config.get("dnion", "key") + timeStr).hexdigest()


def getDilianLogSignature():
    timeStr = datetime.datetime.now().strftime('%Y%m%d')
    return hashlib.md5(config.get("dnion", "username") + config.get("dnion", "logkey") + timeStr).hexdigest()


def getCCSign(data, key):
    dataList = data.keys()
    dataList.sort()
    sList = []
    for d in dataList:
        sList.append(d + '=' + str(data[d]))
    s = '&'.join(sList)
    s1 = s + key
    s2 = s1.encode('utf-8')
    m = hashlib.md5(s2)
    m.digest()
    sign = m.hexdigest()
    return sign


def getStartTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%d %H:%M", ltime)
    return timeStr


def get_FileSize(filePath):
    # filePath = unicode(filePath, 'utf8')
    fsize = os.path.getsize(filePath)
    # fsize = fsize / float(1024 * 1024)
    return fsize


def getDilianTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y%m%d", ltime)
    return timeStr


# 百度日志开始时间
def getBaidustartTime(ts):
    tss = ts / 3600 * 3600
    ltime = time.localtime(tss)
    timeStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", ltime)
    timeDate = datetime.datetime.strptime(timeStr, "%Y-%m-%dT%H:%M:%SZ")
    temp = datetime.timedelta(hours=8)
    resultStr = timeDate - temp
    resultTime = resultStr.strftime("%Y-%m-%dT%H:%M:%SZ")
    return resultTime


# 百度api开始时间
def getBaiduapistartTime(ts):
    tss = ts / 300 * 300
    ltime = time.localtime(tss)
    timeStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", ltime)
    timeDate = datetime.datetime.strptime(timeStr, "%Y-%m-%dT%H:%M:%SZ")
    temp = datetime.timedelta(hours=8)
    resultStr = timeDate - temp
    resultTime = resultStr.strftime("%Y-%m-%dT%H:%M:%SZ")
    return resultTime


# 百度api结束时间
def getBaiduEndTime(ts):
    startTime = getBaiduapistartTime(ts)
    endTime = time.mktime(time.strptime(startTime, "%Y-%m-%dT%H:%M:%SZ"))
    return (datetime.datetime.fromtimestamp(endTime) + datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")


# 百度直播日志api结束时间
def getBaidu2ndLssLogTime(ts):
    startTime = getBaiduapistartTime(ts)
    endTime = time.mktime(time.strptime(startTime, "%Y-%m-%dT%H:%M:%SZ"))
    return (datetime.datetime.fromtimestamp(endTime) + datetime.timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%SZ")


# 百度流媒体的时间
def getBaiduLssStarttime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", ltime)
    timeDate = datetime.datetime.strptime(timeStr, "%Y-%m-%dT%H:%M:%SZ")
    temp = datetime.timedelta(hours=8)
    resultStr = timeDate - temp
    resultTime = resultStr.strftime("%Y-%m-%dT%H:%M:%SZ")
    return resultTime


def getBaiduLssEndTime(ts):
    startTime = getBaiduLssStarttime(ts)
    endTime = time.mktime(time.strptime(startTime, "%Y-%m-%dT%H:%M:%SZ"))
    return (datetime.datetime.fromtimestamp(endTime) + datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")


def getConverstantstarttime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", ltime)
    timeDate = datetime.datetime.strptime(timeStr, "%Y-%m-%dT%H:%M:%SZ")
    temp = datetime.timedelta(hours=8)
    resultStr = timeDate - temp
    resultTime = resultStr.strftime("%Y-%m-%dT%H:%M:%SZ")
    return resultTime


def getConverstantendtime(ts):
    startTime = getConverstantstarttime(ts)
    endTime = time.mktime(time.strptime(startTime, "%Y-%m-%dT%H:%M:%SZ"))
    return (datetime.datetime.fromtimestamp(endTime) + datetime.timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%SZ")


# MDN接口开始时间
def getMdnPortStartTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", ltime)
    timeDate = datetime.datetime.strptime(timeStr, "%Y-%m-%dT%H:%M:%SZ")
    # temp = datetime.timedelta(hours=8)
    # resultStr= timeDate-temp
    resultTime = timeDate.strftime("%m/%d/%Y:%H:%M:%S")
    return resultTime


# MDN接口结束时间
def getMdnPortEndTime(ts):
    startTime = getMdnPortStartTime(ts)
    endTime = time.mktime(time.strptime(startTime, "%m/%d/%Y:%H:%M:%S"))
    return (datetime.datetime.fromtimestamp(endTime) + datetime.timedelta(minutes=5)).strftime("%m/%d/%Y:%H:%M:%S")


# MDN日志开始时间
def getMdnLogstartTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", ltime)
    timeDate = datetime.datetime.strptime(timeStr, "%Y-%m-%dT%H:%M:%SZ")
    temp = datetime.timedelta(hours=8)
    resultStr = timeDate - temp
    resultTime = resultStr.strftime("%m/%d/%Y:%H:%M:%S")
    startTime = time.mktime(time.strptime(resultTime, "%m/%d/%Y:%H:%M:%S"))
    a = (datetime.datetime.fromtimestamp(startTime) - datetime.timedelta(minutes=10)).strftime("%Y%m%d-%H%M")
    return a


# 获取Mdn日志的结束时间
def getMdnLogEdnTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y%m%d-%H%M", ltime)
    timeDate = datetime.datetime.strptime(timeStr, "%Y%m%d-%H%M")
    temp = datetime.timedelta(hours=8)
    resultStr = timeDate - temp
    resultTime = resultStr.strftime("%Y%m%d-%H%M")
    end = time.mktime(time.strptime(resultTime, "%Y%m%d-%H%M"))
    endtime = datetime.datetime.fromtimestamp(end).strftime("%Y%m%d-%H%M")
    return endtime


def getBaiduLssLogTime(ts):
    return datetime.datetime.fromtimestamp(long(ts) / 3600 * 3600).strftime("%Y-%m-%dT%H:%M:%SZ")


# get time from config period
def get_baidu_start_5mins(ts):
    return datetime.datetime.fromtimestamp(long(ts)).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_baidu_endtime(start_ts, interval=300 - 1):
    return datetime.datetime.fromtimestamp((long(start_ts) + interval)
                                           ).strftime("%Y-%m-%dT%H:%M:%SZ")


def getBaiduDownloadEndTime(ts):
    startTime = getBaidustartTime(ts)
    endTime = time.mktime(time.strptime(startTime, "%Y-%m-%dT%H:%M:%SZ"))
    return (datetime.datetime.fromtimestamp(endTime) + datetime.timedelta(minutes=59)).strftime("%Y-%m-%dT%H:%M:%SZ")


def getEndTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%d %H:%M", ltime)

    return timeStr


def getQQStartTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%d %H:%M:%S", ltime)
    return timeStr


def getQQENdTime(ts):
    return (datetime.datetime.fromtimestamp(ts) + datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")


def getKingStartTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%dT%H:%M+0800", ltime)
    return timeStr


def getKingLogStartTime(ts):
    ts = long(ts) / 3600 * 3600
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%dT%H:%M:%S+0800", ltime)
    return timeStr


def getKingEndTime(ts):
    return (datetime.datetime.fromtimestamp(ts) + datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M+0800")


def getKingLogTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%d", ltime)
    return timeStr


def getCCStartTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y%m%d%H%M", ltime)
    return timeStr


def getCCEndTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y%m%d%H%M", ltime)
    return timeStr


def getCCLogTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y%m%d%H", ltime)
    return timeStr


def getUdnStartTime(ts):
    return datetime.datetime.utcfromtimestamp(ts).strftime("%Y.%m.%d %H:%M")


def getUdnEndTime(ts):
    return (datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(minutes=5)).strftime("%Y.%m.%d %H:%M")


def getUdnTime(ts):
    return datetime.datetime.utcfromtimestamp(ts).strftime("%Y.%m.%d")


def tsToDate(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def tsToHour(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%H")


def getConversantTime(ts):
    # to GMT +0000
    ltime = datetime.datetime.utcfromtimestamp(ts)
    # ltime=time.localtime(ts-96*300)
    timeStr = ltime.strftime("%Y%m%d_%H%M")
    return timeStr


def getUdnLogTime(ts):
    ltime = time.localtime(long(ts) / 1000)
    timeStr = time.strftime("[%d/%b/%Y:%H:%M:%S %Z]", ltime)
    return timeStr


def getNGAAStartTime(ts):
    ltime = time.localtime(ts)
    timeStr = time.strftime("%Y-%m-%d %H:%M:%S", ltime)
    return timeStr


def getNGAAEndTime(ts):
    return (datetime.datetime.fromtimestamp(ts) + datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")


def getNGAAEndLogTime(ts):
    return (datetime.datetime.fromtimestamp(ts) + datetime.timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")


def getAzapiStartTime(ts):
    tss = ts / 300 * 300
    ltime = time.localtime(tss)
    timeStr = time.strftime("%Y-%m-%dT%H:%MZ", ltime)
    timeDate = datetime.datetime.strptime(timeStr, "%Y-%m-%dT%H:%MZ")
    temp = datetime.timedelta(hours=8)
    resultStr = timeDate - temp
    resultTime = resultStr.strftime("%Y-%m-%dT%H:%MZ")
    return resultTime


def getAzapiEndTime(ts):
    startTime = getAzapiStartTime(ts)
    endTime = time.mktime(time.strptime(startTime, "%Y-%m-%dT%H:%MZ"))
    return (datetime.datetime.fromtimestamp(endTime) + datetime.timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%MZ")


def getccvideostarttime(ts):
    return datetime.datetime.fromtimestamp(ts / 300 * 300).strftime("%Y-%m-%d %H:%M")


def getccvideoendtime(ts):
    return datetime.datetime.fromtimestamp(ts / 300 * 300).strftime("%Y-%m-%d %H:%M")


# ccvideo 日志开始时间
def getccvideoLogstarttime(ts):
    timeArray = time.localtime(ts)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M", timeArray)
    starttime = time.mktime(time.strptime(otherStyleTime, "%Y-%m-%d %H:%M"))
    return (datetime.datetime.fromtimestamp(starttime)).strftime("%Y-%m-%d %H:%M")


# ccvideo  日志结束时间
def getccvideoLogendtime(ts):
    timeArray = time.localtime(ts)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M", timeArray)
    starttime = time.mktime(time.strptime(otherStyleTime, "%Y-%m-%d %H:%M"))
    return (datetime.datetime.fromtimestamp(starttime) + datetime.timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M")


def zipFile(root_file, zip_file):
    try:
        logger.debug(("zipFile Begin", root_file, zip_file))
        g = gzip.GzipFile(filename="access.log", mode='wb', compresslevel=9, fileobj=open(zip_file, 'wb'))
        f = file(root_file)
        while True:
            line = f.read(1024)
            if line:
                g.write(line)
            else:
                break
        g.close()
        f.close()
        logger.debug(("zipFile End", root_file, zip_file))
        return True
    except:
        logger.debug("zipFile ERROR:" + str(sys.exc_info()))
        return False


def gunzipFile(zip_file, dest_file):
    try:
        logger.debug(("gunzipFile Begin", zip_file, dest_file))
        with open(dest_file, 'wb') as f_out, gzip.open(zip_file, 'rb') as f_in:
            shutil.copyfileobj(f_in, f_out)
        logger.debug(("gunzipFile End", zip_file, dest_file))
        return True
    except:
        logger.debug("gunzipFile ERROR:" + str(sys.exc_info()))
        return False


def moveFile(src_file, dest_file):
    logger.info("Begin moveFile %s to %s" % (src_file, dest_file))
    d_path = "/".join(dest_file.split("/")[:-1])
    if not os.path.isdir(d_path):
        os.makedirs(d_path)
    if os.path.exists(dest_file):
        os.remove(dest_file)
    shutil.move(src_file, dest_file)
    logger.info("moveFile %s to %s" % (src_file, dest_file))


def getUdnAccountID():
    try:
        AccountIDs = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getUdnAcountID.php", {})
        r_cache.set("UDN", AccountIDs)
        return json.loads(AccountIDs)
    except:
        logger.error("getUdnAccountID ERROR:" + str(sys.exc_info()))
        return json.loads(r_cache.get("UDN"))


def getDomain(cdn):
    try:
        domains = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getDomainBycdnV2.php",
                              {"cdn": cdn, "sp": "elmeast"})
        ds = json.loads(domains)
        for i in ds:
            if i is None:
                ds.remove(i)
        r_cache.set("_".join([cdn, "elmeast"]), json.dumps(ds))
        return ds
    except:
        logger.error("getDomain ERROR:" + str(sys.exc_info()))
        return json.loads(r_cache.get("_".join([cdn, "elmeast"])))


def getMdnAccountId():
    try:
        return json.loads(http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getAccountID_mdn.php", p={}))
    except:
        logger.error("getMdnAccountId ERROR:" + str(sys.exc_info()))


def getDownDomain(cdn, tp):
    try:
        domains = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getplayDomain.php", "")
        ds = json.loads(domains)
        for i in ds:
            if i is None:
                ds.remove(i)
        r_cache.set('baiduLssDownDomain', json.dumps(ds))
        print(ds)
        return ds
    except:
        logger.error("getBaiduLssDownDomain ERROR:" + str(sys.exc_info()))
        return json.loads(r_cache.get('baiduLssDownDomain'))


def getUpDomain(cdn, tp):
    try:
        domains = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getpushDomain.php", "")
        ds = json.loads(domains)
        for i in ds:
            if i is None:
                ds.remove(i)
        r_cache.set('baiduLssUpDomain', json.dumps(ds))
        return ds
    except:
        logger.error("getBaiduLssUpDomain ERROR:" + str(sys.exc_info()))
        return json.loads(r_cache.get('baiduLssUpDomain'))


def getDomainBaiduLss():
    try:
        domainsDown = json.loads(http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getplayDomain.php", ""))
        domainsUp = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getpushDomain.php", "")
        for d in json.loads(domainsUp):
            domainsDown.append(d)
        r_cache.set("baiduLssAllDomain", domainsDown)
        return domainsDown
    except:
        logger.error("getBaiduLssAllDomain ERROR:" + str(sys.exc_info()))
        return json.loads(r_cache.get("baiduLssAllDomain"))


def getKingDomain(domainid):
    try:
        domain = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getDomainByKingId.php",
                             {"domainid": domainid})
        r_cache.set(domainid, domain)
        return domain
    except:
        logger.error("getKingDomain ERROR:" + str(sys.exc_info()))
        return r_cache.get(domainid)


def getUsernameByDomain(domain):
    try:
        username = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getUsernameByDomainV2.php",
                               {"domain": domain, "sp": "elmeast"})
        r_cache.set(domain + "username", username)
        return username
    except:
        logger.error("getUsernameByDomain ERROR:" + str(sys.exc_info()))
        return r_cache.get(domain + "username")


def getDomainsBySP(sp="elmeast"):
    try:
        domains = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getDomains.php",
                              {"sp": sp})
        r_cache.set(sp + "domains", domains)
        return domains
    except:
        logger.error("getDomainsBySP ERROR:" + str(sys.exc_info()))
        return r_cache.get(sp + "domains")


def is_chinatelecom_domain(domain):
    cache_key = "isctdoamin:{}".format(domain)
    url = "http://alpha.elmeast.com.cn/api/calledForlikun/IsTeleChannel.php"
    res = http.getter(url, {"domain": domain})
    if res is not None:
        r_cache.setex(cache_key, 3600, res)
    else:
        res = r_cache.get(cache_key)

    if res is None:
        return False
    return json.loads(res)["isTele"]


def getDomainsThree():
    try:
        username = config.get("ThreeUser", "name")
        domains = []
        for user in username.split(","):
            d = http.getter("http://alpha.elmeast.com.cn/api/calledForlikun/getDomainByAccount.php", {"account": user})
            domains += json.loads(d)
        r_cache.set("domains", json.dumps(domains))
        return json.dumps(domains)
    except:
        logger.error("getDomainsBySP ERROR:" + str(sys.exc_info()))
        return r_cache.get("domains")


def checkip(ip):
    p = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
    if p.match(ip):
        return True
    else:
        return False


def commandRun(command, timeout=10):
    stdout = ""
    stderr = ""
    proc = subprocess.Popen(command, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    poll_seconds = .250
    deadline = time.time() + timeout
    while time.time() < deadline and proc.poll() == None:
        time.sleep(poll_seconds)
    if proc.poll() == None:
        if float(sys.version[:3]) >= 2.6:
            proc.terminate()
    else:
        stdout, stderr = proc.communicate()
    return stdout, stderr, proc.returncode


from random import randint
import hmac


def hex_hmac_sha256(sign_key, signing_string):
    signed_bytes = hmac.new(sign_key, signing_string, digestmod=hashlib.sha256).digest()
    return signed_bytes.encode('hex')


def ConverstantEncrypt(uri, startTime, endTime, domains):
    access_key_id = config.get("ConverstantApi", "id")
    access_key_secret = config.get("ConverstantApi", "secret")

    request_method = "POST"
    request_timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.localtime(time.time() + time.timezone))
    nonce = randint(10000, 99999)
    request_body = '{"domains":' + str(
        json.dumps(domains)) + ',"startTime":"' + startTime + '", "endTime":"' + endTime + '"}'
    print request_body
    signing_string = "%s\n%s\n%s\n%d\n%s\n%s" % (
    request_method.upper(), uri, request_timestamp, nonce, access_key_id, request_body)
    signature = hex_hmac_sha256(access_key_secret, signing_string)
    authorization_val = "HMAC-SHA256 " + access_key_id + ":" + signature

    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)

    url = "%s%s" % ("https://cdn-api.swiftfederation.com", uri)
    request = urllib2.Request(url, data=request_body)

    request.add_header("Authorization", authorization_val)
    request.add_header("Content-Type", "application/json; charset=utf-8")
    request.add_header("x-sfd-date", request_timestamp)
    request.add_header("x-sfd-nonce", nonce)

    request.get_method = lambda: request_method
    return opener.open(request).read()

