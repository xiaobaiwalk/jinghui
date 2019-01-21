
#coding=utf8
import datetime
import requests
import time
import urllib
from core.config import config
from core.aliyun import init_client

if __name__ == '__main__':

    x = datetime.datetime.now() - datetime.timedelta(hours=1)
    xx = x.strftime('%Y-%m-%d %H') + ':00:00'

    timeArray = time.strptime(xx, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    print x
    print xx

    print timeStamp


    print ("============================")

    requestt = requests.get('http://alpha.elmeast.com.cn/api/calledForlikun/getDomainByUserName.php?username=Microsoft')
    print requestt.json()

    # domainArr = []
    # for hh in xrange(0, len(requestt.json())):
    #     domainArr.append(requestt.json()[hh])
    # print domainArr
    # print type(domainArr)

    req = requests.get('http://alpha.elmeast.com.cn/api/calledForlikun/getDomainByUserName.php?username=Microsoft')

    print req.json()

    # f = '/data1/storage/logd\microsoft\2019-01-16\wmsj\18\update1.csgo.wmsj.cn.gz'
    # ff = f.replace("\\", '/')

    f = '/data1/storage/logd/aliyun/2019-01-16/wmsj/18/update1.csgo.wmsj.cn.gz'
    # print ff
    ts = 1547632800

    endtime = ts + 3600

    # get_url = 'http://openapi.elmeast.com/insertaccesslogtodb?domain=www.baidu.com&accesslogname=' + f + '&starttime=' + str(ts) + '&endtime=' + str(endtime) + '&signature=fasdfaf'

    get_url = 'http://openapi.elmeast.com/insertaccesslogtodb?domain=www.baidu.com&accesslogname=' + f + '&starttime=' + str(
        ts) + '&endtime=' + str(endtime) + '&signature=fasdfaf'

    print get_url

    requesttt = requests.get(get_url)

    print requesttt

    aa = config.getint("retry", "log")
    print aa

    cc = init_client('aliyun')
    print cc

    timestamp = int(time.mktime(now.timetuple())) / 300 * 300
    print timestamp