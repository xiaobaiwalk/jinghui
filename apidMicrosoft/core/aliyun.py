# coding:utf-8
import time
import json
from config import config

from aliyunsdkcore.client import AcsClient
from aliyunsdkcdn.request.v20180510 import DescribeDomainTrafficDataRequest
from aliyunsdkcdn.request.v20180510 import DescribeCdnDomainLogsRequest
from aliyunsdkcdn.request.v20180510 import DescribeDomainSrcTrafficDataRequest
from aliyunsdkcdn.request.v20180510 import DescribeDomainBpsDataRequest
from aliyunsdkcdn.request.v20180510 import DescribeDomainHttpCodeDataRequest
from aliyunsdkcdn.request.v20180510 import DescribeDomainRegionDataRequest
from aliyunsdkcdn.request.v20180510 import DescribeDomainQpsDataRequest
from aliyunsdkcdn.request.v20180510 import DescribeRangeDataByLocateAndIspServiceRequest


def get_flow(tp, cdn, url, ts, domain):
    """5 mins, utc time"""
    client = init_client(cdn)
    req = DescribeDomainTrafficDataRequest.DescribeDomainTrafficDataRequest()
    resp = req_common(client, req, domain, ts, ts + 300)
    return resp


def get_bandwith(tp, cdn, url, ts, domain):
    """5 mins, utc time"""
    client = init_client(cdn)
    req = DescribeDomainBpsDataRequest.DescribeDomainBpsDataRequest()
    resp = req_common(client, req, domain, ts, ts + 300)
    return resp


def get_origin_flow(tp, cdn, url, ts, domain):
    client = init_client(cdn)
    req = DescribeDomainSrcTrafficDataRequest.DescribeDomainSrcTrafficDataRequest()
    resp = req_common(client, req, domain, ts, ts + 300)
    return resp


def get_httpcode(tp, cdn, url, ts, domain):
    client = init_client(cdn)
    req = DescribeDomainHttpCodeDataRequest.DescribeDomainHttpCodeDataRequest()
    resp = req_common(client, req, domain, ts, ts + 300)
    return resp


def get_logs(tp, cdn, url, ts, domain):

    """one hour
    {"status":0, "data": [{"doamin": xxx, "urls": [url, url2]}]}
    """
    res = {"status": 0}
    client = init_client(cdn)
    req = DescribeCdnDomainLogsRequest.DescribeCdnDomainLogsRequest()
    resp = req_common(client, req, domain, ts, ts + 3600)
    res_json = json.loads(resp)
    details = res_json['DomainLogDetails']['DomainLogDetail']
    datas = []

    for detail in details:
        data = {"domain": detail['DomainName']}
        items = detail['LogInfos']['LogInfoDetail']
        urls = []
        for i in items:
            urls.append("http://{}".format(i["LogPath"]))

        data["urls"] = urls
        datas.append(data)

    res['data'] = datas
    return json.dumps(res)


def get_pv(tp, cdn, url, ts, domain):
    """5 mins"""
    client = init_client(cdn)
    req = DescribeDomainQpsDataRequest.DescribeDomainQpsDataRequest()
    resp = req_common(client, req, domain, ts, ts + 300)
    return resp


def get_fenbu(tp, cdn, url, tts, domain):
    """one day"""
    ts = tts / 86400 * 86400
    client = init_client(cdn)
    req = DescribeDomainRegionDataRequest.DescribeDomainRegionDataRequest()
    resp = req_common(client, req, domain, ts, ts + 3600*24)
    return resp

def get_flowfenbu(tp, cdn, url, ts, domain):
    client = init_client(cdn)
    req = DescribeRangeDataByLocateAndIspServiceRequest.DescribeRangeDataByLocateAndIspServiceRequest()
    req.set_accept_format("json")
    req.set_DomainNames(domain)
    start_utc = ts_to_utc_datetime(ts)
    end_utc = ts_to_utc_datetime(ts + 300)
    req.set_StartTime(start_utc)
    req.set_EndTime(end_utc)
    resp = client.do_action_with_exception(req)
    return resp


def init_client(cdn_account):
    client = AcsClient(
        config.get(cdn_account, "AK"),
        config.get(cdn_account, "SK"),
        "cn-hangzhou"
    )
    return client


def req_common(client, req, domain, start_ts, end_ts):
    start_utc = ts_to_utc_datetime(start_ts)
    end_utc = ts_to_utc_datetime(end_ts)
    req.set_accept_format("json")
    req.set_DomainName(domain)
    req.set_StartTime(start_utc)
    req.set_EndTime(end_utc)
    resp = client.do_action_with_exception(req)
    return resp


def ts_to_utc_datetime(ts):
    tss = ts/300*300 - 3600*8
    ltime = time.localtime(tss)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", ltime)
