# -*- coding: utf-8 -*-

__author__ = 'Lincoln'
from config import config
import simplejson as json
import requests
import urllib2
import urllib
import logging
import sys

logger = logging.getLogger("worker")

def getter(url,p,headers={}):
    try:
        logger.debug((url,p))
        r = requests.get(url, params=p, headers=headers,timeout=config.getint("http","timeout"),verify=False)
        return r.text
    except:
        logger.debug(sys.exc_info())
        return None

def poster(url,p,headers=None):
    try:
        logger.debug((url,p))
        r = requests.post(url, data=json.dumps(p), timeout=config.getint("http","timeout"),headers=headers)
        return r.text
    except:
        logger.debug(sys.exc_info())
        return None

def doPost(url, data):
    try:
        logger.debug((url,data))
        data = urllib.urlencode(data)
        req = urllib2.Request(url)
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
        response = opener.open(req, data)
        return response.read()
    except:
        logger.debug(sys.exc_info())
        return None