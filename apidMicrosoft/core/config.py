# -*- coding: utf-8 -*-

__author__ = 'Lincoln'

import ConfigParser,os

# FILE = '/Application/config/get_api.conf'
FILE = 'E:\\apid\\get_api.conf'

if not os.path.isfile(FILE):
    raise ConfigParser.Error("No get_api.conf in PATH")

config = ConfigParser.RawConfigParser()
config.read(FILE)

def initConfig():
    config = ConfigParser.RawConfigParser()
    # config.read('/Application/config/api_list.conf')
    config.read('E:\\apid\\api_list.conf')
    return config

if __name__ == '__main__':
    print FILE