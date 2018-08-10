'''
Created on 10 Aug 2018

@author: ostlerr
'''
import configparser
from datacite import DataCiteMDSClient

def getConfig():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def getDataCiteClient():
    config = getConfig()
    client = DataCiteMDSClient(
        username=config['DATACITE']['user'],
        password=config['DATACITE']['password'],
        prefix=config['DATACITE']['prefix'],
        test_mode=False
    )
    
    return client