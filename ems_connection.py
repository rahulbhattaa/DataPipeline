# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 09:49:20 2022

@author: MEP7FE
"""
from pycelonis import get_celonis
import ems_secure
import sys
import os
import platform
import logging
from bosch_logging import init_logging
import socket

proxy_germany ='rb-proxy-de.bosch.com:8080'
proxy_india = 'https://rb-proxy-de.bosch.com:8080'

proxy = ''

ibc_url_production = "https://bosch.eu-4.celonis.cloud"
ibc_url = ibc_url_production

errorFound = False

### General Token (RESTFullMonitoring)
ibc_api_token_production = ems_secure.ibc_api_token_production

ibc_api_token = ibc_api_token_production
ibc_timeout = 1000

location = ''

# Connect String
login = {
    "url": ibc_url,
    "api_token": ibc_api_token,
    "timeout": ibc_timeout,
    "permissions": False,
    "connect": True,
    "key_type": "APP_KEY"
}

login_ems = { 
    "api_token": ibc_api_token,
    "key_type": "APP_KEY",
    "permissions": False
}

class EMS_Connection(object):
        
    def __init__(self):        
        print("######## EMS CONNECTION ###########")
        init_logging()
        self.__logger = logging.getLogger("ems_connection")
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)        

        self.__logger.debug("Python Version: " + sys.version) 
        self.__logger.debug("Python Location: " + os.path.dirname(sys.executable))
        self.__logger.debug("Celonis Connection API")
        self.__logger.debug("Configuring environment ...") 

        if self.__running_local():
            location = self.__get_location()
            self.__logger.info('>> I am running on a local environment in << ' + location + ' >>')
            if location != 'de':
                proxy = proxy_india
                # os.environ['http_proxy'] = proxy 
                # os.environ['HTTP_PROXY'] = proxy
                # os.environ['https_proxy'] = proxy
                # os.environ['HTTPS_PROXY'] = proxy
            else:
                proxy = proxy_germany
                #not necessary (anymore?)
                #os.environ['http_proxy'] = proxy 
                #os.environ['HTTP_PROXY'] = proxy
                #os.environ['https_proxy'] = proxy
                #os.environ['HTTPS_PROXY'] = proxy
            self.c_source = get_celonis(**login)            
        else:
            self.__logger.info('>> I am running on the cloud')
            self.c_source = get_celonis(**login_ems)            


    def __running_local(self):
        result = False
        if platform.system() == 'Windows':
            result = True

        return result

    def __get_location(self): 
        #BMH2-C-0006K.kor.apac.bosch.com   
        #PL-Z4528.    pl   .de.bosch.com     
        x = socket.getfqdn()
        sx = x.split('.')
        location = sx[-3]

        return location