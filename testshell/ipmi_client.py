#!/usr/bin/env python
# coding: utf-8

import json

import httplib2

from common import conf


HTTP_POST = 'POST'
TYPE_JSON = 'application/json'

CHASSIS_PATH = '/chassis/'
MACHINE_PATH = '/machine/'
POWER_STATUS_PATH = 'power_status'
ALL_STATUS_PATH = 'status'
SHUTDOWN_PATH = "shutdown"
WAKEUP_PATH = "wake_up"
POH_PATH = 'poh'
FORCE_SHUTDOWN_PATH = "force_shutdown"
HARD_REBOOT_PATH = 'reset'
SOFT_REBOOT_PATH = 'reboot'


class IpmiClient:

    def __init__(self, config=None):
        if config is None:
            self.config = conf.read_conf()
        else:
            self.config = config
        self.endpoint = self.config.ipmi_url
        self.http_client = httplib2.Http()
        self.http_client.force_exception_to_status_code = True

    def run_wakeup(self, host):
        url = self.endpoint + CHASSIS_PATH + WAKEUP_PATH
        header = {'Content-type': TYPE_JSON}
        body = """{'username':'%s', 'password':'%s', 'host':'%s'}""" % (self.config.bmc_user,
                                                                        self.config.bmc_password,
                                                                        host)
        res = self.__http_request__(url, HTTP_POST, header, body)

        if '200' != res[0]['status']:
            return 'error'
        else:
            # json_content = json.loads(res[1])
            if '500' != res[0]['status']:
                return 'success'
            else:
                return 'error'

    def run_shutdown(self, host):
        url = self.endpoint + CHASSIS_PATH + SHUTDOWN_PATH
        header = {'Content-type': TYPE_JSON}
        body = """{'username':'%s', 'password':'%s', 'host':'%s'}""" % (self.config.bmc_user,
                                                                        self.config.bmc_password,
                                                                        host)
        res = self.__http_request__(url, HTTP_POST, header, body)

        if '200' != res[0]['status']:
            return 'error'
        else:
            # json_content = json.loads(res[1])
            if '500' != res[0]['status']:
                return 'success'
            else:
                return 'error'

    def run_force_shutdown(self, host):
        url = self.endpoint + CHASSIS_PATH + FORCE_SHUTDOWN_PATH
        header = {'Content-type': TYPE_JSON}
        body = """{'username':'%s', 'password':'%s', 'host':'%s'}""" % (self.config.bmc_user,
                                                                        self.config.bmc_password,
                                                                        host)
        res = self.__http_request__(url, HTTP_POST, header, body)

        if '200' != res[0]['status']:
            return 'error'
        else:
            # json_content = json.loads(res[1])
            if '500' != res[0]['status']:
                return 'success'
            else:
                return 'error'

    def run_reboot(self, host):
        url = self.endpoint + CHASSIS_PATH + SOFT_REBOOT_PATH
        header = {'Content-type': TYPE_JSON}
        body = """{'username':'%s', 'password':'%s', 'host':'%s'}""" % (self.config.bmc_user,
                                                                        self.config.bmc_password,
                                                                        host)
        res = self.__http_request__(url, HTTP_POST, header, body)

        if '200' != res[0]['status']:
            return 'error'
        else:
            # json_content = json.loads(res[1])
            if '500' != res[0]['status']:
                return 'success'
            else:
                return 'error'

    def run_reset(self, host):
        url = self.endpoint + CHASSIS_PATH + HARD_REBOOT_PATH
        header = {'Content-type': TYPE_JSON}
        body = """{'username':'%s', 'password':'%s', 'host':'%s'}""" % (self.config.bmc_user,
                                                                        self.config.bmc_password,
                                                                        host)
        res = self.__http_request__(url, HTTP_POST, header, body)

        if '200' != res[0]['status']:
            return 'error'
        else:
            # json_content = json.loads(res[1])
            if '500' != res[0]['status']:
                return 'success'
            else:
                return 'error'

    def get_all_status(self, host):
        url = self.endpoint + CHASSIS_PATH + ALL_STATUS_PATH
        header = {'Content-type': TYPE_JSON}
        body = """{'username':'%s', 'password':'%s', 'host':'%s'}""" % (self.config.bmc_user,
                                                                        self.config.bmc_password,
                                                                        host)
        res = self.__http_request__(url, HTTP_POST, header, body)
        if '200' != res[0]['status']:
            return 'error'
        else:
            if '200' == res[0]['status']:
                json_content = json.loads(res[1])
                return json_content['result']
            else:
                return 'error'

    def get_power_status(self, host):
        url = self.endpoint + CHASSIS_PATH + POWER_STATUS_PATH
        header = {'Content-type': TYPE_JSON}
        body = """{'username':'%s', 'password':'%s', 'host':'%s'}""" % (self.config.bmc_user,
                                                                        self.config.bmc_password,
                                                                        host)
        res = self.__http_request__(url, HTTP_POST, header, body)
          
        if '200' != res[0]['status']:
            print 'status error:' + str(res[0]['status'])
            print url, header, body
            return 'error'
        else:
            if '200' == res[0]['status']:
                print 'status 200'
                json_content = json.loads(res[1])
                return json_content['result']['power']
            else:
                print 'status error'
                return 'error'

    def get_poh(self, host):
        url = self.endpoint + MACHINE_PATH + POH_PATH
        header = {'Content-type': TYPE_JSON}
        body = """{'username':'%s', 'password':'%s', 'host':'%s'}""" % (self.config.bmc_user,
                                                                        self.config.bmc_password,
                                                                        host)
        res = self.__http_request__(url, HTTP_POST, header, body)

        if '200' != res[0]['status']:
            return 'error'
        else:
            if '200' == res[0]['status']:
                json_content = json.loads(res[1])
                return json_content['result']['poh']['POHCounter']
            else:
                return 'error'

    def __http_request__(self, url, method, header, body=None):
        resp, content = self.http_client.request(url, method, headers=header, body=body)
        return resp, content
