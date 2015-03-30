#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from django.conf import settings
from oslo.config import cfg


def read_conf():
    if not cfg.CONF._opts:
        ipmi_opts = [
            cfg.StrOpt('openam_url',
                       default='http://127.0.0.1/openam'),
            cfg.StrOpt('debug_openam_url',
                       default='http://127.0.0.1/openam'),
            cfg.StrOpt('ipmi_url',
                       default='http://127.0.0.1/ipmi'),
            cfg.StrOpt('zabbix_url',
                       default='http://127.0.0.1/zabbix'),
            cfg.StrOpt('bmc_user',
                       default='root'),
            cfg.StrOpt('bmc_password',
                       default='password'),
            cfg.BoolOpt('debug',
                        default=False),
            cfg.BoolOpt('debug_authenticate',
                        default=False),
            cfg.StrOpt('is_admin_role',
                       default='admin'),
            cfg.BoolOpt('is_demo_mode',
                        default=False)
        ]
        cfg.CONF.register_cli_opts(ipmi_opts)
        print '/etc/ool_br_rest' + '/ipmi.conf'
        cfg.CONF(['--config-file', '/etc/ool_br_rest' + '/ipmi.conf'])
        print '===================LOADING CONFIG====================='
        for k, v in cfg.CONF.items():
            print '%s = %s' % (str(k), str(v))
        print '===================LOADING CONFIG====================='
    return cfg.CONF

