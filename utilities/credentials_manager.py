# -*- coding: utf-8 -*-
"""
Created on Wed Mar 28 20:02:56 2018
"""

import os


def get_elk_auth():
    env_vars = 'SAP_ES_USER', 'SAP_ES_PASSWORD'
    for env_var in env_vars:
        if env_var not in os.environ:
            raise Exception("Missing OS ENV var '%s'" % (env_var,))

    return [os.environ[env_var].lstrip('"').rstrip('"') for env_var in env_vars]
