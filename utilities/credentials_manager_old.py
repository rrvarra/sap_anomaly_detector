# -*- coding: utf-8 -*-
"""
Created on Wed Mar 28 20:02:56 2018

@author: ad_sarkardi
"""
"""
import rv.crypt
import getpass
import json

def set_keyring(service_name, user_name, secret_object):
        '''
        Configure the json encoded secret object in keyring under  service_name and username.
        Need to call this function to set credentials from a sys account elevated cmd prompt
        :param username: keyring username
        :param secret_object: object to be json encoded and stored.
        :return: None
        '''
        
        print("Setting KeyRing for Service: {} Name: {} - Account: {}".format(service_name,
                                                                                     user_name, getpass.getuser()))

        value = json.dumps(secret_object)
        cr = rv.crypt.Crypt()
        cr.keyring_set(service_name=service_name, user_name=user_name,
                       password=value, salted=True)

        v = json.loads(cr.keyring_get(service_name=service_name, user_name=user_name,
                                      salted=True))
        assert v == secret_object, "Keyring set/get mismatch expected {} got {}".format(secret_object, v)

    

def get_keyring(service_name, user_name):
    '''
    Retrieve the json encoded secret object fro keyring under service_name and username.
    :param username: keyring username
    :return: secret_object
    '''

    cr = rv.crypt.Crypt()
    v = None
    try:
        v = json.loads(cr.keyring_get(service_name=service_name, user_name=user_name,
                                      salted=True))
    except:
        msg = "Failed to get keyring data for Service: {} User: {} - under account {}".format( \
                service_name, user_name, getpass.getuser())
        print(msg) # need to log it later
    return v


"""