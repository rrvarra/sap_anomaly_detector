# -*- coding: utf-8 -*-
import logging
from pprint import pformat, pprint
import json

from elasticsearch.helpers import scan
from elasticsearch import Elasticsearch
import pandas as pd

from utilities import credentials_manager as cm

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)


MAX_AGG_DEPTH = 5

def get_es_instance(es_config):
    hosts = es_config.get('HOST')
    ca_certs = es_config.get('ca_certs')
    user_name = es_config.get('USER_NAME')
    timeout = es_config.get('timeout')

    config_args = {}
    if ca_certs:
        config_args['ca_certs'] = ca_certs
    if timeout:
        config_args['timeout'] = timeout
    if isinstance(user_name, str):
        service_name = es_config.get('SERVICE_NAME')
        config_args['http_auth'] = cm.get_elk_auth()

    es = Elasticsearch(hosts=hosts, **config_args)
    return es



def es_get_aggregations_data(es_instance, index, doc_type, query, filter_path=None):

    #response = es_instance.search(index=index, doc_type=doc_type,
    #                              body=query, filter_path=filter_path, request_timeout=60)
    logging.info("Query: %s", json.dumps(query, indent=2))
    logging.info("Filter: %s", filter_path)
    response = es_instance.search(index=index,
                                  body=query, filter_path=filter_path, request_timeout=60
                                  )
    agg_results = response.get('aggregations')
    agg_results['info'] = response.get('hits')
    return agg_results

def es_get_aggregations_data_all(es_instance, index, doc_type, query, filter_path=None):

    response = es_instance.search(index=index, doc_type=doc_type,
                                  body=query, filter_path=filter_path, request_timeout=60)
    return response



def es_get_search_data(es_instance, index, doc_type, query, filter_path=None):

    response = scan(client=es_instance, index=index, doc_type=doc_type,
                    query=query, filter_path=filter_path, request_timeout=60)
    data = list(response)
    return data




def read_aggregated_output(agg_results, source_fields):
    """reads the aggregated counts output from es results.
    Expects nested agg fields to follow the same order as in source_fields"""
    if agg_results['info']['total']==0:
        return None
    n = len(source_fields)

    if n > MAX_AGG_DEPTH:
        raise ValueError("Can parse upro 4 aggregation levels only")
    event_groups =[]
    for sub_aggr_0 in agg_results["by_"+source_fields[0]]["buckets"]:
        value_0 = sub_aggr_0['key']
        if n==1:
            event_groups.append({source_fields[0]:value_0,
                                 "counts":sub_aggr_0['doc_count']})
        else:
            for sub_aggr_1 in sub_aggr_0["by_"+source_fields[1]]['buckets']:
                value_1 = sub_aggr_1['key']
                if n==2:
                    event_groups.append({source_fields[0]:value_0,
                                     source_fields[1]:value_1,
                                     "counts":sub_aggr_1['doc_count']})
                else:
                    for sub_aggr_2 in sub_aggr_1["by_"+source_fields[2]]['buckets']:
                        value_2 = sub_aggr_2['key']
                        if n==3:
                            event_groups.append({source_fields[0]:value_0,
                                             source_fields[1]:value_1,
                                             source_fields[2]:value_2,
                                             "counts":sub_aggr_2['doc_count']})

                        else:
                            for sub_aggr_3 in sub_aggr_2["by_"+source_fields[3]]['buckets']:
                                value_3 = sub_aggr_3['key']
                                if n==4:
                                    event_groups.append({source_fields[0]:value_0,
                                                     source_fields[1]:value_1,
                                                     source_fields[2]:value_2,
                                                     source_fields[3]:value_3,
                                                     "counts":sub_aggr_3['doc_count']})
                                else:
                                    for sub_aggr_4 in sub_aggr_3["by_"+source_fields[4]]['buckets']:
                                        value_4 = sub_aggr_4['key']
                                        if n==5:
                                            event_groups.append({source_fields[0]:value_0,
                                                             source_fields[1]:value_1,
                                                             source_fields[2]:value_2,
                                                             source_fields[3]:value_3,
                                                             source_fields[4]:value_4,
                                                             "counts":sub_aggr_4['doc_count']})
    eventtypes = pd.DataFrame(event_groups)
    eventtypes = eventtypes.sort_values(['counts'],ascending=[0])
    return eventtypes
