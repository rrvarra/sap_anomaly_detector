# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 03:03:51 2018

@author: ad_tghosh
"""

from data_service import es_query_engine as qe
from CONFIG import sap_application_config as sapconf
import pandas as pd
from utilities import email_manager as em

#import requests
#from requests.packages.urllib3.exceptions import InsecureRequestWarning
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

pd.set_option('display.max_colwidth', -1)
#pd.options.display.float_format = '{0:.2f}'.format

# Initialize ES instance

# Initialize ES instance

es_config = sapconf.ES_CONFIG
es = qe.get_es_instance(es_config=es_config)


data_loading_check = qe.es_get_aggregations_data(es_instance=es,
                                   index=sapconf.ES_CONFIG['INDEX'],
                                   doc_type=sapconf.ES_CONFIG['DOC_TYPE'],
                                   query=sapconf.DATA_LOADS)

if data_loading_check['info']['total']==0:
    anomalies_df = pd.DataFrame({'timestamp':[sapconf.END_TS_PST],
    'name':['No Logs Loaded to elk for last 1 hour']})
    sender = sapconf.EMAIL_FROM
    receivers = sapconf.EMAIL_TO
    email_content = sapconf.EMAIL_TEMPLATE_NODATA.format(anomaly_table=anomalies_df.to_html(escape=False,
                                                                                              index=False))
        
        
    em.send_html_email_alert(receiver_list=receivers, sender=sender, 
                                 title='No SAP Logs Data Loading', message=email_content)

else:
    
    results = qe.es_get_search_data(es_instance=es,
                          index = sapconf.ES_CONFIG['INDEX'],
                          doc_type = sapconf.ES_CONFIG['DOC_TYPE'],
                          query = sapconf.PRINT_ERR_QUERY)
    
    if len(results) > 0:
        data = [ doc["_source"] for doc in results]    
        anomalies_df = pd.DataFrame(data)
        
    
        sender = sapconf.EMAIL_FROM
        receivers = sapconf.EMAIL_TO
        email_content = sapconf.EMAIL_TEMPLATE.format(anomaly_table=anomalies_df.to_html(escape=False,
                                                                                              index=False))
        
        
        em.send_html_email_alert(receiver_list=receivers, sender=sender, 
                                 title=sapconf.EMAIL_SUBJECT, message=email_content)
