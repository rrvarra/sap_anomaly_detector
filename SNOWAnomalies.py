# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 14:58:17 2018

@author: tghosh
"""


from data_service import es_query_engine as qe
from CONFIG import snow_event_config as conf
from anomaly_models.rule_based_anomalies import AnomalyRule
import pandas as pd
#from time import sleep
from utilities import email_manager as em

#import requests
#from requests.packages.urllib3.exceptions import InsecureRequestWarning
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

pd.set_option('display.max_colwidth', -1)

es_config = conf.ES_CONFIG
es = qe.get_es_instance(es_config=es_config)


rules = [AnomalyRule(name='User generates transaction over threshold',fieldname='counts',
                     threshold=1000),
         AnomalyRule(name='Disco Agent transations > 20M', fieldname='user_count', 
                     threshold=20000000), 
         AnomalyRule(name='Response status is 500', fieldname='response_status_count'),
         AnomalyRule(name="Monitor System Cache Synchronization Message",
                     threshold=8000000, fieldname='msg_count'),
]


    
users_res = qe.es_get_aggregations_data(es_instance=es,
                                   index=conf.ES_CONFIG['INDEX'],
                                   doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                   query=conf.USERS_GROUP_QUERY)


users_df = qe.read_aggregated_output(users_res, conf.SRC_FIELDS)
if users_df is not None:
    users_df['counts'] = users_df['counts'].apply(lambda x:int(x))


query_data = [

              users_df, 
              qe.es_get_aggregations_data(es_instance=es,
                                   index=conf.ES_CONFIG['INDEX'],
                                   doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                   query=conf.DISCOAGENTE_QUERY),

               
               qe.es_get_aggregations_data(es_instance=es,
                                   index=conf.ES_CONFIG['INDEX'],
                                   doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                   query=conf.RESPONSE_CODE_QUERY),

               qe.es_get_aggregations_data(es_instance=es,
                                   index=conf.ES_CONFIG['INDEX'],
                                   doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                   query=conf.RARE_MESSAGE)
               
               ]


                     

anomalies = pd.DataFrame()

data_loading_check = qe.es_get_aggregations_data(es_instance=es,
                                   index=conf.ES_CONFIG['INDEX'],
                                   doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                   query=conf.DATA_LOADS)

if data_loading_check['info']['total']==0:
    anomalies = pd.DataFrame({'timestamp':[conf.END_TS_PST],
    'name':['No Data Loaded for last 15 minutes']})
    
else:
    for rule_num in range(4):
        print(rules[rule_num].name)
        df = rules[rule_num].get_anomalies(data=query_data[rule_num])
        if not df.empty:
            if rule_num == 0:
                df.insert(0,column= 'username', value= df['user.keyword'].values)
                df = df.drop(['user.keyword'], axis=1)
            elif rule_num == 1:
                df.insert(0,column= 'username', value=  ['Disco.Agent'])
            anomalies=anomalies.append(df)

        if not anomalies.empty:
            anomalies.insert(0, column='timestamp' , value=[conf.END_TS_PST]*anomalies.shape[0])
            anomalies  = anomalies[['timestamp','name', 'counts','username']]
            print(anomalies.head(10))


if not anomalies.empty:
    sender = conf.EMAIL_FROM
    receivers = conf.EMAIL_TO
    email_title = conf.EMAIL_SUBJECT.format(anomaly_count=anomalies.shape[0])
    email_content = conf.EMAIL_TEMPLATE.format(time_period=conf.END_TS_PST,
                                                   anomaly_table=anomalies.to_html(escape=False,
                                                                                          index=False))
    
    
    em.send_html_email_alert(receiver_list=receivers, sender=sender, 
                             title=email_title, message=email_content)
