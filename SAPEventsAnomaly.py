# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 14:22:15 2018

@author: tghosh
"""

from data_service import es_query_engine as qe
from CONFIG import sap_event_config as sapconf
import pandas as pd
from data_service.timeseries import TimeSeries
from anomaly_models.ts_anomalies import SeasonalEsdEwma
from data_service import data_processor as dp
import logutil


log_util.set_logging(r'D:\LOGS\SAP_ANOMALY')
#import requests
#from requests.packages.urllib3.exceptions import InsecureRequestWarning
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

pd.set_option('display.max_colwidth', -1)
#pd.options.display.float_format = '{0:.2f}'.format

# Initialize ES instance

# Initialize ES instance

es_config = sapconf.ES_CONFIG
es = qe.get_es_instance(es_config=es_config)

# Get all types of error events
event_types_res = qe.es_get_aggregations_data(es_instance=es,
                                   index=sapconf.ES_CONFIG['INDEX'],
                                   doc_type=sapconf.ES_CONFIG['DOC_TYPE'],
                                   query=sapconf.EVENT_GROUPS_QUERY)

event_types_df = qe.read_aggregated_output(event_types_res, sapconf.SRC_FIELDS)
#event_types_df = event_types_df.sort_values(['counts'],ascending=False)
print('EventTypes:', event_types_df.shape)
print(event_types_df.head(10))



# Get anomalies for each event
idam_evt_grps = {}

for idx, row in event_types_df.iterrows():
    # get data for each event    
    #print(row)
    query = sapconf.get_event_counts_query(**row)
    results = qe.es_get_aggregations_data(es_instance=es,
                                   index=sapconf.ES_CONFIG['INDEX'],
                                   doc_type=sapconf.ES_CONFIG['DOC_TYPE'],
                                   query=query)
    
    df_tmp = pd.DataFrame(results['group_by_datetime']['buckets'])
    df_tmp['timestamp'] = pd.to_datetime(df_tmp['key_as_string'])
    df_tmp=df_tmp.drop(['key_as_string','key'], axis =1)
    print(df_tmp.shape)
    try:
        description = results['info']['hits'][0]['_source']['Description']
    except:
        description = "Not Found"
    
    # get time series interval data
    ts_df = df_tmp[['timestamp', 'doc_count']]
    ts = pd.Series(ts_df.doc_count)
    ts.index = ts_df.timestamp   
    
    #added 0 bins to data - no need to check num points
    ts_obj = TimeSeries(ts, frequency=24)
    ts_obj.apply_concept_drift_test(recency_weeks=sapconf.RUN_MODEL_RECENCY_WEEKS)
    
    if ts_obj.concept_drift_present:
        pass
    else:
        anomaly_model = SeasonalEsdEwma(ts_obj, 
                                        check_recency_weeks=sapconf.RUN_MODEL_RECENCY_WEEKS)
        #print(anomaly_model.get_anomaly_url(**row))
        anomaly_df = anomaly_model.get_anomalies()
            
        evt_type = row[sapconf.SRC_FIELDS]
        key = '-'.join(list(evt_type))
        evt_details = dict(evt_type)
        evt_details['description'] = description
        kibana_params = {'source': evt_type['Source'],
                         'log_name': evt_type['LogName'],
                         'event_id': evt_type['EventID'],                         
                         'pipe_name': evt_type['HostInfo.PipeName']}
        idam_evt_grps[key] = {'event_type' : evt_details}
        idam_evt_grps[key]['raw_ts'] = ts
                     
        if not anomaly_df.empty:
            anomaly_df['KibanaURL'] = sapconf.KIBANA_BASE_URL.format(**kibana_params)
            #Need to add this url to dataframe
            idam_evt_grps[key]['anomalies'] = anomaly_df


all_anomalies = [pd.concat([pd.DataFrame([idam_evt_grps[key].get('event_type')])\
                                                            .reset_index(drop=True),
                            idam_evt_grps[key].get('anomalies')\
                                              .reset_index(drop=True)
                            ],
                           #ignore_index=True, 
                           axis=1)
                for key in idam_evt_grps.keys() 
                    if 'anomalies' in idam_evt_grps[key].keys()]   
    
all_anomalies_df = pd.concat(all_anomalies, axis=0).reset_index(drop=True)
all_anomalies_df = all_anomalies_df[all_anomalies_df.timestamp == sapconf.END_DAY]


if not all_anomalies_df.empty:
    all_anomalies_df = all_anomalies_df.sort_values(by=['score', 'count'], ascending=False)
    all_anomalies_df = dp.embed_df_column_url(df=all_anomalies_df, 
                                              url_column='KibanaURL',
                                              embed_text='Dashboard')
             
    
    all_anomalies_df = all_anomalies_df[all_anomalies_df['count']>3]
    
    columns = ['timestamp', 'HostInfo.PipeName', 'LogName', 'Source', 'EventID',
               'count', 'score', 'Kibana', 'description']
    all_anomalies_df = all_anomalies_df[columns]
    print('All Anomalies before filter: ', all_anomalies_df.shape, ' Threshold: ', sapconf.MIN_THRESHOLDS)
    all_anomalies_df=SeasonalEsdEwma._filter_df_by_threshold(all_anomalies_df,
                                                             'count', 
                                                             sapconf.MIN_THRESHOLDS)
    print('Filtered Anomalies: ', all_anomalies_df.shape)

print("Filtered",all_anomalies_df.shape)
total_anomalies = len(all_anomalies_df)

if total_anomalies:
    from utilities import email_manager as em

    sender = sapconf.EMAIL_FROM
    receivers = sapconf.EMAIL_TO
    email_title = sapconf.EMAIL_SUBJECT.format(anomaly_count=total_anomalies)
    email_content = sapconf.EMAIL_TEMPLATE.format(time_period=sapconf.END_DAY,
                                                anomaly_table=all_anomalies_df.to_html(escape=False,
                                                                                          index=False))

    print("Sending EMAIL:", email_title)
    em.send_html_email_alert(receiver_list=receivers, sender=sender, 
                             title=email_title, message=email_content)
