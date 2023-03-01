# -*- coding: utf-8 -*-
"""
Created on Wed May 23 16:30:53 2018

@author: tghosh
"""

from data_service import es_query_engine as qe
from CONFIG import wlan_event_config as conf
import pandas as pd
from data_service.timeseries import TimeSeries
from anomaly_models.ts_anomalies import SeasonalEsdEwma
from data_service import data_processor as dp


pd.set_option('display.max_colwidth', -1)
#pd.options.display.float_format = '{0:.2f}'.format

# Initialize ES instance

es_config = conf.ES_CONFIG
es = qe.get_es_instance(es_config=es_config)


# Get all types of error events
event_types_res = qe.es_get_aggregations_data(es_instance=es,
                                   index=conf.ES_CONFIG['INDEX'],
                                   doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                   query=conf.EVENT_GROUPS_QUERY)

event_types_df = qe.read_aggregated_output(event_types_res, conf.SRC_FIELDS)
#event_types_df = event_types_df.sort_values(['counts'],ascending=False)
print(event_types_df.head(10))
print(event_types_df.shape)

# Get anomalies for each event
wlan_host_grps = {}

for idx, row in event_types_df.iterrows():
    # get data for each event    
    
    query = conf.get_event_counts_query(**row)
    results = qe.es_get_aggregations_data(es_instance=es,
                                   index=conf.ES_CONFIG['INDEX'],
                                   doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                   query=query)
    df_tmp = pd.DataFrame(results['group_by_datetime']['buckets'])
    df_tmp['timestamp'] = pd.to_datetime(df_tmp['key_as_string'])
    df_tmp=df_tmp.drop(['key_as_string','key'], axis =1)
    
    # get time series interval data
    ts_df = df_tmp[['timestamp', 'doc_count']]
    ts = pd.Series(ts_df.doc_count)
    ts.index = ts_df.timestamp   
    # actual processing\execution
    ts_obj = TimeSeries(ts, frequency=24)
    ts_obj.apply_concept_drift_test(recency_weeks=conf.RUN_MODEL_RECENCY_WEEKS)
    
    if ts_obj.concept_drift_present:
        pass
    else:
        anomaly_model = SeasonalEsdEwma(ts_obj, 
                                        check_recency_weeks=conf.RUN_MODEL_RECENCY_WEEKS)
        #print(anomaly_model.get_anomaly_url(**row))
        anomaly_df = anomaly_model.get_anomalies()
        evt_type = row[['host_full','tag1']]
        key = '-'.join(list(evt_type))
        evt_details = dict(evt_type)
        kibana_params = {'host_full': evt_type['host_full'], 'tag1':evt_type['tag1']}
        wlan_host_grps[key] = {'event_type' : evt_details}
        wlan_host_grps[key]['raw_ts'] = ts
        if not anomaly_df.empty:
            #url=anomaly_model.get_anomaly_url(**row)
            anomaly_df['KibanaURL'] = conf.KIBANA_BASE_URL.format(**kibana_params)
            #Need to add this url to dataframe
            wlan_host_grps[key]['anomalies'] = anomaly_df
            
        
    
    

all_anomalies = [pd.concat([pd.DataFrame([wlan_host_grps[key].get('event_type')])\
                                                            .reset_index(drop=True),
                            wlan_host_grps[key].get('anomalies')\
                                              .reset_index(drop=True)
                            ],
                           #ignore_index=True, 
                           axis=1)
                for key in wlan_host_grps.keys() 
                    if 'anomalies' in wlan_host_grps[key].keys()]   

all_anomalies_df = pd.concat(all_anomalies, axis=0).reset_index(drop=True)
all_anomalies_df = all_anomalies_df[all_anomalies_df.timestamp == conf.END_DAY]
all_anomalies_df = all_anomalies_df[all_anomalies_df.score > 0.5]

if not all_anomalies_df.empty:
    all_anomalies_df = all_anomalies_df.sort_values(by=['score', 'count'], ascending=False)
    all_anomalies_df = dp.embed_df_column_url(df=all_anomalies_df, 
                                              url_column='KibanaURL',
                                              embed_text='Dashboard')
    
print(all_anomalies_df.shape)
total_anomalies = len(all_anomalies_df)


from utilities import email_manager as em

sender = conf.EMAIL_FROM
receivers = conf.EMAIL_TO
email_title = conf.EMAIL_SUBJECT.format(anomaly_count=total_anomalies)
email_content = conf.EMAIL_TEMPLATE.format(time_period=conf.END_DAY,
                                               anomaly_table=all_anomalies_df.to_html(escape=False,
                                                                                      index=False))


em.send_html_email_alert(receiver_list=receivers, sender=sender, 
                         title=email_title, message=email_content)    
