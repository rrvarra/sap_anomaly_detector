# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 02:12:00 2018

@author: ad_tghosh, dipanjan
"""

from data_service import es_query_engine as qe
from CONFIG import messaging_event_config as conf
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
idam_evt_grps = {}

for idx, row in event_types_df.iterrows():
    # get data for each event    
    #print(row)
    query = conf.get_event_counts_query(**row)
    results = qe.es_get_aggregations_data(es_instance=es,
                                   index=conf.ES_CONFIG['INDEX'],
                                   doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                   query=query)
    
    df_tmp = pd.DataFrame(results['group_by_datetime']['buckets'])
    df_tmp['timestamp'] = pd.to_datetime(df_tmp['key_as_string'])
    df_tmp=df_tmp.drop(['key_as_string','key'], axis =1)
    try:
        description = results['info']['hits'][0]['_source']['Description']
    except:
        description = "Not Found"
    
    # get time series interval data
    ts_df = df_tmp[['timestamp', 'doc_count']]
    ts = pd.Series(ts_df.doc_count)
    ts.index = ts_df.timestamp  
    
    
    # store history for each event > 10 data points
    if len(ts) > 10:
        # actual processing\execution
        ts_obj = TimeSeries(ts, frequency=24)
        anomaly_model = SeasonalEsdEwma(ts_obj, check_recency_weeks=2)
        #print(anomaly_model.get_anomaly_url(**row))
        anomaly_df = anomaly_model.get_anomalies()
        
        evt_type = row[[ 'LogName', 'Source', 'EventID']]
        key = '-'.join(list(evt_type))
        evt_details = dict(evt_type)
        evt_details['description'] = description
        kibana_params = {'source': evt_type['Source'],
                         'log_name': evt_type['LogName'],
                         'event_id': evt_type['EventID']
                         }
        idam_evt_grps[key] = {'event_type' : evt_details}
        idam_evt_grps[key]['raw_ts'] = ts
        
        
        if not anomaly_df.empty:
            #url=anomaly_model.get_anomaly_url(**row)
            anomaly_df['KibanaURL'] = conf.KIBANA_BASE_URL.format(**kibana_params)
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
all_anomalies_df = all_anomalies_df[all_anomalies_df.timestamp == conf.END_DAY]


if not all_anomalies_df.empty:
    all_anomalies_df = all_anomalies_df.sort_values(by=['score'], ascending=False)
    all_anomalies_df = dp.embed_df_column_url(df=all_anomalies_df, 
                                              url_column='KibanaURL',
                                              embed_text='Dashboard')
    
    
    def get_host_details(record, top_hosts=3):    
        host_query = conf.get_anomaly_hosts_query(**record)
        response = qe.es_get_aggregations_data(es, 
                                               index=conf.ES_CONFIG['INDEX'],
                                               doc_type=conf.ES_CONFIG['DOC_TYPE'],
                                               query=host_query)
        data = {'Unique Hosts': response['distinct_hosts']['value'],
                'Top Hosts': {item['key'] : item['doc_count'] 
                    for item in response['hosts']['buckets'][:3]}
                }
        return pd.Series(data)
    
    host_details_df = all_anomalies_df.apply(lambda row: get_host_details(row), 
                                          axis=1)
    all_anomalies_df = pd.concat([all_anomalies_df, host_details_df], 
                                 axis=1)
    
    
    columns = ['timestamp', 'LogName', 'Source', 'EventID',
               'count', 'score', 'Unique Hosts', 'Kibana', 'Top Hosts', 'description']
    all_anomalies_df = all_anomalies_df[columns]
    
    
    
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

