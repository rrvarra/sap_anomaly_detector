# -*- coding: utf-8 -*-
"""
Created on Mon Apr 09 22:02:00 2018

@author: ad_sharmatu
"""
from data_service import es_query_engine as qe
from CONFIG import skype_diag_config as skypeconf
import pandas as pd
from data_service.timeseries import TimeSeries
from anomaly_models.ts_anomalies import SeasonalEsdEwma
from data_service import data_processor as dp
from utilities import email_manager as em

pd.set_option('display.max_colwidth', -1)
# pd.options.display.float_format = '{0:.2f}'.format

# Initialize ES instance
es = qe.get_es_instance(skypeconf.ES_CONFIG)

#Get all types of error events
event_types_res = qe.es_get_aggregations_data(es_instance=es,
                                              index=skypeconf.ES_CONFIG['INDEX'],
                                              doc_type=skypeconf.ES_CONFIG['DOC_TYPE'],
                                              query=skypeconf.EVENT_GROUPS_QUERY)

event_types_df = qe.read_aggregated_output(event_types_res, skypeconf.SRC_FIELDS)
event_types_df = event_types_df.sort_values(['counts'], ascending=False)

#GET join count for all the days in the range
join_count_res = qe.es_get_aggregations_data(es_instance=es,
                                              index=skypeconf.ES_CONFIG['INDEX'],
                                              doc_type=skypeconf.ES_CONFIG['DOC_TYPE'],
                                              query=skypeconf.JOIN_COUNT_QUERY)
join_count_df = pd.DataFrame(join_count_res['group_by_datetime']['buckets'])
join_count_df['timestamp'] = pd.to_datetime(join_count_df['key_as_string'])
join_count_df = join_count_df.drop(['key_as_string', 'key'], axis=1)
join_count_df = join_count_df[join_count_df.doc_count>0]
join_count_df.rename(columns={'doc_count':'join_count'}, inplace=True)

# Get anomalies for each event
skype_diagid_grps = {}

for idx, row in event_types_df.iterrows():
    query = skypeconf.get_event_counts_query(**row)
    results = qe.es_get_aggregations_data(es_instance=es,
                                          index=skypeconf.ES_CONFIG['INDEX'],
                                          doc_type=skypeconf.ES_CONFIG['DOC_TYPE'],
                                          query=query)

    df_tmp = pd.DataFrame(results['group_by_datetime']['buckets'])
    df_tmp['timestamp'] = pd.to_datetime(df_tmp['key_as_string'])
    df_tmp = df_tmp.drop(['key_as_string', 'key'], axis=1)
    try:
        description = results['info']['hits'][0]['_source']['Description']
    except:
       description = "Not Found"

    ## get time series interval data
    ts_df = df_tmp[['timestamp', 'doc_count']]
    ts_df = pd.merge(join_count_df, ts_df, how='inner', on='timestamp')
    ts_df['norm_error_count'] = ts_df['doc_count']/ts_df['join_count']
    ts = pd.Series(ts_df.norm_error_count)
    ts.index = pd.to_datetime(ts_df.timestamp)
    # store history for each event > 10 data points
  
    ts_obj = TimeSeries(ts, frequency=24)
    ts_obj.apply_concept_drift_test(recency_weeks=skypeconf.RUN_MODEL_RECENCY_WEEKS)

    if ts_obj.concept_drift_present:
        pass
    else:
        anomaly_model = SeasonalEsdEwma(ts_obj, plot_series=False,
                                        check_recency_weeks=skypeconf.RUN_MODEL_RECENCY_WEEKS)
        anomaly_model.ewma_halflife = 0.5
        # print(anomaly_model.get_anomaly_url(**row))
        anomaly_df = anomaly_model.get_anomalies()

        evt_type = str(row['MsDiagId'])
        key = str(row['MsDiagId'])
        evt_details = dict()
        evt_details['description'] = description
        skype_diagid_grps[key] = {'event_ID': evt_details}
        skype_diagid_grps[key] = {'event_type': evt_details}
        skype_diagid_grps[key]['raw_ts'] = ts

        if not anomaly_df.empty:
            #url=anomaly_model.get_anomaly_url(**row)
            anomaly_df['KibanaURL'] = skypeconf.KIBANA_BASE_URL.format(row['MsDiagId'])
            # Need to add this url to dataframe
            skype_diagid_grps[key]['anomalies'] = anomaly_df
            skype_diagid_grps[key]['ConferenceUriCount'] = df_tmp.iloc[-1].ConferenceUriCount['value']
            skype_diagid_grps[key]['FromUriCount'] = df_tmp.iloc[-1].FromUriCount['value']
            skype_diagid_grps[key]['ToUriCount'] = df_tmp.iloc[-1].ToUriCount['value']
            skype_diagid_grps[key]['ErrorCount'] = df_tmp.iloc[-1].doc_count

all_anomalies = [pd.concat([pd.DataFrame([skype_diagid_grps[key].get('event_type')]) \
                           .reset_index(drop=True),
                            skype_diagid_grps[key].get('anomalies') \
                           .reset_index(drop=True),
                            pd.DataFrame([skype_diagid_grps[key].get('ConferenceUriCount')],columns=['ConferenceUriCount']) \
                           .reset_index(drop=True),
                            pd.DataFrame([skype_diagid_grps[key].get('FromUriCount')],columns=['FromUriCount']) \
                           .reset_index(drop=True),
                            pd.DataFrame([skype_diagid_grps[key].get('ToUriCount')],columns=['ToUriCount']) \
                           .reset_index(drop=True),
                           pd.DataFrame([skype_diagid_grps[key].get('ErrorCount')],columns=['ErrorCount']) \
                           .reset_index(drop=True),
                            pd.DataFrame([key],columns=['MsDiagId']).reset_index(drop=True)
                            ],
                           # ignore_index=True, 
                           axis=1)
                 for key in skype_diagid_grps.keys()
                 if 'anomalies' in skype_diagid_grps[key].keys()]

all_anomalies_df = pd.concat(all_anomalies, axis=0).reset_index(drop=True)
all_anomalies_df = all_anomalies_df[all_anomalies_df.timestamp == skypeconf.END_DAY]
all_anomalies_df = all_anomalies_df.sort_values(by=['score', 'count'], ascending=False)
filtered_anomalies_df = all_anomalies_df[all_anomalies_df.ErrorCount > 10]
total_anomalies = len(filtered_anomalies_df)

if not filtered_anomalies_df.empty:
	filtered_anomalies_df = dp.embed_df_column_url(df=filtered_anomalies_df,
											  url_column='KibanaURL',
											  embed_text='Dashboard')

from utilities import email_manager as em

sender = skypeconf.EMAIL_FROM
receivers = skypeconf.EMAIL_TO
email_title = skypeconf.EMAIL_SUBJECT.format(anomaly_count=total_anomalies)
email_content = skypeconf.EMAIL_TEMPLATE.format(time_period=skypeconf.END_DAY,
												  anomaly_table=filtered_anomalies_df.to_html(escape=False,
																						  index=False))

em.send_html_email_alert(receiver_list=receivers, sender=sender,
							 title=email_title, message=email_content)
