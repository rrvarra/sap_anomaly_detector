# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 22:01:08 2018

@author: ad_tghosh/ad_sarkardi/rrvarra
"""
import logging
from data_service.timeseries import TimeSeries
import matplotlib.pyplot as plt
import anomaly_models.smirnov_grubbs_esd as sge
import pandas as pd
import numpy as np

class AnomalyModel:
    def __init__(self, timeseries, adjust_seasonality, 
                 seasonal_period='weekly',
                 check_recency_weeks=2):
        #print(timeseries.__class__.__name__)
        if not isinstance(timeseries, TimeSeries) :
            raise TypeError ("Data must be of type TimeSeries")
        self.timeseries = timeseries
        self.frequency = timeseries.frequency
        self.data_slice_index = -(check_recency_weeks*24*7 // timeseries.frequency)
        self.timeseries_slice = TimeSeries(timeseries.ts[self.data_slice_index:],
                                           frequency=self.frequency)
        self.adjust_seasonality = adjust_seasonality
    
    def get_anomalies(self, add_kibana_link=False, **kwargs):
        indx, scores = self._get_anomaly_indices_scores()
        scores = np.round(scores, 2)
        if len(indx):
            logging.info('Indx %s Scores: %s', indx, scores)
        
        anomaly_df = self.timeseries_slice.subsample(indx)
        anomaly_df = pd.DataFrame(anomaly_df).reset_index()
        anomaly_df = pd.concat([anomaly_df, pd.DataFrame(scores,
                                                         columns=['score'])], 
                               axis=1)
        anomaly_df.columns = ['timestamp', 'count', 'score']
        anomaly_df = anomaly_df.sort_values(by=['timestamp'])
        
        latest_anomaly = anomaly_df[-1:]
        
        if add_kibana_link:
            latest_anomaly['kibanaurl'] = self._get_anomaly_url(latest_anomaly.index, **kwargs)
        return latest_anomaly 
    
    def _get_anomaly_indices_scores(self):
        raise NotImplementedError('Please implement get_anomaly_indices() method')
    
    def _get_anomaly_url(self, **kwargs):
        raise NotImplementedError
    

    def _filter_df_by_threshold(df, threshold_col_name, min_threshold=[]):
        """ Filter anomaly dataframe rows by some threshold column value
        
            Arguments:
            df -- anomaly dataframe
            threshold_col_name -- name of threshold value column in dataframe: eg event count 
            min_threshold -- a list of dict, each having filter key-value entries
                             to filter rows in df and apply threshold filter and
                             a 'Threshold' key. Default threshold = 0.
            Returns:
                filterd dataframe
        """
        if len(min_threshold) > 0:
            try:
                for filter_row in min_threshold:
                    
                    threshold = filter_row.get('Threshold', 0)
                    df_tmp = df
                    for k, v in filter_row.items():
                        if k in df.columns:
                            df_tmp = df_tmp[df_tmp[str(k)]==v]
                    
                    if df_tmp[threshold_col_name].sum() < threshold:
                        df = df.drop(df_tmp.index)
            except:
                logging.error('FAILED anomaly filtering by mincount')
        return df
                
class SeasonalEsdEwma(AnomalyModel):    
    
    def __init__(self, timeseries, seasonal_period='weekly', 
                 check_recency_weeks=2,
                 adjust_seasonality=True,plot_series = False):
        
        super().__init__(timeseries=timeseries, 
                         adjust_seasonality=adjust_seasonality,
                         check_recency_weeks=check_recency_weeks,
                         seasonal_period=seasonal_period)
        
        self.ewma_halflife = 1
        self.esd_alpha = 0.05
        self.mindatalen = 10
        self.show_plots = plot_series
        
    def _get_anomaly_indices_scores(self):      
        
        if self.adjust_seasonality:
            res_series, seasonal_component = self.timeseries.remove_seasonal() 
        else:
            '''only ewma + esd'''
            res_series = self.timeseries
        
        ts_res = res_series.ewma_diff(self.ewma_halflife)
        if self.show_plots:
            plt.plot(res_series.get_rawdata(),  label='original')            
            if self.adjust_seasonality:
                plt.plot(ts_res.get_rawdata(),  label='residual') 
                plt.plot(seasonal_component.get_rawdata(),  label='season') 
            plt.legend(loc='upper left')          
            plt.show() 
        ts_recent = ts_res.ts[self.data_slice_index:]
        anomaly_idx, anomaly_scores = sge.max_test_indices(ts_recent.values,
                                                           alpha=self.esd_alpha, get_score=True)
        #anomaly_idx = sorted(anomaly_idx)
        return anomaly_idx, anomaly_scores




#1−exp(log(0.5)/halflife)    


#cd = event_types_df[event_types_df.EventID=='7000'].iloc[0]        
#query_cd = idamconf.get_event_counts_query(**cd)
#query_cd
#results_cd = qe.es_get_aggregations_data(es_instance=es,
#                               index=idamconf.ES_CONFIG['INDEX'],
#                               doc_type=idamconf.ES_CONFIG['DOC_TYPE'],
#                               query=query_cd)
#
#df_cd = pd.DataFrame(results_cd['group_by_datetime']['buckets'])
#df_cd['timestamp'] = pd.to_datetime(df_cd['key_as_string'])
#df_cd = df_cd.drop(['key_as_string','key'], axis =1)
#description = results_cd['info']['hits'][0]['_source']['Description']
#
#ts_cd_df = df_cd[['timestamp', 'doc_count']]
#ts_cd = pd.Series(ts_cd_df.doc_count)
#ts_cd.index = ts_cd_df.timestamp  
#
#
#
#
#s = event_types_df[event_types_df.EventID=='109'].iloc[0]        
#query_s = idamconf.get_event_counts_query(**s)
#query_s
#results_s = qe.es_get_aggregations_data(es_instance=es,
#                               index=idamconf.ES_CONFIG['INDEX'],
#                               doc_type=idamconf.ES_CONFIG['DOC_TYPE'],
#                               query=query_s)
#
#df_s = pd.DataFrame(results_s['group_by_datetime']['buckets'])
#df_s['timestamp'] = pd.to_datetime(df_s['key_as_string'])
#df_s = df_s.drop(['key_as_string','key'], axis =1)
#description = results_s['info']['hits'][0]['_source']['Description']
#
#ts_s_df = df_s[['timestamp', 'doc_count']]
#ts_s = pd.Series(ts_s_df.doc_count)
#ts_s.index = ts_s_df.timestamp
#    
#    
#    
#ewma_halflife = 1
#esd_alpha = 0.05
#mindatalen = 10 
    
#ts_s = ts_s.diff()    
#ts_obj = TimeSeries(ts_s, frequency=24) 
#anomaly_model = SeasonalEsdEwma(ts_obj, check_recency_weeks=2)
#anomaly_df = anomaly_model.get_anomalies()   
#res_series, seasonal_component = ts_obj.remove_seasonal()    
#ts_res = res_series.ewma_diff(ewma_halflife)    
#plt.plot(ts_res.get_rawdata(),  label='residual') 
#plt.plot(seasonal_component.get_rawdata(),  label='season') 
#plt.legend(loc='upper left')    