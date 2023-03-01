# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 02:36:43 2018

@author: ad_tghosh/ad_sarkardi
"""
import pandas as pd
from seasonal import fit_seasons, adjust_seasons
import anomaly_models.smirnov_grubbs_esd as sge
import numpy as np

#the output of data layer will be an instance of following class

class TimeSeries:
    
    """A time series dataframe/Series with time as index.
    Assumes default weekly seasonal period and daily observation frequency"""
    
    def __init__(self, tsdata,frequency = 24 ,seasonal_period_days=7, name=None):
        
        #if multivariate expect a data = dataframe with time axis as index
        if isinstance(tsdata, pd.DataFrame) or isinstance(tsdata, pd.Series):
            if not tsdata.index.__class__.__name__ == 'DatetimeIndex':
                raise ValueError('datetime series index required')
            else:
                self.ts = tsdata
        else:
            raise ValueError('Expected a dataframe')
       
        self.ts = tsdata
        self.concept_drift_present = False
        self.mindatalen = 10 #min number of points for ewma removal
        self.seasonal_decomp_freq_map = {}
        if isinstance(tsdata, pd.DataFrame) and self.ts.shape[1] > 1:
            self.multivariate = True
        elif isinstance(tsdata, pd.Series):
            self.multivariate = False
            
        if frequency is None:
            self.frequency = self._get_frequency(tsdata.index)
        else:
            self.frequency = frequency
        
        
        self._missing_value_check(handle_na='linear')
        # TODO: this is wrong, freq should be 24x60
        self.seasonal_decomp_freq = ((24*60)//(self.frequency*60)) * seasonal_period_days
        
    def _missing_value_check(self, handle_na = 'zero'):
        #check missing values/na in time series and fix them
        #interpolate : ‘linear’, ‘time’, ‘index’, ‘values’, ‘nearest’, ‘zero’ (pandas.DataFrame.interpolate)
        return 
    
    def _get_frequency(self, index):
        #calculates time series frequency from time index
        return '1d'
    
    
    def apply_concept_drift_test(self, recency_weeks=2):
        frequency = self.frequency
        data_slice_index = -(recency_weeks*24*7 // frequency)
        ts = self.ts
        ts = ts[data_slice_index:]
        
        recent_total_frequency = sum(ts.values[-3:])
        if recent_total_frequency < 15:
            self.concept_drift_present = False
        else:        
            halflife = 1
            if len(ts.values) >= self.mindatalen:
                ewma =  ts.ewm(halflife=halflife).mean()
                ts_res = ts.subtract(ewma)
            else:
                ts_res = ts  
                
            diff_series = np.diff(ts_res)
            max_idx = len(ts_res) - 1
            alpha = 0.05
            anomaly_idxs = sge.max_test_indices(diff_series,
                                               alpha=alpha, 
                                               get_score=False)
            if anomaly_idxs:
                # diff series has one less value always
                max_anomaly_idx = np.max(anomaly_idxs) + 1 
                recent_anomaly_diff = max_idx - max_anomaly_idx 
                recent_anomaly_diff = recent_anomaly_diff * (self.frequency/24)
                
                if 1 <= recent_anomaly_diff <= 5:
                    self.concept_drift_present = True
                else:
                    self.concept_drift_present = False
                    
            else:
                self.concept_drift_present = False
        
        
    
    def remove_seasonal(self):
        """removes season component and returns de-seasoned series, seasonal component
           
        """
        if self.multivariate:
            raise ValueError("Cannot remove seasonality from multivariate series")
        
        if len(self.ts) >= self.seasonal_decomp_freq*2: #should have atleast 2 seasons data
            try:
                seasons, trend = fit_seasons(self.ts.values, period=self.seasonal_decomp_freq)
                if seasons is not None:                
                    adjusted = adjust_seasons(self.ts.values, seasons=seasons)
                    res_comp = adjusted - trend      
                    seasonal_component = self.ts.values - adjusted - trend
                    seasonal_series = pd.Series(seasonal_component, index=self.ts.index)
                    res_series = pd.Series(res_comp, index=self.ts.index)     
                    
                else:                    
                    seasonal_series = pd.Series([0]*len(self.ts.index), index=self.ts.index)
                    res_series = self.ts
                    
            except:
                seasonal_series = pd.Series([0]*len(self.ts.index), index=self.ts.index)
                res_series = self.ts
        else:
            seasonal_series = pd.Series([0]*len(self.ts.index), index=self.ts.index)
            res_series = self.ts
        return TimeSeries(res_series) , TimeSeries(seasonal_series)
    
    def ewma_diff(self, halflife):
        if len(self.ts.values) >= self.mindatalen:
            ewma =  self.ts.ewm(halflife=halflife).mean()
            ts_res = self.ts.subtract(ewma)
        else:
            ts_res = self.ts    
        
        return TimeSeries(ts_res)
    
    
    def subsample(self,indices):
        #should support both time index and row number integer index.
        # TODO: for now only rows are returned based on row number, need to change
        sample_df = self.ts.iloc[indices]            
        return sample_df
    
    
    def get_rawdata(self):
        return self.ts.values
    
    def __str__(self):
        return str(self.ts.head(2))