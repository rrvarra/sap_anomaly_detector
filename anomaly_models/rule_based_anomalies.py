# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 14:56:11 2018

@author: tghosh
"""
import pandas as pd


class AnomalyRule:
    def __init__(self,name, fieldname, threshold=0, bound_type='upper'):
        self.name = name
        self.threshold = threshold
        self.fieldname = fieldname
        self.bound_type = bound_type

    
    def get_anomalies(self, data): 
        """data - dataframe/count aggregated json
        Returns anomaly violating thresholds"""
        if data is None:
            return pd.DataFrame()
        if isinstance(data, pd.DataFrame):
            if self.bound_type is 'upper':
                df = data[data[self.fieldname]>self.threshold]
            else:
                df = data[data[self.fieldname]<self.threshold]
            df.insert(0,column= 'name', value= [self.name]*df.shape[0])
            return df
        else:
            #json response
            value = data[self.fieldname]['value']
            if self.bound_type is 'upper' and value > self.threshold or \
                   self.bound_type is 'lower' and value < self.threshold:                
                return pd.DataFrame({'name':[self.name], 'counts':[data[self.fieldname]['value']]})
            else:
               return pd.DataFrame() 
    