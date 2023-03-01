# -*- coding: utf-8 -*-
"""
Created on Fri Mar 2 20:00:00 2018

@author: ad_sarkardi
"""


import pandas as pd

def order_df_by_cols(df, columns):
    df = df[columns]
    return df


def embed_df_column_url(df, url_column, embed_column=None, embed_text=None):
    
    if embed_column:
        df[embed_column] = df.apply(lambda row: 
                                '<a href="{}">{}</a>'.format(row[url_column], 
                                                             row[embed_column]), 
                                    axis=1)
    elif embed_text:
        df['Kibana'] = df.apply(lambda row: 
                                '<a href="{}">{}</a>'.format(row[url_column], 
                                                             embed_text), 
                                    axis=1)
    else:
        df['Kibana'] = df.apply(lambda row: 
                                   '<a href="{}">{}</a>'.format(row[url_column], 
                                                             row[url_column]), 
                                    axis=1)
    df = df.drop(labels=[url_column], axis=1)
    return df
