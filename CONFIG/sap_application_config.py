# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 02:36:01 2018

@author: ad_tghosh
"""

from datetime import datetime, timedelta


_CA_CERT_FILE = 'CONFIG/IntelSHA256RootCA-Base64.crt'



ES_CONFIG = {
    'INDEX': 'sap_print-*',
    'DOC_TYPE': None,
    'ca_certs': _CA_CERT_FILE, 
    'SERVICE_NAME': 'ITI_ANOMALY',
    'USER_NAME': 'SAP_APP_DEV_AUTH',
    'HOST' : 'https://elkdeves1.intel.com:3602',
    'timeout': 240
}

TS_PATTERN = '%Y-%m-%dT%H:%M:%S'

TS_PATTERN_EMAIL = '%Y-%m-%d %H:%M:%S'

TS_PATTERN_DAY = '%Y-%m-%d'
dt = datetime.now()

end_dt = dt
start_dt = end_dt- timedelta(minutes=15)

START_TS = start_dt.strftime(TS_PATTERN)
END_TS = end_dt.strftime(TS_PATTERN)

START_DAY = start_dt.strftime(TS_PATTERN_DAY)
END_DAY = end_dt.strftime(TS_PATTERN_DAY)

TS_TYPE = 'count'
TS_FIELD = "@timestamp"
RUN_MODEL_RECENCY_WEEKS = 2
SRC_FIELDS =  ['msg']

EMAIL_TO = ['tamoghna.ghosh@intel.com'
            ,'parameshwar.s.thippaiah@intel.com'
            ,'thomas.jenn.woei.tan@intel.com'
            ]

EMAIL_FROM = 'iti_analytics@intel.com'

END_TS_PST = end_dt.strftime(TS_PATTERN_EMAIL)

EMAIL_SUBJECT = 'SAP Print Server Errors'
EMAIL_TEMPLATE = (
        """<html>
                <h2>SAP Print Application Error Alert</h2><br/>
                
                {anomaly_table}<br/></html>
           """)

EMAIL_TEMPLATE_NODATA = (
        """<html>
                <h2>No Logs Found SAP Print</h2><br/>
                
                {anomaly_table}<br/></html>
           """)


PRINT_ERR_QUERY = {       
        
    
        "_source" : ["msg","@timestamp"],
        "query": {
			  
          "bool":{
                    "must": [
                                { 
                                  "range": {
                                        "@timestamp": {
                                            "gte": START_TS,
                                            "lte": END_TS
                                        }
                                  }
                                },
                                {"query_string" : {
                          				  "default_field" : "msg",
                          				  "query" : "*error*",
                          				  "analyze_wildcard": 'true'
                          			    }
                                  
                                }
                                
							]
					} 
        }   
}


DATA_LOADS = {
   
 "_source" : SRC_FIELDS,
        "size":0,
        
        "query": {
            "bool":{
                    "must": [
                                { 
                                  "range": {
                                        "@timestamp": {
                                            "gte": START_TS,#"now-1h",
                                            "lte": END_TS,#"now"
                                        }
                                  }
                                }
              
                    ]
                    
            } 
        },
        "aggs" : {
                "doc_count" : {
                            "value_count" : {
                                    "field" : TS_FIELD
                                    }
                }
        }     
}

