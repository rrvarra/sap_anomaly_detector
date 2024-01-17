# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 14:16:52 2018

@author: tghosh
"""

from datetime import datetime, timedelta
import platform

_CA_CERT_FILE = 'CONFIG/IntelSHA256RootCA-Base64.crt'



ES_CONFIG = {
    'INDEX': 'sap_evtlog-*',
    'DOC_TYPE': 'sap_evtlog',
    'ca_certs': _CA_CERT_FILE,
    'SERVICE_NAME': 'ITI_ANOMALY',
    'USER_NAME': 'SAP_EVT_ANOMALY_AUTH',
    'HOST' : 'https://sapps.eck1es.intel.com:9443',
    'timeout': 240
}


TS_PATTERN = '%Y-%m-%dT23:59:59'
TS_PATTERN_DAY = '%Y-%m-%d'
dt = datetime.now()
#end_dt = dt - timedelta(hours=24*6) # 20-3-2018
#start_dt = end_dt- timedelta(hours=24*66)
end_dt = dt - timedelta(hours=24)
start_dt = end_dt- timedelta(hours=24*60)

START_TS = start_dt.strftime(TS_PATTERN)
END_TS = end_dt.strftime(TS_PATTERN)

START_DAY = start_dt.strftime(TS_PATTERN_DAY)
END_DAY = end_dt.strftime(TS_PATTERN_DAY)

TS_INTERVAL = '24h'
TS_TYPE = 'count'
TS_FIELD = "Ts"
RUN_MODEL_RECENCY_WEEKS = 2
SRC_FIELDS =  ['Source','LogName','EventID','HostInfo.PipeName']
OTHER_FIELDS = ['Description']


EMAIL_TO = [
        'sap.l3.us@intel.com',
        'SAPL3IIDC@intel.com',
        'ram.r.varra@intel.com',
        'walter.burke@intel.com',
        'john.r.barbour@intel.com',
        'abhishek.shukla@intel.com',
        'gowri.shankar.gavara@intel.com',
        'karl.e.mailman@intel.com',
        'gary.gilardi@intel.com',
        'sap.l3.basis@intel.com',
        'balamurugan.sivasubramanian@intel.com',
        'vipin.muthukattil@intel.com'
]
#EMAIL_TO = ['ram.r.varra@intel.com']
EMAIL_FROM = 'anomaly_detector@intel.com'

EMAIL_SUBJECT = 'Top {anomaly_count} SAP Event Anomalies for today from %s ' % (platform.node(), )
EMAIL_TEMPLATE = (
        """<html>
                <h2>SAP Events Anomaly Detection Alert</h2><br/>
                Following are the anomalies for: <b>{time_period}</b></br>
                {anomaly_table}<br/></html>
           """)

GROUP_LIMIT = 500

EVENT_GROUPS_QUERY = {

        "_source" : SRC_FIELDS,
        "size":0,

        "query": {
            "bool":{
                    "must": [
                                {
                                  "range": {
                                        "Ts": {
                                            "gte": START_TS,
                                            "lte": END_TS
                                        }
                                  }
                                },
                                {
                                  "terms": {
                                          "Level": ["Error"]
                                  }
                                },
                                {
                                  "terms": {
                                          "HostInfo.Use": ["Prod"]
                                  }
                                }

                    ]

            }
        },

        "aggs":{
                "by_Source": {
                        "terms": {
                                "field": "Source","size": GROUP_LIMIT
                        },
                        "aggs":{
                                "by_LogName": {
                                        "terms": {
                                                "field": "LogName","size": GROUP_LIMIT
                                        },
                                        "aggs":{
                                                "by_EventID": {
                                                        "terms": {
                                                                "field": "EventID","size": GROUP_LIMIT
                                                        },
                                                        "aggs":{
                                                                "by_HostInfo.PipeName": {
                                                                        "terms": {
                                                                                "field": "HostInfo.PipeName","size": GROUP_LIMIT
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
        }

}



def get_event_counts_query(**kwargs):

    return{
          "_source" : OTHER_FIELDS,
          "size" : 1,
          "query": {
              "bool":{
                "must": [
                  { "range": {
                        "Ts": {
                            "gte": START_TS,
                            "lte": END_TS
                        }
                    }
                  },
                  { "match": {
                         "Level": "Error"
                     }
                  },
                  {
                    "match": {
                        "HostInfo.Use": "Prod"
                        }
                   },
                    { "match": {
                         "EventID": kwargs["EventID"]
                     }
                  },
                    { "match": {
                         "LogName": kwargs["LogName"]
                     }
                  },
                    { "match": {
                         "Source": kwargs["Source"]
                     }
                  },
                    { "match": {
                         "HostInfo.PipeName": kwargs["HostInfo.PipeName"]
                     }
                  }
                ]

              }
            },

             "aggs":{
              "group_by_datetime":{
                  "date_histogram":{
                                  "min_doc_count" : 0,
                                  "field":TS_FIELD,
                                  "fixed_interval":TS_INTERVAL,
                                  "extended_bounds" : {
                                        "min" : START_TS,
                                        "max" : END_TS
                                 }
                    }

              }
            }
        }


MIN_THRESHOLDS = [
        {"LogName":"APPLICATION", "EventID":"35712","Threshold": float("inf")},
        {"LogName":"APPLICATION", "EventID":"14197","Threshold": float('inf')},
        {"LogName":"APPLICATION", "EventID":"16421","HostInfo.PipeName":"SLT",
         "Threshold": float('inf')},
        {"LogName":"APPLICATION", "EventID":"16419","HostInfo.PipeName":"SLT",
         "Threshold": float('inf')}
]

old_url = "https://elkprdkibana1.intel.com:6625/app/kibana#/dashboard/"

KIBANA_BASE_URL=(
        "https://elkprdkibana1.intel.com:6601/s/sap/app/kibana#dashboard/"
        "d57f43d0-cbeb-11e7-a144-7d8b9d99da37?_g=(refreshInterval:(display:Off,pause:!f,value:0),"
        "time:(from:now-7d,mode:quick,to:now))&_a=("
        "description:'',"
        "filters:!("
        "('$state':(store:appState),"
        	"meta:(alias:!n,apply:!t,disabled:!f,index:'sap_evtlog-*',"
        	"key:Level,negate:!f,type:phrase,value:Error),"
        	"query:(match:(Level:(query:Error,type:phrase)))),"
        "('$state':(store:appState),"
        	"meta:(alias:!n,disabled:!f,index:'sap_evtlog-*',"
        	"key:HostInfo.Use,negate:!f,type:phrase,value:Prod),"
        	"query:(match:(HostInfo.Use:(query:Prod,type:phrase)))),"
        "('$state':(store:appState),"
        	"meta:(alias:!n,disabled:!f,index:'sap_evtlog-*',"
        	"key:EventID,negate:!f,type:phrase,value:'{event_id}'),"
        	"query:(match:(EventID:(query:'{event_id}',type:phrase)))),"
        "('$state':(store:appState),"
        	"meta:(alias:!n,disabled:!f,index:'sap_evtlog-*',"
        	"key:LogName,negate:!f,type:phrase,value:'{log_name}'),"
        	"query:(match:(LogName:(query:'{log_name}',type:phrase)))),"
        "('$state':(store:appState),"
        	"meta:(alias:!n,disabled:!f,index:'sap_evtlog-*',"
        	"key:Source,negate:!f,type:phrase,value:'{source}'),"
        	"query:(match:(Source:(query:'{source}',type:phrase)))),"
#        "('$state':(store:appState),"
#        	"meta:(alias:!n,disabled:!f,index:'sap_evtlog-*',"
#        	"key:HostInfo.Role,negate:!f,type:phrase,value:'{role}'),"
#        	"query:(match:(HostInfo.Role:(query:'{role}',type:phrase)))),"
        "('$state':(store:appState),"
        "meta:(alias:!n,disabled:!f,index:'sap_evtlog-*',"
        "key:HostInfo.PipeName,negate:!f,type:phrase,value:'{pipe_name}'),"
        "query:(match:(HostInfo.PipeName:(query:'{pipe_name}',type:phrase))))"
        "))"
)