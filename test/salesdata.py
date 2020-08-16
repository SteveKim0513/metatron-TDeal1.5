"""
T-DEAL Druid 적재 - testdata
"""
import socket
from datetime import datetime
import argparse
from util.util import CreateLogger
from config.config import TargetConfig
import os
import boto3
import requests, json

# read filename
targetPath = 'ls /data/s3data/testdata'
targetName = os.popen(targetPath).read().split('\n')[0]

def run(logger):

    # dataName 넣기
    logger.info('======== Start testdata =========')
    try:
        # folderName = folder name in mounted bucket(metatron-druid-tdeal)
        sourceDataPath = '/data/druid-ingestion/test/'
        
        ## 현재가 수정할 곳 1 : 적재 csv 이름, 적재 후 discovery 데이터 소스(images 폴더 참고)
        filterFile = 'targetName'
        datasourceName = 'test_jh'
        intervalValue = '1900-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z'

        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL

        # 현재가 수정할 곳 2 : druid Overload Console에서 log 확인
        ingestionSpec = {

            "type" : "index",
            "id" : "index_test_jh_2020-08-16T07:54:43.052Z",
            "resource" : {  
                "availabilityGroup" : "index_test_jh_2020-08-16T07:54:43.052Z",
                "requiredCapacity" : 1
            },
            "spec" : {
                "dataSchema" : {
                "dataSource" : "datasourceName",
                "parser" : {
                    "type" : "csv.stream",
                    "timestampSpec" : {
                    "column" : "current_datetime",
                    "missingValue" : "2020-08-16T07:54:43.048Z",
                    "invalidValue" : "2020-08-16T07:54:43.048Z",
                    "replaceWrongColumn" : true
                    },
                    "dimensionsSpec" : {
                    "dimensions" : [ "癤퓋toreName", "productName" ],
                    "dimensionExclusions" : [ ],
                    "spatialDimensions" : [ ]
                    },
                    "columns" : [ "癤퓋toreName", "productName", "price", "amount", "margin" ],
                    "delimiter" : ",",
                    "recordSeparator" : "\n",
                    "skipHeaderRecord" : true,
                    "charset" : "UTF-8"
                },
                "metricsSpec" : [ {
                    "type" : "count",
                    "name" : "count"
                }, {
                    "type" : "sum",
                    "name" : "price",
                    "fieldName" : "price",
                    "inputType" : "double"
                }, {
                    "type" : "sum",
                    "name" : "amount",
                    "fieldName" : "amount",
                    "inputType" : "double"
                }, {
                    "type" : "sum",
                    "name" : "margin",
                    "fieldName" : "margin",
                    "inputType" : "double"
                } ],
                "enforceType" : true,
                "granularitySpec" : {
                    "type" : "uniform",
                    "segmentGranularity" : "DAY",
                    "queryGranularity" : "DAY",
                    "rollup" : false,
                    "append" : false,
                    "intervals" : [intervalValue]
                },
                "dimensionFixed" : false
                },
                "ioConfig" : {
                "type" : "index",
                "firehose" : {
                    "type" : "local",
                    "baseDir" : sourceDataPath,
                    "filter" : filterFile,
                    "extractPartition" : false
                }
                },
                "tuningConfig" : {
                "type" : "index",
                "targetPartitionSize" : 5000000,
                "indexSpec" : {
                    "bitmap" : {
                    "type" : "roaring"
                    },
                    "dimensionSketches" : {
                    "type" : "none"
                    },
                    "allowNullForNumbers" : false
                },
                "buildV9Directly" : true,
                "ignoreInvalidRows" : true,
                "maxRowsInMemory" : 75000,
                "maxOccupationInMemory" : -1
                }
            },
            "context" : {
                "druid.task.runner.dedicated.host" : "engine.tdeal:8091"
            },
            "groupId" : "index_test_jh_2020-08-16T07:54:43.052Z",
            "dataSource" : datasourceName,
            "interval" : intervalValue
        }


        URL = deleteUrl + '/druid/coordinator/v1/datasources/' + datasourceName
        headers = {'charset' : 'utf-8'}
        response = requests.delete(URL, headers = headers)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('====== DELETE testdata =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - testdata =====')
    except:
        logger.exception("Got exception on sample")
