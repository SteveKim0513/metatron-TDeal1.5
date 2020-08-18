"""
T-DEAL Druid 적재 - inventory
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
targetPath = 'ls /data/s3data/inventory'
targetName = os.popen(targetPath).read().split('\n')[0]

def run(logger):

    # dataName 넣기
    logger.info('======== Start inventory =========')
    try:
        filterFile = targetName
        intervalValue = '1900-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z'

        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL

        # folderName = folder name in mounted bucket(metatron-druid-tdeal)
        sourceDataPath = 'data/s3data/inventory'
        
        ## 현재가 수정할 곳 1 : 적재 후 discovery 데이터 소스(images 폴더 참고)
        datasourceName = 'as_xjisd'

        # 현재가 수정할 곳 2 : druid Overload Console에서 log 확인, 아래 변수명은 유지
        ingestionSpec = {
          "type" : "index",
            "spec" : {
                "dataSchema" : {
                "dataSource" : datasourceName,
                "parser" : {
                "type" : "csv.stream",
                    "timestampSpec" : {
                    "column" : "date",
                    "format" : "yyyy-MM-dd HH:mm",
                    "replaceWrongColumn" : True,
                    "timeZone" : "Etc/UCT",
                    "locale" : "en"
                },
                "dimensionsSpec" : {
                  "dimensions" : [ "productOptionName", "stockType", "shopName", "displayType", "productName", "saleType", "id" ],
                  "dimensionExclusions" : [ ],
                  "spatialDimensions" : [ ]
                },
                "columns" : [ "productOptionName", "stockType", "shopName", "displayType", "productName", "saleType", "date", "quantity", "id" ],
                "delimiter" : ",",
                "recordSeparator" : "\n",
                "skipHeaderRecord" : True,
                "charset" : "UTF-8"
              },
              "metricsSpec" : [ {
                "type" : "count",
                "name" : "count"
              }, {
                "type" : "sum",
                "name" : "quantity",
                "fieldName" : "quantity",
                "inputType" : "double"
              } ],
              "enforceType" : True,
              "granularitySpec" : {
                "type" : "uniform",
                "segmentGranularity" : "MONTH",
                "queryGranularity" : "DAY",
                "rollup" : False,
                "append" : False,
                "intervals" : [intervalValue ]
              }
            },
            "ioConfig" : {
              "type" : "index",
              "firehose" : {
                "type" : "local",
                "baseDir" : "data/s3data/inventory",
                "filter" : filterFile
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
                "allowNullForNumbers" : False
              },
              "buildV9Directly" : True,
              "ignoreInvalidRows" : False,
              "maxRowsInMemory" : 75000,
              "maxOccupationInMemory" : -1
              }
            },
          "dataSource" : datasourceName,
          "interval" : intervalValue
        }

        URL = deleteUrl + '/druid/coordinator/v1/datasources/' + datasourceName
        headers = {'charset' : 'utf-8'}
        response = requests.delete(URL, headers = headers)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('====== DELETE inventory =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - inventory =====')
    except:
        logger.exception("Got exception on sample")
