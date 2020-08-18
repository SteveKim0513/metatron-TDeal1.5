"""
T-DEAL Druid 적재 - simulation
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
targetPath = 'ls /data/s3data/simulation'
targetName = os.popen(targetPath).read().split('\n')[0]
print(targetName)

def run(logger):

    # dataName 넣기
    logger.info('======== Start simulation =========')
    try:
        filterFile = targetName
        intervalValue = '1900-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z'

        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL

        # folderName = folder name in mounted bucket(metatron-druid-tdeal)
        sourceDataPath = 'data/s3data/simulation'

        ## 현재가 수정할 곳 1 : 적재 후 discovery 데이터 소스(images 폴더 참고)
        datasourceName = 'simulation'

        # 현재가 수정할 곳 2 : druid Overload Console에서 log 확인
        ingestionSpec = {
        "type" : "index",
          "spec" : {
            "dataSchema" : {
              "dataSource" : datasourceName,
              "parser" : {
                "type" : "csv.stream",
                "timestampSpec" : {
                  "column" : "base_date",
                  "format" : "yyyy-MM-dd",
                  "replaceWrongColumn" : True,
                  "timeZone" : "UTC",
                  "locale" : "en"
                },
                "dimensionsSpec" : {
                  "dimensions" : [ "고유번호", "customer_name", "brand_name" ],
                  "dimensionExclusions" : [ ],
                  "spatialDimensions" : [ ]
                },
                "columns" : [ "base_date", "고유번호", "발송수", "구매건수", "구매수량", "구매금액", "CVR", "평균판매단가", "기준단가", "발송당기대단가", "발송당거래액", "CTR", "클릭수", "customer_name", "brand_name" ],
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
                "name" : "발송수",
                "fieldName" : "발송수",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "구매건수",
                "fieldName" : "구매건수",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "구매수량",
                "fieldName" : "구매수량",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "구매금액",
                "fieldName" : "구매금액",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "CVR",
                "fieldName" : "CVR",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "평균판매단가",
                "fieldName" : "평균판매단가",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "기준단가",
                "fieldName" : "기준단가",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "발송당기대단가",
                "fieldName" : "발송당기대단가",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "발송당거래액",
                "fieldName" : "발송당거래액",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "CTR",
                "fieldName" : "CTR",
                "inputType" : "double"
              }, {
                "type" : "sum",
                "name" : "클릭수",
                "fieldName" : "클릭수",
                "inputType" : "double"
              } ],
              "enforceType" : True,
              "granularitySpec" : {
                "type" : "uniform",
                "segmentGranularity" : "MONTH",
                "queryGranularity" : "DAY",
                "rollup" : False,
                "append" : False,
                "intervals" : [intervalValue]
              }
            },
            "ioConfig" : {
              "type" : "index",
              "firehose" : {
               "type" : "local",
                "baseDir" : "data/s3data/simulation",
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
        logger.info('====== DELETE simulation =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - simulation =====')
    except:
        logger.exception("Got exception on sample")
