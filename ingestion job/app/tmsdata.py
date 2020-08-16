"""
T-DEAL Druid 적재 - tmsdata
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
targetPath = 'ls /data/s3data/tmsdata'
targetName = os.popen(targetPath).read().split('\n')[0]
print(targetName)

def run(logger):

    # dataName 넣기
    logger.info('======== Start tmsdata =========')
    try:
        filterFile = targetName
        intervalValue = '1900-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z'

        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL

        # folderName = folder name in mounted bucket(metatron-druid-tdeal)
        sourceDataPath = 'data/s3data/tmsdata'
        
        ## 현재가 수정할 곳 1 : 적재 csv 이름, 적재 후 discovery 데이터 소스(images 폴더 참고)
        datasourceName = 'tms_data'

        # 현재가 수정할 곳 2 : druid Overload Console에서 log 확인 
        ingestionSpec = {
            "type": "index",
            "spec": {
                "dataSchema": {
                    "dataSource": datasourceName,
                    "parser": {
                        "type": "csv.stream",
                        "timestampSpec": {
                            "column": "발송일자",
                            "format": "yy-MM-dd",
                            "replaceWrongColumn": True,
                            "timeZone": "UTC",
                            "locale": "en"
                        },
                        "dimensionsSpec": {
                            "dimensions": ["shop", "발송형태", "고유번호", "발송시간", "campaign_id", "base_date"],
                            "dimensionExclusions": [],
                            "spatialDimensions": []
                        },
                        "columns": ["shop", "발송일자", "발송량", "발송형태", "발송단가", "광고비용", "발송결과실패", "발송결과성공", "수신결과완료", "고유번호",
                                    "발송시간", "campaign_id", "base_date"],
                        "delimiter": ",",
                        "recordSeparator": "\n",
                        "skipHeaderRecord": True,
                        "charset": "UTF-8"
                    },
                    "metricsSpec": [{
                        "type": "count",
                        "name": "count"
                    }, {
                        "type": "sum",
                        "name": "발송량",
                        "fieldName": "발송량",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "발송단가",
                        "fieldName": "발송단가",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "광고비용",
                        "fieldName": "광고비용",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "발송결과실패",
                        "fieldName": "발송결과실패",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "발송결과성공",
                        "fieldName": "발송결과성공",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "수신결과완료",
                        "fieldName": "수신결과완료",
                        "inputType": "double"
                    }],
                    "enforceType": True,
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "MONTH",
                        "queryGranularity": "DAY",
                        "rollup": False,
                        "append": False,
                        "intervals": [intervalValue]
                    }
                },
                "ioConfig": {
                    "type": "index",
                    "firehose": {
                        "type": "local",
                        "baseDir": sourceDataPath,
                        "filter": filterFile
                    }
                },
                "tuningConfig": {
                    "type": "index",
                    "targetPartitionSize": 5000000,
                    "indexSpec": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionSketches": {
                            "type": "none"
                        },
                        "allowNullForNumbers": False
                    },
                    "buildV9Directly": True,
                    "ignoreInvalidRows": False,
                    "maxRowsInMemory": 75000,
                    "maxOccupationInMemory": -1
                }
            },
            "dataSource": datasourceName,
            "interval": intervalValue
        }

        URL = deleteUrl + '/druid/coordinator/v1/datasources/' + datasourceName
        headers = {'charset' : 'utf-8'}
        response = requests.delete(URL, headers = headers)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('====== DELETE tmsdata =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - tmsdata =====')
    except:
        logger.exception("Got exception on sample")
