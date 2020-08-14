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

def run(logger):

    # dataName 넣기
    logger.info('======== Start inventory =========')
    try:
        # folderName = folder name in mounted bucket(metatron-druid-tdeal)
        sourceDataPath = 'data/s3data/inventory'
        
        ## 현재가 수정할 곳 1 : 적재 csv 이름, 적재 후 discovery 데이터 소스(images 폴더 참고)
        filterFile = '${fildName}.csv'
        datasourceName = '${dataSourceName}'
        intervalValue = '1900-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z'

        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL

        # 현재가 수정할 곳 2 : druid Overload Console에서 log 확인, 아래 변수명은 유지
        ingestionSpec = {
            "type": "index",
            "spec": {
                "dataSchema": {
                    "dataSource": datasourceName,
                    "parser": {
                        ...
                    "enforceType": True,
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "MONTH",
                        "queryGranularity": "HOUR",
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
                ...
            },
            "dataSource": datasourceName,
            "interval": intervalValue
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
