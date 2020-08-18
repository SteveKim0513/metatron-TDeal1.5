"""
T-DEAL Druid 적재 - campaign-data
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
targetPath = 'ls /data/s3data/campaign-data'
targetName = os.popen(targetPath).read().split('\n')[0]

def run(logger):

    # dataName 넣기
    logger.info('======== Start campaign-data =========')
    try:
        filterFile = targetName
        intervalValue = '1900-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z'

        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL

        # folderName = folder name in mounted bucket(metatron-druid-tdeal)
        sourceDataPath = 'data/s3data/campaign-data'
        
        ## 현재가 수정할 곳 1 : 적재 후 discovery 데이터 소스(images 폴더 참고)
        datasourceName = 'campaign_data'

        # 현재가 수정할 곳 2 : druid Overload Console에서 log 확인
        ingestionSpec = {
            "type": "index",
            "spec": {
                "dataSchema": {
                    "dataSource": datasourceName,
                    "parser": {
                        "type": "csv.stream",
                        "timestampSpec": {
                            "column": "dt",
                            "format": "yyyyMMdd",
                            "replaceWrongColumn": True,
                            "timeZone": "UTC",
                            "locale": "en"
                        },
                        "dimensionsSpec": {
                            "dimensions": ["prod_code", "media_id", "campaign_id", "media_product", "seg_id", "unique_num", "customer_code",
                                           "customer_name", "brand_code", "brand_name", "decrypt_procode", "base_date"],
                            "dimensionExclusions": [],
                            "spatialDimensions": []
                        },
                        "columns": ["dt", "prod_code", "media_id", "campaign_id", "media_product", "seg_id", "landing_count",
                                    "unique_user_landing_count", "order_count", "unique_user_order_count",
                                    "payment_count", "unique_user_payment_count", "purchase_count",
                                    "unique_user_purchase_count", "cancel_count", "unique_user_cancel_count",
                                    "payment_amount", "cancel_amount", "total_amount", "payment_quantity",
                                    "purchase_quantity", "cancel_quantity", "refund_count", "refund_quantity",
                                    "unique_user_refund_count", "refund_amount", "unique_num", "customer_code",
                                    "customer_name", "brand_code", "brand_name", "decrypt_procode", "base_date"],
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
                        "name": "landing_count",
                        "fieldName": "landing_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "unique_user_landing_count",
                        "fieldName": "unique_user_landing_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "order_count",
                        "fieldName": "order_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "unique_user_order_count",
                        "fieldName": "unique_user_order_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "payment_count",
                        "fieldName": "payment_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "unique_user_payment_count",
                        "fieldName": "unique_user_payment_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "purchase_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "unique_user_purchase_count",
                        "fieldName": "unique_user_purchase_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "cancel_count",
                        "fieldName": "cancel_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "unique_user_cancel_count",
                        "fieldName": "unique_user_cancel_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "payment_amount",
                        "fieldName": "payment_amount",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "cancel_amount",
                        "fieldName": "cancel_amount",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "total_amount",
                        "fieldName": "total_amount",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "payment_quantity",
                        "fieldName": "payment_quantity",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "purchase_quantity",
                        "fieldName": "purchase_quantity",
                        "inputType": "double"
                    }, {
                    }, {
                        "type": "sum",
                        "name": "cancel_quantity",
                        "fieldName": "cancel_quantity",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "refund_count",
                        "fieldName": "refund_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "refund_quantity",
                        "fieldName": "refund_quantity",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "unique_user_refund_count",
                        "fieldName": "unique_user_refund_count",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "refund_amount",
                        "fieldName": "refund_amount",
                        "inputType": "double"
                    }],
                    "enforceType": True,
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "YEAR",
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
                        "baseDir": "data/s3data/campaign-data",
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
        logger.info('====== DELETE campaign-data =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - campaign-data =====')
    except:
        logger.exception("Got exception on sample")
