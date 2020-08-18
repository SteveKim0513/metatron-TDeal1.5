"""
T-DEAL Druid 적재 - salesdata
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
targetPath = 'ls /data/s3data/salesdata'
targetName = os.popen(targetPath).read().split('\n')[0]
print(targetName)

def run(logger):

    # dataName 넣기
    logger.info('======== Start salesdata =========')
    try:
        filterFile = targetName
        intervalValue = '1900-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z'

        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL

        # folderName = folder name in mounted bucket(metatron-druid-tdeal)
        sourceDataPath = '/data/s3data/salesdata'

        ## 현재가 수정할 곳 1 : 적재 후 discovery 데이터 소스(images 폴더 참고)
        datasourceName = 'sales_data'

        # 현재가 수정할 곳 2 : druid Overload Console에서 log 확인
        ingestionSpec = {
            "type": "index",
            "spec": {
                "dataSchema": {
                    "dataSource": datasourceName,
                    "parser": {
                        "type": "csv.stream",
                        "timestampSpec": {
                            "column": "주문일시",
                            "format": "yyyy-MM-dd HH:mm:ss",
                            "replaceWrongColumn": True,
                            "timeZone": "Etc/UCT",
                            "locale": "en"
                        },
                        "dimensionsSpec": {
                            "dimensions": ["base_date", "시간", "상품주문번호", "주문상태", "배송상태", "SHOP", "상품번호", "상품명", "옵션", "주문번호", "주문순번", "배송번호", "수취인", "연락처", "배송지", "우편번호", "택배사", "택배코드", "송장번호","발송처리일", "구매자ID", "구매자명", "구매자 연락처", "취소일시", "결제타입", "결제방법", "구매확정일시","unique_num", "customer_code", "customer_name", "brand_code", "brand_name","decrypt_procode"],
                            "dimensionExclusions": [],
                            "spatialDimensions": []
                        },
                        "columns": ["base_date", "시간", "상품주문번호", "주문상태", "배송상태", "주문일시", "SHOP", "상품번호", "상품명", "옵션", "수량", "판매가", "할인가", "공급가", "옵션가", "옵션 공급가", "주문금액", "결제금액", "취소금액", "기본배송비", "지역별배송비", "주문번호", "주문순번", "배송번호", "수취>인", "연락처", "배송지", "우편번호", "택배사", "택배코드", "송장번호", "발송처리일", "구매자ID", "구매자명", "구매자 연락처", "취소일시", "결제타입", "결제방법", "구매확정일시", "unique_num", "customer_code", "customer_name", "brand_code", "brand_name", "decrypt_procode"],
                        "delimiter": ",",
                        "recordSeparator": "\n",
                        "skipHeaderRecord": True,
                        "charset": "UTF-8"
                    },
                    "metricsSpec": [{
                        "type": "relay",
                        "name": "수량",
                        "fieldName": "수량",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                       "name": "판매가",
                        "fieldName": "판매가",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                        "name": "할인가",
                        "fieldName": "할인가",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                        "name": "공급가",
                        "fieldName": "공급가",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                        "name": "옵션가",
                        "fieldName": "옵션가",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                        "name": "옵션 공급가",
                        "fieldName": "옵션 공급가",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                        "name": "주문금액",
                        "fieldName": "주문금액",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                        "name": "결제금액",
                        "fieldName": "결제금액",
                        "inputType": "long"
                   }, {
                        "type": "relay",
                        "name": "취소금액",
                        "fieldName": "취소금액",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                        "name": "기본배송비",
                        "fieldName": "기본배송비",
                        "inputType": "long"
                    }, {
                        "type": "relay",
                        "name": "지역별배송비",
                        "fieldName": "지역별배송비",
                        "inputType": "long"
                    }],
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
                        "baseDir": "data/s3data/salesdata",
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
        logger.info('====== DELETE salesdata =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - salesdata =====')
    except:
        logger.exception("Got exception on sample")
