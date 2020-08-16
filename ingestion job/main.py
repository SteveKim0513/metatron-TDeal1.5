"""
T-DEAL AIRFLOW 배치
"""
from datetime import datetime
import argparse
from util.util import CreateLogger
from app import inventory, salesdata, landingtype, campaigndata, tmsdata, simulation
## app folder 내 파일명이 import되는 것과 일치해야함

def main(args, logger):
    logger.info('======== T-DEAL =========')
    for target in args.target:
        logger.info('====== CSV File : ' + target)
        if target == 'INVENTORY':
            try:
                inventory.run(logger)
            except:
                logger.exception("Got exception on main - INVENTORY")
        elif target == 'SALESDATA':
            try:
                salesdata.run(logger)
            except:
                logger.exception("Got exception on main - SALESDATA")
        elif target == 'LANDINGTYPE':
            try:
                landingtype.run(logger)
            except:
                logger.exception("Got exception on main - LANDINGTYPE")
        elif target == 'CAMPAIGNDATA':
            try:
                campaigndata.run(logger)
            except:
                logger.exception("Got exception on main - CAMPAIGNDATA")
        elif target == 'TMSDATA':
            try:
                tmsdata.run(logger)
            except:
                logger.exception("Got exception on main - TMSDATA")
        elif target == 'SIMULATION':
              try:
                simulation.run(logger)
            except:
                logger.exception("Got exception on main - SIMULATION")

        ## add condition
        '''
        elif target == '${dataName}':
            try:
                tmsdata3.run(logger)
            except:
                logger.exception("Got exception on main - ${dataName}")
        '''



if __name__ == '__main__':
    start_time = datetime.now().strftime('%Y%m%d_%H%M')
    logger = CreateLogger('batch', "logs/{socket.gethostname()}_log.log")
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', nargs='+', help='batch')
    args = parser.parse_args()
    try:
        main(args, logger)
    except:
        logger.exception("Got exception on main")
        raise