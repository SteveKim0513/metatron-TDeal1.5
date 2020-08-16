"""
T-DEAL AIRFLOW 배치
"""
from datetime import datetime
import argparse
from util.util import CreateLogger
from app import test

def main(args, logger):
    logger.info('======== T-DEAL =========')
    for target in args.target:
        logger.info('====== CSV File : ' + target)
        if target == 'TEST':
            try:
                test.run(logger)
            except:
                logger.exception("Got exception on main - TEST")



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