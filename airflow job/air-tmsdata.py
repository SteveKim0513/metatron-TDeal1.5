from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['example@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'air-tmsdata',
    default_args=default_args,
    description='schedule air-tmsdata',
    schedule_interval=timedelta(minutes=5)
)

startlog = BashOperator(
    task_id='start-log-tmsdata',
    bash_command='echo "START AIRFLOW for tmsdata',
    dag=dag
)

rootUser = BashOperator(
    task_id='rootUser-tmsdata',
    bash_command='sudo su -',
    dag=dag
)

chownFile = BashOperator(
    task_id='chownFile-tmsdata',
    bash_command='chown -R metatron:metatron /data/s3data/tmsdata',
    dag=dag
)

metatronUser = BashOperator(
    task_id='metatronUser-tmsdata',
    bash_command='sudo su metatron',
    dag=dag
)

runVM = BashOperator(
    task_id='runVM-tmsdata',
    bash_command='source /data/druid-ingestion/druid-batch/bin/activate',
    dag=dag
)

moveDir = BashOperator(
    task_id='move-directory-druid-ingestion-tmsdata',
    bash_command='cd /data/druid-ingestion/t-deal-discovery/',
    dag=dag
)

runIngestion = BashOperator(
    task_id='run-druid-ingestion-tmsdata',
    bash_command='python3.7 main.py --target TMSDATA',
    dag=dag
)

endlog = BashOperator(
    task_id='end-log-tmsdata',
    bash_command='echo "END AIRFLOW for tmsdata',
    dag=dag
)

rootUser.set_downstream(startlog)
chownFile.set_downstream(rootUser)
metatronUser.set_downstream(chownFile)
runVM.set_downstream(metatronUser)
moveDir.set_downstream(runVM)
runIngestion.set_downstream(moveDir)
endlog.set_downstream(runIngestion)