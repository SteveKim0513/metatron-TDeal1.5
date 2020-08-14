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
    'air-salesdata',
    default_args=default_args,
    description='schedule air-salesdata',
    schedule_interval=timedelta(minutes=5)
)


startlog = BashOperator(
    task_id='start-log-salesdata',
    bash_command='echo "START AIRFLOW for salesdata',
    dag=dag
)

runVM = BashOperator(
    task_id='runVM-salesdata',
    bash_command='source /data/druid-ingestion/druid-batch/bin/activate',
    dag=dag
)

moveDir = BashOperator(
    task_id='move-directory-druid-ingestion-salesdata',
    bash_command='cd /data/druid-ingestion/t-deal-discovery/',
    dag=dag
)

runIngestion = BashOperator(
    task_id='run-druid-ingestion-salesdata',
    bash_command='python3.7 main.py --target SALESDATA',
    dag=dag
)


endlog = BashOperator(
    task_id='end-log-salesdata',
    bash_command='echo "END AIRFLOW for salesdata',
    dag=dag
)


runVM.set_downstream(startlog)
moveDir.set_downstream(runVM)
runIngestion.set_downstream(moveDir)
endlog.set_downstream(runIngestion)