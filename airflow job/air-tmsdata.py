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
    description='schedule tmsdata',
    schedule_interval=timedelta(minutes=5)
)

startlog = BashOperator(
    task_id='start-log-tmsdata',
    bash_command='echo "START"',
    dag=dag
)

runVM = 'source /data/druid-ingestion/druid-batch/bin/activate\n' + 'cd /data/druid-ingestion/t-deal-discovery\n' + 'python3 main.py --target TMSDATA\n'
VMBash = BashOperator(
    task_id='runVM-tmsdata',
    bash_command=runVM,
    dag=dag
)

endlog = BashOperator(
    task_id='end-log-tmsdata',
    bash_command='echo "END"',
    dag=dag
)


startlog.set_downstream(VMBash)
VMBash.set_downstream(endlog)
