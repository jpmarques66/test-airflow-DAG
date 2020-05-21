from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t1 = KubernetesPodOperator(namespace='test-airflow',
                           image="python:3.6",
                           cmds=["python", "-c"],
                           arguments=["print('hello world')"],
                           labels={"foo": "bar"},
                           name="passing-test",
                           task_id="passing-task",
                           get_logs=True,
                           dag=dag,
                           in_cluster=True
                           )

t2 = KubernetesPodOperator(namespace='test-airflow',
                           image="python:3.6",
                           cmds=["sleep", "5"],
                           labels={"foo": "bar"},
                           name="passing-test",
                           task_id="passing-task",
                           get_logs=True,
                           dag=dag,
                           in_cluster=True
                           )

dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""
templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = KubernetesPodOperator(namespace='test-airflow',
                           image="python:3.6",
                           cmds=["sleep", "5"],
                           labels={"foo": "bar"},
                           name="passing-test",
                           task_id="passing-task",
                           get_logs=True,
                           depends_on_past=False,
                           dag=dag,
                           in_cluster=True
                           )
t1 >> [t2, t3]
