from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email

def email_alert(context):
    title = "Airflow {dag_id} Falhou - Task {task_id}".format(dag_id=context.get('task_instance').dag_id,task_id=context.get('task_instance').task_id)
    body = """<h3>FALHA</h3> <br>
    <b>Dag:</b> {dag_id}  <br>
    <b>Task:</b> {task_id}  <br>
    <b>Execution Time:</b> {exec_date}  <br>
    <b>Log Url:</b> <a href='{log_url}'>Acessar Log do Erro</a> <br>
    """.format(task_id=context.get('task_instance').task_id, 
               dag_id=context.get('task_instance').dag_id,
               log_url=context.get('task_instance').log_url,
               exec_date=context.get('execution_date'))
    send_email(context.get('task').email, title, body)

project_name = 'nome-do-projeto-git' # sem .git
file_path = '/Code/Deployment/main/main.py'
git_tag = '0.0.1'
args = {
    'retries':1,
    'on_failure_callback':email_alert,
    'email':['candidabruxel@gmail.com']
}
# PostgreSQL connection details
DB_USERNAME = 'postgres'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_DATABASE = 'desafio'

dag = DAG(
    dag_id = project_name.upper().replace('-','_'),
    start_date = datetime(2022, 10, 20, 1 ,0),
    schedule_interval = '0 3 * * *',
    catchup = False,
    tags = [project_name.upper().replace('-','_')],
    max_active_runs = 1,
    default_args = args
    )

try:
    dop = DockerOperator(
        task_id = 'docker_operator',
        image = project_name + ':' + git_tag,
        command = 'python3 /app/main.py',
        docker_url = 'unix://var/run/docker.sock',
        network_mode = 'bridge',
        auto_remove = True,
        environment = {
                'UID_POSTGRES': DB_USERNAME,
                'PWD_POSTGRES': DB_PASSWORD,
                'HOST_POSTGRES': DB_HOST,
                'PORT_POSTGRES': DB_PORT,
                'DB_NAME': DB_DATABASE,
                'CITOKEN' :  Variable.get("CITOKEN"),
                'EXECUTION_DATE': "{{ ds }}",
                'COMMAND': 'python3 /app/' + project_name + file_path,
                'PROJECT_NAME':project_name
            },
            
        dag=dag
    )
except Exception as e:
    raise AirflowException(e)
