from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime
import os
import shutil


# -------------------- Переменные -------------------------------------
FILES_DIR = Variable.get("FILES_DIR", default_var="./temp_files")
RES_DIR = Variable.get("RES_DIR", default_var="./results")
NUM_FILES = 100

# -------------------- Функции для шагов DAG ---------------------------


def generate_files_func(**kwargs):
    """Генерирует 100 текстовых файлов. Использован скрипт от преподавателя"""
    import string
    import random
    
    if not os.path.exists(FILES_DIR):
        os.makedirs(FILES_DIR)

    for i in range(1, NUM_FILES + 1):
        file_path = os.path.join(FILES_DIR, f"file_{i}.txt")
        with open(file_path, 'w') as f:
            text = ''.join(random.choice(string.ascii_lowercase) for _ in range(1000))
            f.write(text)
    kwargs['ti'].xcom_push(key='files_dir', value=FILES_DIR) # Передача пути


def count_a_func(file_path, res_path):
    """Подсчитывает количество вхождений 'a' в файле и записывает результат."""
    count = 0
    with open(file_path, 'r') as f:
        content = f.read()
        count = content.count('a')
    with open(res_path, 'w') as res_file:
        res_file.write(str(count))

    
def remove_temp_files(**kwargs):
  """Удаляет временную папку"""
  shutil.rmtree(kwargs['ti'].xcom_pull(task_ids='generate_files_task', key='files_dir'))

def clear_res_dir():
    """Очищаем папку с результатами"""
    for file in os.listdir(RES_DIR):
        os.remove(os.path.join(RES_DIR, file))


with DAG(
    dag_id='parallel_counter',
    start_date=datetime(2023, 11, 1),
    schedule=None,
    catchup=False,
    tags=['example'],
) as dag:

    # ---------------- Шаг 1: Генерация файлов ----------------------
    generate_files_task = PythonOperator(
        task_id='generate_files_task',
        python_callable=generate_files_func,
    )
    
     # ---------------- Шаг 2: Очистка папки результатов ----------------------
    clear_res_dir_task = PythonOperator(
         task_id="clear_res_dir",
         python_callable=clear_res_dir
     )
    

    # ---------------- Шаг 3: Параллельный подсчет --------------------
    with TaskGroup("parallel_count_group") as parallel_count_group:
        count_tasks = [
            PythonOperator(
                task_id=f'count_a_task_{i}',
                python_callable=count_a_func,
                op_kwargs={
                  "file_path": os.path.join(FILES_DIR, f"file_{i}.txt"),
                  "res_path": os.path.join(RES_DIR, f"{i}.res")
                },
            )
            for i in range(1, NUM_FILES + 1)
        ]

    # ---------------- Шаг 4: Итоговый подсчет -----------------------
    
    total_count_task = BashOperator(
            task_id="total_count_task",
            bash_command=f"find {RES_DIR} -name '*.res' -exec cat {{}} \\; | awk '{{ sum+=$1 }} END {{ print sum }}' > {RES_DIR}/total.res"
        )


    remove_files_task = PythonOperator(
          task_id="remove_temp_files_task",
          python_callable=remove_temp_files
    )
    
    generate_files_task >> clear_res_dir_task >> parallel_count_group >> total_count_task >> remove_files_task