from airflow import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
from os.path import join
import pandas as pd


with DAG(
    dag_id='climate_data',
    start_date=pendulum.datetime(2022, 12, 5, tz='UTC'),
    schedule_interval='0 0 * * 1', # execute every mondays
) as dag:

    task_1 = BashOperator(
        task_id = 'create_folder',
        bash_command = 'mkdir -p "/home/fillipessampaio/Documents/Curso-Apache-Airflow/week={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extract_climate_data(data_interval_end):
        # Date intervals
        start_date = data_interval_end
        end_date = ds_add(data_interval_end, 7)
        
        # API URL 
        city = 'Toronto'
        key = '64NSBQ9FEQQ4SF7YY2AXNMMW7'
        url = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/', 
                    f'{city}/{start_date}/{end_date}',
                    f'?unitGroup=metric&key={key}&include=days&contentType=csv'
                    )

        # API request
        data = pd.read_csv(url)

        # Writing files
        file_path = f'/home/fillipessampaio/Documents/Curso-Apache-Airflow/week={data_interval_end}/'
        data.to_csv(file_path + 'raw_data.csv')
        data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatures.csv')
        data[['datetime', 'description', 'icon']].to_csv(file_path + 'conditions.csv')


    task_2 = PythonOperator(
        task_id = 'extract_climate_data',
        python_callable = extract_climate_data,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )


    task_1 >> task_2