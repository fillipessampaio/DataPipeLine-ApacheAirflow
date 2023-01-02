import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta


# Date intervals
start_date = datetime.today()
end_date = start_date + timedelta(days=7)


# Date format
start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')


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
file_path = f'/home/fillipessampaio/Documents/Curso-Apache-Airflow/week={start_date}/'
os.mkdir(file_path)

data.to_csv(file_path + 'raw_data.csv')
data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatures.csv')
data[['datetime', 'description', 'icon']].to_csv(file_path + 'conditions.csv')