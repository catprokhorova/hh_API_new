from urllib import response
import requests
from pprint import pprint
import json
import pandas as pd
from converter import Converter

vert = Converter()

queries = ['developer', 'frontend', 'backend', 'fullstack', 'gamedev', 'game',  'system', 'software', 'engineer',
'ios', 'android', 'windows', 'linux', 'разработчик', 'фронтенд', 'бэкенд', 'системный', 'инженер', 'python',
'JavaScript', 'PHP', 'C', 'Bitrix', '1C', 'C#', 'C++', 'React', 'Angular', 'Django', 'Flask', 'FastAPI', 'iohttp']

url = 'https://api.hh.ru/vacancies'
result = pd.DataFrame()

for query in queries:
    for page in range(0,1):
        data = {}
        resp = requests.get(url, params={'text': query, 'page': page, 'period': 30}).json()['items']
        for j in resp:
            data['id'] = j['id']
            data['job_title'] = j['name']
            data['created_at'] = j['created_at']
            data['city'] = j['area']['name']
            salary = j.get('salary')
            if salary:
                if salary['currency'] != 'RUR':
                    data['salary_from'] = salary['from'] * vert.get_valute(salary['currency']) if salary['from'] else None
                    data['salary_to'] = salary['to'] * vert.get_valute(salary['currency']) if salary['to'] else None
                    data['currency'] = salary['currency']
                else:
                    data['salary_from'] = salary['from']
                    data['salary_to'] = salary['to']
                    data['currency'] = salary['currency']
            else:
                data['salary_from'] = salary
                data['salary_to'] = salary
                data['currency'] = salary
            result = pd.concat([result, pd.DataFrame([data])]).drop_duplicates()

result.to_csv('dev_vac_test.csv', sep=';', index=False)