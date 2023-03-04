import requests
from pprint import pprint
from requests import Session
import pandas as pd
from tqdm import tqdm
from converter import ConverterHH
from datetime import datetime

vert = ConverterHH()

queries = [
    'Data Scientist',
    'Системный аналитик',
    'Аналитик данных',
    '1С-аналитик',
    'Финансовый аналитик',
    'Маркетинговый аналитик',
    'DataOps-инженер',
    'Дата-журналист',
    'Продуктовый аналитик',
    'Аналитик BI',
    'Дата-инженер'
    ]

URL = 'https://api.hh.ru/vacancies'



for query in tqdm(queries):
    result = pd.DataFrame()
    pages = requests.get(
            URL, 
            params={'text': query, 
                    'area': 113,
                    'experience': 'noExperience',
                    'archived': False,
                    'period': 30},
        ).json()['pages']
    for page in tqdm(range(pages)):
        data = {}
        resp = requests.get(
            URL,
            params={'text': query, 
                    'area': 113,
                    'experience': 'noExperience',
                    'archived': False,
                    'page': page,
                    'period': 30},
        ).json()['items']
        for j in resp:
            data['id'] = j['id']
            data['spec'] = query
            data['job_title'] = j['name']
            data['city'] = j['area']['name']
            data['employer'] = j['employer']['name']
            data['schedule'] = j['schedule']['name']
            data['published'] = datetime.strptime(j['published_at'].split('T')[0], '%Y-%m-%d')
            salary = j.get('salary')
            if salary:
                course = vert.get_valute(salary['currency'])
                if salary['currency'] != 'RUR' and course:
                    data['salary_from'] = round(salary['from'] / course) if salary['from'] else None
                    data['salary_to'] = round(salary['to'] / course) if salary['to'] else None
                    data['currency'] = 'RUR'
                else:
                    data['salary_from'] = salary['from']
                    data['salary_to'] = salary['to']
                    data['currency'] = salary['currency']
            else:
                data['salary_from'] = salary
                data['salary_to'] = salary
                data['currency'] = salary
            result = pd.concat([result, pd.DataFrame([data])])

    result.to_csv(f'results/{query}_vac.csv', sep=';', index=False)
