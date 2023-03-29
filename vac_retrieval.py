import requests
from pprint import pprint
import pandas as pd
from numpy import NaN
from tqdm import tqdm
from converter import ConverterHH
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
import os

load_dotenv()

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

headers = {'access_token': os.getenv('TOKEN', ' '),
           'token_type': 'bearer'}

# retrieving list of areas
areas = []
url_ar = 'https://api.hh.ru/areas'
res = requests.get(url_ar, headers=headers).json()
for el in res[0]['areas']:
    areas.append(el['id'])

# retrieving list of professional roles
professional_role = ['157', '79', '156', '10', '150', '165', '164', '148', '41', '163', '134', '40']

URL = 'https://api.hh.ru/vacancies'

for query in tqdm(queries):
    result = pd.DataFrame()
    for area in areas:
        sleep(5)
        for role in professional_role:
            sleep(1)
            pages = requests.get(
                    URL,
                    headers=headers, 
                    params={'text': query,
                            # 'search_field': 'name',
                            'professional_role': role,
                            'area': area,
                            'archived': False,
                            'period': 30}
                ).json()['pages']
            for page in tqdm(range(pages)):
                data = {}
                resp = requests.get(
                    URL,
                    headers=headers,
                    params={'text': query,
                            # 'search_field': 'name',
                            'professional_role': role,
                            'area': area,
                            'archived': False,
                            'page': page,
                            'period': 30}
                ).json()['items']
                for j in resp:
                    data['id'] = j['id']
                    data['spec'] = query
                    data['job_title'] = j['name']
                    data['city'] = j['area']['name']
                    data['employer'] = j['employer']['name']
                    if j['schedule']:
                        data['schedule'] = j['schedule']['name']
                    else:
                        data['schedule'] = NaN
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
