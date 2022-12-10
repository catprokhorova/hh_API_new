import requests
import pandas as pd
from tqdm import tqdm
from converter import Converter

vert = Converter()

queries = [
        # 'developer',
        # 'frontend',
        # 'backend',
        # 'fullstack',
        # 'gamedev',
        # 'game',
        # 'system',
        # 'software',
        # 'engineer',
        # 'ios',
        # 'android',
        # 'windows',
        # 'linux',
        # 'разработчик',
        # 'фронтенд', 'бэкенд',
        # 'системный',
        # 'инженер',
        'python',
        # 'JavaScript',
        # 'PHP',
        # 'C',
        # 'Bitrix',
        # '1C',
        # 'C#',
        # 'C++',
        # 'React',
        # 'Angular',
        # 'Django',
        # 'Flask',
        # 'FastAPI',
        # 'iohttp'
    ]

URL = 'https://api.hh.ru/vacancies'

result = pd.DataFrame()

for query in tqdm(queries):
    pages = requests.get(
            URL, 
            params={'text': query, 'period': 30},
        ).json()['pages']
    for page in tqdm(range(pages)):
        data = {}
        resp = requests.get(
            URL,
            params={'text': query, 'page': page, 'period': 30},
        ).json()['items']
        for j in resp:
            data['id'] = j['id']
            data['job_title'] = j['name']
            data['created_at'] = j['created_at']
            data['city'] = j['area']['name']
            data['schedule'] = j['schedule']['name']
            salary = j.get('salary')
            if salary:
                course = vert.get_valute(salary['currency'])
                if salary['currency'] != 'RUR' and course:
                    data['salary_from'] = salary['from'] * course if salary['from'] else None
                    data['salary_to'] = salary['to'] * vert.get_valute(salary['currency']) if salary['to'] else None
                    data['currency'] = 'RUR'
                else:
                    data['salary_from'] = salary['from']
                    data['salary_to'] = salary['to']
                    data['currency'] = salary['currency']
            else:
                data['salary_from'] = salary
                data['salary_to'] = salary
                data['currency'] = salary
            result = pd.concat([result, pd.DataFrame([data])]).drop_duplicates()

result.to_csv('zmeya_dushnila.csv', sep=';', index=False)
