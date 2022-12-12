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
    pages = requests.get(url, params={'text': query, 'period': 30}).json()['pages']
    print(pages)