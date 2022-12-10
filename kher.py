import requests
from pprint import pprint

url = 'https://api.hh.ru/vacancies'
r = requests.get(url).json()
id_list = []

with open('spec.csv', encoding='utf-8') as spec:
    for el in spec:
        id_list.append(el.strip().split(';')[1])

print(id_list)