import requests
import pandas as pd
from tqdm import tqdm
import time
import os
from pprint import pprint
import json
from bs4 import BeautifulSoup
from fake_headers import Headers
from converter import Converter

vert = Converter()

url = 'https://hh.ru/search/vacancy?area=113&enable_snippets=true&search_period=30&text=python'
headers = Headers(os="win", headers=True).generate()

resp = requests.get(url, headers=headers).text
soup = BeautifulSoup(resp, features="html.parser")
last_page = int(soup.find_all(class_='pager-item-not-in-short-range')[-1].find('span').text)

for i in range(last_page + 1):
    url = f'{url}&page={i}'
    resp = requests.get(url, headers=headers).text
    soup = BeautifulSoup(resp, features="html.parser")
    cards = soup.find_all(class_='serp-item')
    for card in cards:
        title = card.find(class_='serp-item__title').text
        emp = card.find(class_='bloko-v-spacing-container bloko-v-spacing-container_base-2').text
        city = card.find(class_='vacancy-serp__vacancy-address').text
        print(title, city)
        break
    break
