import requests
import pandas as pd
import re
from tqdm import tqdm
from time import sleep
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

def get_id(url, pages):
    count = 0
    for i in range(pages + 1):
        url = f'{url}&page={i}'
        resp = requests.get(url, headers=headers).text
        soup = BeautifulSoup(resp, features="html.parser")
        cards = soup.find_all(class_='serp-item')
        sleep(2)
        for card in cards:
            count += 1
            link = card.find('a', class_='serp-item__title')
            print(count, link.get('href'))
            id = re.findall(r'[0-9]+', link.get('href'))[0]
            with open('ids.txt', 'a', encoding='utf-8') as w, open('log.txt', 'a', encoding='utf-8') as l:
                w.write(id + '\n')
                l.write(f'{i} {count} {link}\n')
            sleep(1)
get_id(url, last_page)