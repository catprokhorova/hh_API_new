import asyncio
import os
from pprint import pprint
from time import sleep, time

import requests
from aiohttp import ClientSession
from dotenv import load_dotenv
from more_itertools import chunked
from tqdm import tqdm

from converter import ConverterHH

load_dotenv()

vert = ConverterHH()

queries = [
    "Data Scientist",
    "Системный аналитик",
    "Аналитик данных",
    "1С-аналитик",
    "Финансовый аналитик",
    "Маркетинговый аналитик",
    "DataOps-инженер",
    "Дата-журналист",
    "Продуктовый аналитик",
    "Аналитик BI",
    "Дата-инженер",
    "Бизнес-аналитик",
]

headers = {"Authorization": f'Bearer {os.getenv("TOKEN", " ")}'}

# retrieving list of areas
areas = []
AREA_URL = "https://api.hh.ru/areas"
res = requests.get(AREA_URL, headers=headers).json()
for el in res[0]["areas"]:
    areas.append(el["id"])

URL = "https://api.hh.ru/vacancies"

params = {
    "search_field": "name",
    "archived": "false",
    "period": 30,
    "per_page": 100,
}


async def get_response(client, *args, **kwargs):
    sleep(0.1)
    response = await client.get(*args, **kwargs)
    return await response.json()


# async def get_vacancy():
#     client = ClientSession()


def get_number_pages():
    result = []
    print('====РЕГИОНЫ====')
    for area in tqdm(areas):
        params["area"] = area
        print('+++++ПРОФЕССИИИ')
        for query in tqdm(queries):
            params["text"] = query
            respons = requests.get(
                url=URL, params=params, headers=headers
            ).json()
            if len(respons["items"]) > 0:
                result.append(
                    {"query": query, "area": area, "pages": respons["pages"]}
                )

    return result


async def get_dict_pages(client, area, query):
    params["area"] = area
    params["text"] = query
    response = await get_response(client, URL, params=params)
    if len(response["items"]) > 0:
        return {"query": query, "area": area, "pages": response["pages"]}


# async def get_number_pages(client):
#     result = []
#     print('====РЕГИОНЫ====')
#     for area_chunk in tqdm(chunked(areas, 2)):
#         coros = []
#         for area in area_chunk:
#             print('++++ПРОФЕССИИ+++++')
#             for query in tqdm(queries):
#                 coros.append(get_dict_pages(client, area, query))
#         result_coros = await asyncio.gather(*coros)
#         result_coros = [res for res in result_coros if res is not None]
#         result += result_coros
#     return result


async def main():
    async with ClientSession(headers=headers) as client:
        result = await get_number_pages(client)
        pprint(result)


if __name__ == "__main__":
    start_time = time()
    # asyncio.run(main())
    pprint(get_number_pages())
    print(round((time() - start_time) / 60))
