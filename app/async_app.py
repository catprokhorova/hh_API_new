import asyncio
import datetime
import logging
import os
from pprint import pprint

import httpx
from dotenv import load_dotenv
from tqdm import tqdm

from converter import ConverterHH

load_dotenv()

logging.basicConfig(
    filename="example.log", encoding="utf-8", level=logging.DEBUG
)

vert = ConverterHH()

QUERIES = [
    # "Data Scientist",
    # "Системный аналитик",
    # "Аналитик данных",
    # "1С-аналитик",
    "Финансовый аналитик",
    # "Маркетинговый аналитик",
    # "DataOps-инженер",
    # "Дата-журналист",
    # "Продуктовый аналитик",
    # "Аналитик BI",
    # "Дата-инженер",
    # "Бизнес-аналитик",
]

HEADERS = {"Authorization": f'Bearer {os.getenv("TOKEN", " ")}'}


def chunk_generator(list_: list | range, max_val):
    for idx in range(0, len(list_), max_val):
        yield list_[idx : idx + max_val]


# retrieving list of areas
areas = []
AREA_URL = "https://api.hh.ru/areas"
res = httpx.get(AREA_URL, headers=HEADERS).json()
for el in res[0]["areas"]:
    areas.append(el["id"])

VACANCIES_URL = "https://api.hh.ru/vacancies"

params = {
    "search_field": "name",
    "archived": "false",
    "period": 30,
    "per_page": 100,
}


async def get_response(client: httpx.AsyncClient, *args, **kwargs):
    # await asyncio.sleep(0.1)
    response = await client.get(*args, **kwargs)
    return response.json()


def get_number_pages():
    result = []
    print("====РЕГИОНЫ====")
    for area in tqdm(areas):
        params["area"] = area
        print("+++++ПРОФЕССИИИ")
        for query in tqdm(QUERIES):
            params["text"] = query
            respons = httpx.get(
                url=VACANCIES_URL, params=params, headers=HEADERS
            ).json()
            with open("log_sync.log", "a") as f:
                f.write(f"{respons}\n")
            if len(respons["items"]) > 0:
                result.append(
                    {"query": query, "area": area, "pages": respons["pages"]}
                )

    return result


async def get_dict_pages(client, area, query):
    params["area"] = area
    params["text"] = query
    response = await get_response(client, VACANCIES_URL, params=params)
    if len(response["items"]) > 0:
        return {"query": query, "area": area, "pages": response["pages"]}


# async def get_number_pages(client):
#     result = []
#     print("====РЕГИОНЫ====")
#     for area_chunk in tqdm(chunked(areas, 2)):
#         coros = []
#         for area in area_chunk:
#             print("++++ПРОФЕССИИ+++++")
#             for query in tqdm(queries):
#                 coros.append(get_dict_pages(client, area, query))
#         result_coros = await asyncio.gather(*coros)
#         result_coros = [res for res in result_coros if res is not None]
#         result += result_coros
#     return result


async def main():
    async with httpx.AsyncClient(headers=HEADERS) as client:
        result = await get_number_pages(client)
        pprint(result)


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(f"Время исполнения: {datetime.datetime.now() - start}")
