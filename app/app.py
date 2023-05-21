import asyncio
import os
from datetime import datetime
from pprint import pprint
from time import sleep
from typing import Optional

import requests
from aiohttp import ClientSession
from dotenv import load_dotenv
from more_itertools import chunked
from tqdm import tqdm

from converter import ConverterHH

load_dotenv()

vert = ConverterHH()

queries = [
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


async def get_response(client: ClientSession, *args, **kwargs) -> dict:
    await asyncio.sleep(1)
    response = await client.get(*args, **kwargs)
    return await response.json()


# async def get_vacancy():
#     client = ClientSession()


async def get_dict_pages(
    client: ClientSession, area: str, query: str
) -> Optional[dict]:
    params["area"] = area
    params["text"] = query
    response = await get_response(client, URL, params=params)
    return {
        "query": query,
        "area": area,
        "pages": response["pages"],
        "items": response["items"],
    }


async def get_number_pages(client: ClientSession) -> list:
    result = []
    print("====РЕГИОНЫ====")
    for area_chunk in tqdm(chunked(areas, 2)):
        coros = []
        for area in area_chunk:
            print("++++ПРОФЕССИИ+++++")
            for query in tqdm(queries):
                coros.append(get_dict_pages(client, area, query))
        result_coros = await asyncio.gather(*coros)
        result += [res for res in result_coros if len(res['items']) > 0]
    return result


async def get_dict_vacancy_id(
    client: ClientSession, area: str, query: str, page: int
) -> dict:
    params["area"] = area
    params["text"] = query
    params["page"] = page
    response = await get_response(client, URL, params=params)
    print(response)
    return {
        "query": query,
        "ids": set(id for vac in response["items"]),
    }


async def get_all_ids(client: ClientSession) -> dict:
    ids = {}
    pages_liist = await get_number_pages(client)
    for area_chunked in tqdm(chunked(areas, 2)):
        for area in area_chunked:
            print("++++ПРОФЕССИИ+++++")
            for query in tqdm(queries):
                print("---СТРАНИЦЫ---")
                for page_dict_chunk in chunked(pages_liist, 5):
                    coros = []
                    for page_dict in page_dict_chunk:
                        ids.setdefault(page_dict["query"], set())
                        for page in range(page_dict["pages"]):
                            # print(query, area, page)
                            coros.append(
                                get_dict_vacancy_id(
                                    client, area=area, query=query, page=page
                                )
                            )
                    result_coros = await asyncio.gather(*coros)
                    for dict_ids in result_coros:
                        key, value = dict_ids.values()
                        ids[key].update(value)
    return ids


async def main():
    async with ClientSession(headers=headers) as client:
        pages_liist = await get_number_pages(client)
        print(len(pages_liist))


if __name__ == "__main__":
    start_time = datetime.now()
    asyncio.run(main())
    print(datetime.now() - start_time)
