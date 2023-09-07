import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from numpy import NaN
from tqdm import tqdm

from converter import ConverterHH

start = time.time()
load_dotenv()
logging.basicConfig(
    filename="requsts.log", encoding="utf-8", level=logging.ERROR
)
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
url_ar = "https://api.hh.ru/areas"
res = requests.get(url_ar, headers=headers).json()
for el in res[0]["areas"]:
    areas.append(el["id"])

# # retrieving list of professional roles
# professional_role = ['157', '79', '156', '10', '150',
#                      '165', '164', '148', '41', '163',
#                      '134', '40']

URL = "https://api.hh.ru/vacancies"


def empty(url, query):
    resp = requests.get(
        url,
        headers=headers,
        params={
            "text": query,
            "search_field": "name",
            "area": 113,
            "archived": False,
            "period": 30,
            "per_page": 100,
        },
    ).json()["items"]
    if len(resp) == 0:
        columns = [
            "id",
            "spec",
            "job_title",
            "city",
            "employer",
            "schedule",
            "skills",
            "experience",
            "published",
            "salary_from",
            "salary_to",
            "currency",
        ]
        pd.DataFrame(columns=columns).to_csv(
            f"results/raw/{query}.csv", sep=";", index=False
        )
        return True


def not_empty(url, query):
    result = pd.DataFrame()
    for area in areas:
        time.sleep(0.5)
        pages = requests.get(
            url,
            headers=headers,
            params={
                "text": query,
                "search_field": "name",
                "area": area,
                "archived": False,
                "period": 30,
                "per_page": 100,
            },
        ).json()["pages"]
        for page in tqdm(range(pages)):
            data = {}
            try:
                resp = requests.get(
                    URL,
                    headers=headers,
                    params={
                        "text": query,
                        "search_field": "name",
                        "area": area,
                        "archived": False,
                        "page": page,
                        "per_page": 100,
                    },
                ).json()["items"]
            except KeyError:
                resp = requests.get(
                    URL,
                    headers=headers,
                    params={
                        "text": query,
                        "search_field": "name",
                        "area": area,
                        "archived": False,
                        "page": page,
                        "per_page": 100,
                    },
                )
                logging.error(f"{resp.headers}")
                logging.error(f"{resp.json()}")
            for j in resp:
                data["date"] = datetime.today().strftime("%Y-%m-%d")
                data["id"] = j["id"]
                data["spec"] = query
                data["job_title"] = j["name"]
                data["city"] = j["area"]["name"]
                data["employer"] = j["employer"]["name"]
                if j["schedule"]:
                    data["schedule"] = j["schedule"]["name"]
                else:
                    data["schedule"] = NaN
                time.sleep(0.5)
                skill = requests.get(
                    f"{URL}/{j['id']}", headers=headers
                ).json()
                if skill.get("key_skills"):
                    data["skills"] = ", ".join(
                        [s["name"] for s in skill["key_skills"]]
                    ).lower()
                else:
                    data["skills"] = NaN
                if skill.get("experience"):
                    data["experience"] = skill["experience"]["name"]
                else:
                    data["experience"] = NaN
                data["published"] = datetime.strptime(
                    j["published_at"].split("T")[0], "%Y-%m-%d"
                )
                salary = j.get("salary")
                if salary:
                    course = vert.get_valute(salary["currency"])
                    if salary["currency"] != "RUR" and course:
                        data["salary_from"] = (
                            round(salary["from"] / course)
                            if salary["from"]
                            else None
                        )
                        data["salary_to"] = (
                            round(salary["to"] / course)
                            if salary["to"]
                            else None
                        )
                        data["currency"] = "RUR"
                    else:
                        data["salary_from"] = salary["from"]
                        data["salary_to"] = salary["to"]
                        data["currency"] = salary["currency"]
                else:
                    data["salary_from"] = salary
                    data["salary_to"] = salary
                    data["currency"] = salary
                result = pd.concat([result, pd.DataFrame([data])])
    result.to_csv(f"results/raw/{query}.csv", sep=";", index=False)


def main():
    for query in tqdm(queries):
        if not empty(URL, query):
            not_empty(URL, query)


if __name__ == "__main__":
    main()
