import os
import time
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from numpy import NaN
from tqdm import tqdm

from converter import ConverterHH

load_dotenv()

URL = "https://api.hh.ru/vacancies"

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
res = requests.get("https://api.hh.ru/areas", headers=headers).json()
for el in res[0]["areas"]:
    areas.append(el["id"])

# # retrieving list of professional roles
# professional_role = ['157', '79', '156', '10', '150',
#                      '165', '164', '148', '41', '163',
#                      '134', '40']


def empty(url, query):
    resp = requests.get(
        url,
        headers=headers,
        params={
            "text": query,
            "search_field": "name",
            "area": 113,
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
                "period": 30,
                "per_page": 100,
            },
        ).json()["pages"]
        for page in tqdm(range(pages)):
            data = {}
            resp = requests.get(
                URL,
                headers=headers,
                params={
                    "text": query,
                    "search_field": "name",
                    "area": area,
                    "page": page,
                    "period": 30,
                    "per_page": 100,
                },
            ).json()["items"]
            for j in resp:
                data["date"] = datetime.today().strftime("%Y-%m-%d")
                vacancy = requests.get(
                    f"{URL}/{j['id']}", headers=headers
                ).json()
                data["id"] = vacancy["id"]
                data["spec"] = query
                data["job_title"] = vacancy["name"]
                data["city"] = vacancy["area"]["name"]
                data["employer"] = vacancy["employer"]["name"]
                if vacancy["schedule"]:
                    data["schedule"] = vacancy["schedule"]["name"]
                else:
                    data["schedule"] = NaN
                time.sleep(0.5)
                if vacancy.get("key_skills"):
                    data["skills"] = ", ".join(
                        [s["name"] for s in vacancy["key_skills"]]
                    ).lower()
                else:
                    data["skills"] = NaN
                if vacancy.get("experience"):
                    data["experience"] = vacancy["experience"]["name"]
                else:
                    data["experience"] = NaN
                data["published"] = datetime.strptime(
                    vacancy["published_at"].split("T")[0], "%Y-%m-%d"
                )
                salary = vacancy.get("salary")
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
