import requests

from db.models import Query
from db.models import MetricsQuery

ACCESS_TOKEN = "y0_AgAEA7qkeLqBAAuw7AAAAAEDGzynAABr7pqZPg9NEb5O0OacK2wWzfFG2A"
USER_ID = "1130000065018497"
HOST_ID = "https:dn.ru:443"

# Формируем URL для запроса мониторинга поисковых запросов
URL = f"https://api.webmaster.yandex.net/v4/user/{USER_ID}/hosts/{HOST_ID}/query-analytics/list"

BODY = {
    "offset": 30000,
    "limit": 4,
    "device_type_indicator": "ALL",
    "text_indicator": "QUERY",
    "region_ids": [],
    "filters": {}
}

response = requests.post(URL, json=BODY, headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                  "Content-Type": "application/json; charset=UTF-8"})

data = response.json()

for query in data['text_indicator_to_statistics']:
    query_name = query['text_indicator']['value']
    new_url = [Query(query=query_name)]
    metrics = []
    date = query['statistics'][0]["date"]
    data_add = {
        "date": date,
        "ctr": 0,
        "impression": 0,
        "demand": 0,
        "clicks": 0,
    }
    for el in query['statistics']:
        if date != el['date']:
            metrics.append(MetricsQuery(
                query=query_name,
                date=date,
                ctr=data_add['ctr'],
                impression=data_add['impression'],
                demand=data_add['demand'],
                clicks=data_add['clicks']
            ))
            data_add = {
                "date": date,
                "ctr": 0,
                "impression": 0,
                "demand": 0,
                "clicks": 0,
            }
            date = el['date']
        field = el["field"]
        if field == "IMPRESSION":
            data_add["impression"] = el["value"]
        elif field == "CLICKS":
            data_add["clicks"] = el["value"]
        elif field == "DEMAND":
            data_add["demand"] = el["value"]
        elif field == "CTR":
            data_add["ctr"] = el["value"]
