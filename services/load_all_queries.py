import requests

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



# for query in data['text_indicator_to_statistics']:
