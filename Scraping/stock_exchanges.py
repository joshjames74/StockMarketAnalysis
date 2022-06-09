import requests

exchange_data_url = 'https://www.tradinghours.com/markets'

html = requests.get(exchange_data_url)
print(html.text)

