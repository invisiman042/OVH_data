import json
import ovh
import datetime as dt
import requests
from tqdm import tqdm

now = dt.datetime.now()

app_key = 'b49e8c798613c61f'
app_secret = '964eb0b2cc9298964934953632c33f51'
consumer_key = 'f5b88bea1ad280f2f4587eeade9453d6'

serviceName = [
    "0033458008127", # ligne tampon support
    "0033458005730", # ligne générale
    "0033458008213" # ligne tampon commercial
]

billingAccount = [
    'aa443014-ovh-1',
]

support_agents = [
                '2231572', # ligne support 5709
                '2236032', # numéro perso Nicolas
                ]

queueId = [
    '472252',
]

date = [
    "2018-12-01",
    "2019-01-01",
    "2019-02-01",
    "2019-03-01",
    "2019-04-01",
    "2019-05-01",
    "2019-06-01",
    "2019-07-01",
    "2019-08-01",
    "2019-09-01",
    "2019-10-01",
    "2019-11-01",
    "2019-12-01",
    "2020-01-01",
    "2020-02-01",
    "2020-03-01",
    "2020-04-01",
    "2020-05-01",
    "2020-06-01",
    "2020-07-01",
    "2020-08-01",
    "2020-09-01",
    "2020-10-01",
    "2020-11-01",
    "2020-12-01",
    "2021-01-01",
    "2021-02-01",
    "2021-03-01",
    "2021-04-01",
    "2021-05-01",
    "2021-06-01",
    "2021-07-01",
    "2021-08-01",
    "2021-09-01",
    "2021-10-01",
    "2021-11-01",
    "2021-12-01",
    "2022-01-01",
    "2022-02-01",
    "2022-03-01",
    "2022-04-01",
    "2022-05-01",
    "2022-06-01",
]

client = ovh.Client(
    endpoint='ovh-eu',
    application_key=app_key,
    application_secret=app_secret,
    consumer_key=consumer_key
)

for chosen_date in tqdm(date):

    data_consumption_request = f'/telephony/{billingAccount[0]}/historyConsumption/{chosen_date}/file'

    response = client.get(data_consumption_request, extension='received.csv')
    data = json.dumps(response, indent=4)

    file_response = requests.get(response['url'])

    with open(f'files/raw_files/data_{chosen_date}_ovh', 'wb') as file:
        file.write(file_response.content)