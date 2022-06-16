import matplotlib.ticker as ticker
import pandas as pd
from collections import Counter, OrderedDict
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt

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
]


def count_occurrence_by_day(data):
    by_day = [row['datetime'].dayofweek for index, row in data.iterrows()]
    return dict(Counter(by_day))


def get_call_loss_time(data):
    by_hour = []

    for index, row in data.iterrows():
        hour = row['datetime'].hour
        minute = round(row['datetime'].minute / 10) * 10
        if minute == 60:
            hour += 1
            minute = 0

        by_hour.append(f'{hour:02d}:{minute:02d}')

    return by_hour


def count_occurrence(data):
    raw_dict = Counter(data)
    sorted_dict = dict(OrderedDict(sorted(raw_dict.items())))

    return sorted_dict


def open_and_format(filepath):
    with open(filepath, 'r') as file:
        data = pd.read_csv(file)

    data = data[data['is_answered'] == 'False'].reset_index()
    data = data.astype({'datetime': 'datetime64'})

    return data


def plot_fig(x, y):
    plt.scatter(formatted_date_list, calls_list)
    plt.gcf().autofmt_xdate()
    plt.xlabel('Heure de la journée')
    plt.ylabel('Nombre d\'appels manqués')
    plt.title('Appels manqués sur la période Janvier 2020 - Mai 2022')
    plt.xlim([dt.datetime(1900, 1, 1, 8, 55), dt.datetime(1900, 1, 1, 18, 5)])
    plt.xticks(
        ticks=[dt.datetime(1900, 1, 1, 9, 00), dt.datetime(1900, 1, 1, 10, 00), dt.datetime(1900, 1, 1, 11, 00),
            dt.datetime(1900, 1, 1, 12, 00), dt.datetime(1900, 1, 1, 13, 00),dt.datetime(1900, 1, 1, 14, 00),
            dt.datetime(1900, 1, 1, 15, 00), dt.datetime(1900, 1, 1, 16, 00), dt.datetime(1900, 1, 1, 17, 00),
            dt.datetime(1900, 1, 1, 18, 00)],
        labels=['9h00','10h00','11h00','12h00','13h00','14h00','15h00','16h00','17h00','18h00']
        )
    plt.show()


whole_data = []

for month in date[25:]:

    filepath = f'/home/nicolas/Development/Projects/OVH/files/analysed_files/data_{month}_ovh_analysed'

    data = open_and_format(filepath)

    whole_data.extend(get_call_loss_time(data))


all_calls_lost = count_occurrence(whole_data)

date_list = list(all_calls_lost.keys())
calls_list = list(all_calls_lost.values())

formatted_date_list = [dt.datetime.strptime(d, '%H:%M') for d in date_list]

plot_fig(formatted_date_list, calls_list)
