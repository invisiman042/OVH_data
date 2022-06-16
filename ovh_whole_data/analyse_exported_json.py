import json
from matplotlib import pyplot as plt

with open('recap.json', 'r') as file:
    data_file = json.load(file)

dates = [date for date in data_file.keys()]
rates = [round(data['rate'], 2) if isinstance(data['rate'], float) else 0 for data in data_file.values()]

nb_calls = [
    (values['data']['true'] + values['data']['false']) if 'false' in values['data'] else values['data']['true'] for values in data_file.values()
    ]

fig, ax = plt.subplots()

ax.scatter(dates, rates)
# plt.xticks(['2022-03"', '2022-04"', '2022-05"'], rotation=45)
plt.xticks(rotation=45)
plt.ylim([0, 1])
plt.grid(True)
plt.show()