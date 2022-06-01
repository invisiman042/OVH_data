import json
from matplotlib import pyplot as plt

with open('recap.json', 'r') as file:
    data_file = json.load(file)

dates = [date for date in data_file.keys()]
rates = [round(data['rate'], 2) if isinstance(data['rate'], float) else 0 for data in data_file.values()]

fig, ax = plt.subplots()

ax.scatter(dates, rates)
plt.ylim([0, 1])
plt.show()