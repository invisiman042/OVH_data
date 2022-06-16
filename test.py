import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

date_list1 = [datetime.time(9, 0), datetime.time(9, 10), datetime.time(9, 20)]
date_list2 = [datetime.time(9, 0, 10), datetime.time(9, 10, 40), datetime.time(9, 20, 30)]
values = [0, 2, 1]

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
# plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
plt.scatter(date_list2, values)
plt.gcf().autofmt_xdate()
plt.show()