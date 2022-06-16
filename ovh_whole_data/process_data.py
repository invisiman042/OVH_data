import pandas as pd
import numpy as np
import json
import argparse
from tqdm import tqdm

# parser = argparse.ArgumentParser()
# parser.add_argument('file_path', help='file path to read')
# # parser.add_argument('phone_number', help='phone number to exclude')
# # parser.add_argument('-scsv', '--save_csv_file', help='save processed csv data to output file', action='store_true')
# args = parser.parse_args()


class nEncoder(json.JSONEncoder):
    '''Correctly encode np dataframe for json purpose'''
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return list(obj)
        return super(nEncoder, self).default(obj)


def read_csv_file(filepath):
    '''Read csv file and return it as a dataframe'''
    with open(filepath) as file:
        data = pd.read_csv(file)
        is_empty = len(data) == 0
    return data, is_empty


def read_json_file(filepath):
    '''Read json file and return it as a dictionnary'''
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data


def change_type(dataframe):
    '''Set the variable type for the following columns : phoneLine, callingNumber, nature, datetime'''
    dataframe = dataframe.dropna()
    dataframe = dataframe.astype({
        'phoneLine': 'string',
        'callingNumber': 'string',
        'nature': 'string',
        'datetime': 'string',
    })
    return dataframe


def filter_phoneline(dataframe):
    '''Filter the dataframe to exclude the waiting queue phone line (useless for data extraction)'''
    dataframe = dataframe[dataframe['phoneLine'] != '33458008127']
    return dataframe    


def sort_dataframe(dataframe):
    '''Sort the dataframe by calling number, then by datetime and finally by phone line. Reset the index to match the current order'''
    dataframe = dataframe.sort_values(by=['callingNumber', 'datetime', 'phoneLine'], ascending=[True, True, False], ignore_index=True)
    dataframe = dataframe.reset_index(drop=True)
    return dataframe


def convert_datetime(dataframe):
    '''Convert string to dataframe object for the columns date and time'''
    # dataframe['date'] = pd.to_datetime(dataframe['date'], format='%Y%m%d')
    # dataframe['time'] = pd.to_datetime(dataframe['time'], format='%H%M%S').dt.time
    dataframe['datetime'] = pd.to_datetime(dataframe['datetime'], format='%Y%m%d%H%M%S')
    return dataframe


def count_customer(dataframe):
    '''Add a new column to count the customers'''
    dataframe['customerCount'] = 1
    customer_count = 1
    for index, row in dataframe[1:].iterrows():
        if row.callingNumber != dataframe.loc[index - 1].callingNumber:
            customer_count += 1
        dataframe.at[index, 'customerCount'] = customer_count
    return dataframe


def count_call(dataframe):
    '''Add a new column to count the calls'''
    dataframe['callCount'] = 1
    call_count = 1
    for index, row in dataframe[1:].iterrows():
        if (row.customerCount != dataframe.loc[index - 1].customerCount) or (row.nature == 'transfert'):
            call_count += 1
        dataframe.at[index, 'callCount'] = call_count
    return dataframe


def process_data(dataframe):
    '''Uses the previous unit functions in order to process the data and return the processed dataframe'''
    dataframe = change_type(dataframe)
    dataframe = filter_phoneline(dataframe)
    dataframe = sort_dataframe(dataframe)
    dataframe = convert_datetime(dataframe)
    dataframe = count_customer(dataframe)
    dataframe = count_call(dataframe)
    return dataframe


def save_df_file(dataframe, save_path):
    '''Save a dataframe to csv file at file path'''
    dataframe.to_csv(save_path, index=False)


def save_json_file(dataframe, save_path):
    '''Save a dictionary to json file by enconding its value to be accepted by json'''
    with open(save_path, 'w') as file:
        # json_string = json.dumps(dataframe, cls=nEncoder)
        json.dump(dataframe, file, cls=nEncoder)


def add_json_data(data, file):
    with open(file, 'r') as f:
        output = json.load(f)
        output.update(data)
    with open(file, 'w') as f:
        items = output.items()
        sorted_dict = dict(sorted(items))
        json.dump(sorted_dict, f)


def analyse_data(dataframe):
    '''Create a dataframe containing all calls and their attributes : total duration of the call, call with operator, if the call has been answered
    and the waiting time before that answer'''
    support_line = 33458005709
    sales_line = 33458005707
    output_data = {}
    total_calls = dataframe.callCount.max()
    output_dataframe = pd.DataFrame(columns=[
            'callingNumber',
            'customer_waiting_time',
            'is_answered',
            'call_duration',
            'waiting_before_answer',
            'datetime',
            'call_count',
            'customer_count',
            ])

    for call in range(1, total_calls + 1):
        total_duration_data = dataframe.loc[(dataframe['callCount'] == call) & (dataframe['phoneLine'] == 33458005730), 'duration']
        call_duration_data = dataframe.loc[(dataframe['callCount'] == call) & (dataframe['phoneLine'] == support_line), 'duration']
        call_datetime = dataframe.loc[(dataframe['callCount'] == call) & (dataframe['phoneLine'] == 33458005730), 'datetime']
        calling_number = dataframe.loc[(dataframe['callCount'] == call) & (dataframe['phoneLine'] == 33458005730), 'callingNumber']
        customer_count = dataframe.loc[(dataframe['callCount'] == call) & (dataframe['phoneLine'] == 33458005730), 'customerCount']

        call_datetime = call_datetime.values[0] if not call_datetime.empty else None
        calling_number = calling_number.values[0] if not calling_number.empty else None
        customer_count = customer_count.values[0] if not customer_count.empty else None
        
        waiting_before_answer = (total_duration_data.values - max(call_duration_data.values))[0] if \
            not call_duration_data.empty and not total_duration_data.empty else None
        total_duration = total_duration_data.values[0] if not total_duration_data.empty else None
        is_answered = True if (not call_duration_data.empty) and (call_duration_data.values[-1] != 0) \
            else False if (not call_duration_data.empty) and (call_duration_data.values[-1] == 0) \
            and (waiting_before_answer != None)\
            else 'not reached'
        call_duration = call_duration_data.values if not call_duration_data.empty else None
        
        output_data = {
            'callingNumber': calling_number,
            'customer_waiting_time': total_duration,
            'is_answered' : is_answered,
            'call_duration' : call_duration,
            'waiting_before_answer' : waiting_before_answer,
            'datetime': call_datetime,
            'call_count': call,
            'customer_count': customer_count,
            }

        # output_dataframe = pd.read_json(json.dumps(output_data, cls=nEncoder), orient='index')
        output_dataframe.loc[call] = pd.Series(output_data)

    return output_dataframe


def calculate_answer_rate(data):
    '''Calculate the answered and missed calls'''
    total_calls = len(data)
    answered_count = 0

    for call, data in data.items():
        if data['is_answered']:
            answered_count += 1

    answer_rate = answered_count / total_calls
    
    return total_calls, answer_rate


def pretty_print(data):
    '''Display json file with style'''
    print(json.dumps(data, indent=4, cls=nEncoder))


def print_df_by_call(dataframe, nb_call, start):
    '''Display processed dataframe splitted by call count'''
    for call in range(nb_call, start):
        print(dataframe[dataframe['callCount'] == call], '\n')


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


def main():
    output_json_path = 'recap.json'
    output_json_path2 = 'recap2.json'

    # for month in tqdm(date):
    month = "2022-06-01"

    file_path = f'data_{month}_ovh'
    data, is_empty = read_csv_file('files/raw_files/' + file_path)
    month_date = "-".join(month.split("-")[:2])

    if not is_empty:
        data_ordered = process_data(data)

        output_path = f'files/ordered_files/{file_path}_ordered'
        save_df_file(data_ordered, output_path)

        dataframe_ordered, is_empty = read_csv_file(output_path)

        output_dataframe = analyse_data(dataframe_ordered)
        save_df_file(output_dataframe, f'files/analysed_files/{file_path}_analysed')

        call_count_data = output_dataframe.is_answered.value_counts().to_dict()
        call_answered = call_count_data[True]
        call_missed = call_count_data[False] if False in call_count_data else 0
        total_calls = call_answered + call_missed

        monthly_recap = {
            f'{month_date}': {
                'data': call_count_data,
                'rate': call_answered / total_calls
            }
        }

    add_json_data(monthly_recap, output_json_path)

if __name__ == '__main__':
    main()
    # df, is_empty = read_csv_file('files/ordered_files/data_2022-05-01_ovh_ordered')
    # print_df_by_call(df, 10, 100)