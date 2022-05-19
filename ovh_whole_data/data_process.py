import pandas as pd
import numpy as np
import json


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
    return data


def read_json_file(filepath):
    '''Read json file and return it as a dictionnary'''
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data


def change_type(dataframe):
    '''Set the variable type for the following columns : phoneLine, callingNumber, nature, datetime'''
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
    dataframe['date'] = pd.to_datetime(dataframe['date'], format='%Y%m%d')
    dataframe['time'] = pd.to_datetime(dataframe['time'], format='%H%M%S').dt.time
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
        json_string = json.dumps(dataframe, cls=nEncoder)
        json.dump(dataframe, file, cls=nEncoder)


def analyse_data(dataframe):
    '''Create a dictionnary containing all calls and their attributes : total duration of the call, call d'''
    output_data = {}
    total_calls = dataframe.callCount.max()

    for call in range(1, total_calls + 1):
        total_duration_data = dataframe.loc[(dataframe['callCount'] == call) & (dataframe['phoneLine'] == 33458005730), 'duration']
        call_duration_data = dataframe.loc[(dataframe['callCount'] == call) & (dataframe['phoneLine'] == 33458005709), 'duration']
        
        total_duration = total_duration_data.values[0] if not total_duration_data.empty else 'empty'
        is_answered = True if (not call_duration_data.empty) and (call_duration_data.values[-1] != 0) \
            else False if (not call_duration_data.empty) and (call_duration_data.values[-1] == 0) \
            else 'not reached'
        call_duration = call_duration_data.values if not call_duration_data.empty else 'empty'
        waiting_before_answer = (total_duration_data.values - max(call_duration_data.values))[0] if \
            not call_duration_data.empty and not total_duration_data.empty else 'empty'

        output_data[f'call{call}'] = {
            'customer_waiting_time': total_duration,
            'is_answered' : is_answered,
            'call_duration' : call_duration,
            'waiting_before_answer' : waiting_before_answer,
            }
    return output_data


def calculate_answer_rate(data):
    total_calls = len(data)
    answered_count = 0

    for call, data in data.items():
        if data['is_answered']:
            answered_count += 1

    answer_rate = answered_count / total_calls
    
    return total_calls, answer_rate


def pretty_print(data):
    print(json.dumps(data, indent=4, cls=nEncoder))


def print_df_by_call(dataframe, nb_call):
    for call in range(nb_call):
        print(dataframe[dataframe['callCount'] == call], '\n')


def main():
    input_path = 'data_2022-05-01_ovh'
    output_path = 'data_2022-05-01_ovh_ordered'
    output_json_path = 'test_file.json'

    # data = read_csv_file(input_path)
    # data_ordered = process_data(data)
    # save_df_file(data_ordered, output_path)

    data = read_csv_file('data_2022-05-01_ovh_ordered')
    output_data = analyse_data(data)

    # pretty_print(output_data)
    save_json_file(output_data, output_json_path)

    json_data = read_json_file(output_json_path)
    total_calls, answer_rate = calculate_answer_rate(json_data)

    print(total_calls, answer_rate)
    # data = read_csv_file(output_path)
    # print_df_by_call(data, 15)

if __name__ == '__main__':
    main()