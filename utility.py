# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 14:46:06 2024

@author: MilanGowdaJP
"""
import os

import numpy as np
import pandas as pd


def make_input_data_continous(input_data, current_date):
    input_data_c = pd.DataFrame()
    # for i in ['Dairy']:
    for i in input_data['H1'].unique():
        for j in input_data['CHNL_NAME'].unique():
            print(i, j)
            c = input_data['H1'] == i
            c1 = input_data['CHNL_NAME'] == j
            df = input_data[c & c1]
            df_ = make_data_continous(df, 'MATERIAL', current_date, 7)
            df_['H1'] = i
            df_['CHNL_NAME'] = j
            input_data_c = pd.concat([input_data_c, df_], axis=0)
    return input_data_c


def collect_data(path, files, col=None):
    d = pd.DataFrame()
    for i in files:
        file_path = path + i
        print(file_path)
        data = pd.read_parquet(file_path, columns=col)
        print(data.shape)
        d = pd.concat([d, data], axis=0)
    return d.reset_index(drop=True)


def generate_condition(df, col_name, col_value):
    try:
        if col_value.lower() == "all":
            return pd.Series(df.shape[0] * [True])
        else:
            return df[col_name] == col_value
    except:
        print(col_name,col_value,'FROM EXCEPTION')

def make_data_continous(df, groupby_col, current_date, max_horizon):
    df['date'] = pd.to_datetime(df['year_month'] + "-01")
    data_ = pd.DataFrame()
    for sku_ in df[groupby_col].unique():
        c = df[groupby_col] == sku_
        temp_ = df[c]
        min_date = temp_['date'].min() - pd.DateOffset(months=1)
        max_date = current_date + pd.DateOffset(months=max_horizon)
        data = pd.DataFrame()
        data["date"] = pd.Series(pd.date_range(min_date, max_date, freq="M")) + pd.DateOffset(day=1)
        data = pd.merge(data, temp_, on=['date'], how='left')
        data[groupby_col] = sku_
        data['year_month'] = data['date'].astype(str).str[:-3]
        data['QTY_BASEUOM_SUM'].fillna(0, inplace=True)
        data['SALES_SUM'].fillna(0, inplace=True)
        data_ = pd.concat([data_, data], axis=0)
    return data_


# OLD DATA


def create_file_not_exists(file_path, relative_path=True):
    paths = file_path.split("/")[1:]
    if relative_path:
        path_ = './'
    else:
        path_ = ''
    for path in paths:
        path_ += '/'
        path_ += path
        if not os.path.exists(path_):
            os.mkdir(path_)


def make_data_continous(df, group_by_col, current_date, max_horizon):
    df['date'] = pd.to_datetime(df['year_month'] + "-01")
    data_ = pd.DataFrame()
    for sku_ in df[group_by_col].unique():
        c = df[group_by_col] == sku_
        temp_ = df[c]
        min_date = temp_['date'].min() - pd.DateOffset(months=1)
        max_date = current_date + pd.DateOffset(months=max_horizon)
        data = pd.DataFrame()
        data["date"] = pd.Series(pd.date_range(min_date, max_date, freq="M")) + pd.DateOffset(day=1)
        data = pd.merge(data, temp_, on=['date'], how='left')
        data[group_by_col] = sku_
        data['year_month'] = data['date'].astype(str).str[:-3]
        data['QTY_BASEUOM_SUM'].fillna(0, inplace=True)
        data['SALES_SUM'].fillna(0, inplace=True)
        data_ = pd.concat([data_, data], axis=0)
    return data_

def make_data_continous_(df, group_by_col, current_date, max_horizon):
    df['date'] = pd.to_datetime(df['year_month'] + "-01")
    data_ = pd.DataFrame()
    for sku_ in df[group_by_col].unique():
        c = df[group_by_col] == sku_
        temp_ = df[c]
        min_date = temp_['date'].min() - pd.DateOffset(months=1)
        max_date = current_date + pd.DateOffset(months=max_horizon)
        data = pd.DataFrame()
        data["date"] = pd.Series(pd.date_range(min_date, max_date, freq="M")) + pd.DateOffset(day=1)
        data = pd.merge(data, temp_, on=['date'], how='left')
        data[group_by_col] = sku_
        data['year_month'] = data['date'].astype(str).str[:-3]
        data['TOTAL_QTY_BASEUOM_SUM'].fillna(0, inplace=True)
        # data['SALES_SUM'].fillna(0, inplace=True)
        data_ = pd.concat([data_, data], axis=0)
    return data_

def load_filters_old(limits, cols, data):
    condition_list = []
    choice_list = []
    for j, i in enumerate(limits):
        if j == 0:
            # print( i)
            # choice = f"< SGD {str(int(i/1000))}k"
            choice = f"< SGD {str(int(i))}"
            choice_list.append(choice)
            condi = data[cols] <= i
            condition_list.append(condi)
        else:
            # print(limits[j-1],i)
            # choice = f"SGD {str(int(limits[j - 1]/1000))}K- {str(int(i/1000))}K"
            choice = f"SGD {str(int(limits[j - 1]))} - {str(int(i))}"
            choice_list.append(choice)
            condi = (data[cols] > limits[j - 1]) & (data[cols] <= i)
            condition_list.append(condi)

        if j == len(limits) - 1:
            # print(i)
            # choice = f"> SGD {str(int(i/1000))}K"
            choice = f"> SGD {str(int(i))}"
            choice_list.append(choice)
            condi = (data[cols] > i)
            condition_list.append(condi)

    return np.select(condition_list, choice_list)


def load_filters(limits, cols, data):
    condition_list = []
    choice_list = []
    for j, i in enumerate(limits):
        if j == 0:
            # print( i)
            # choice = f"< SGD {str(int(i/1000))}k"
            choice = f"< SGD {str(int(i))}"
            choice_list.append(choice)
            condi = data[cols] <= i
            condition_list.append(condi)
        else:
            # print(limits[j-1],i)
            # choice = f"SGD {str(int(limits[j - 1]/1000))}K- {str(int(i/1000))}K"
            choice = f"SGD {str(int(limits[j - 1]))} - {str(int(i))}"
            choice_list.append(choice)
            condi = (data[cols] > limits[j - 1]) & (data[cols] <= i)
            condition_list.append(condi)

        if j == len(limits) - 1:
            # print(i)
            # choice = f"> SGD {str(int(i/1000))}K"
            choice = f"> SGD {str(int(i))}"
            choice_list.append(choice)
            condi = (data[cols] > i)
            condition_list.append(condi)

    return np.select(condition_list, choice_list),choice_list


def round_to_100(num):
    return ((num+10)//10)*10