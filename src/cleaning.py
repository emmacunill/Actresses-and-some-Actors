import pandas as pd
import numpy as np
import pymysql
import sqlalchemy as alch
import os
from dotenv import load_dotenv
from getpass import getpass

#imports

imdb = pd.read_csv("../posibles_datasets/IMDB-Movie-Data.csv")
best_act = pd.read_csv("../posibles_datasets/Top 100 Greatest Hollywood Actors of All Time.csv")
amazon = pd.read_csv("../posibles_datasets/amazon_prime_titles.csv")
award = pd.read_csv("../posibles_datasets/database.csv")
netflix = pd.read_csv("../posibles_datasets/netflix_titles.csv")

#functions


def lists_into_rows(df,columname):

    ''' Function to create a row with duplicate information for every item on a list in every row for a column.
    it takes 2 args:
    - dataframe
    - column name.
    '''

    df[columname] = df[columname].str.split(', ')
    df_expanded = df.explode(columname)
    df_expanded.reset_index(drop=True, inplace=True)
    return df_expanded


def lists_into_columns(df, column_name,split, name1, name2, name3):
    ''' 
    Function to separate a list from every row on a column 
    and put each element into a new column. Takes 6 args:
    - df
    - column name
    - How do you want to split(", " or just ",")
    - new columns names
    '''

    df_split = df[column_name].str.split(split,n=2,expand=True)
    df_split.columns = [name1, name2, name3]
    df_split = df_split.fillna('')
    df_expanded = pd.concat([df, df_split], axis=1)

    return df_expanded



def get_amazon_df(dataframe, column_name1, column_name2, split, name1, name2, name3):

    dataframe = lists_into_rows(dataframe,column_name1)
    dataframe = lists_into_columns(dataframe, column_name2,split, name1, name2, name3)
    dataframe.drop([name3], axis=1, inplace=True)
    return dataframe



def filtered_data_4(df):

    '''Function to filter and clean df 4. It filters the column award and substitutes the Nan with 0.
    Takes 1 arg:
    - df
    '''

    df_7 = df[df["Award"]== "Actress"]
    df_1 = df[df["Award"]=="Actor"]
    df_2 = df[df["Award"]=="Actor in a Supporting Role"]
    df_3 = df[df["Award"]=="Actress in a Supporting Role"]
    df_4 = df[df["Award"]=="Actor in a Leading Role"]
    df_5 = df[df["Award"]=="Actress in a Leading Role"]
    df_6 = pd.concat([df_7, df_1, df_2, df_3, df_4, df_5], ignore_index=True)
    df_6['Winner'].fillna(0, inplace=True)
    
    return df_6



def connection_sql():
    '''Function to connect to sql and upload all the datasets'''

    password = getpass()
    dbName = "actors_actresses"
    connectionData=f"mysql+pymysql://root:{password}@localhost/{dbName}"
    engine = alch.create_engine(connectionData)

    datasets = [imdb, best_act, amazon, award, netflix]
    names = ["imdb", "best_act", "amazon", "award", "netflix"]

    for df,name in zip(datasets,names):
        df.to_sql(name, if_exists="append", con=engine, index=False)



