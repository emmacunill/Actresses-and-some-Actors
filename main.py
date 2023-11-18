import pandas as pd
import numpy as np
import pymysql
import sqlalchemy as alch
import os
from dotenv import load_dotenv
from getpass import getpass


#imports

import src.cleaning as clean


imdb = pd.read_csv("posibles_datasets/IMDB-Movie-Data.csv")
best_act = pd.read_csv("posibles_datasets/Top 100 Greatest Hollywood Actors of All Time.csv")
amazon = pd.read_csv("posibles_datasets/amazon_prime_titles.csv")
award = pd.read_csv("posibles_datasets/database.csv")
netflix = pd.read_csv("posibles_datasets/netflix_titles.csv")


#cleaning

imdb = clean.lists_into_rows(imdb, "Actors")
imdb = clean.lists_into_columns(imdb, "Genre",",", "Genre_1", "Genre_2", "Genre_3")

best_act = clean.lists_into_rows(best_act,"Greatest Performances")
best_act = clean.lists_into_columns(best_act, "Place of Birth",", ","Neighborhood", "City", "Country" )

amazon = clean.get_amazon_df(amazon,"cast","listed_in",", ","genre_1","genre_2","genre_3")

award = clean.filtered_data_4(award)

netflix = clean.lists_into_columns(netflix, "listed_in",",","genre_1", "genre_2", "genre_3")
netflix = clean.lists_into_rows(netflix, "cast")


clean.connection_sql()