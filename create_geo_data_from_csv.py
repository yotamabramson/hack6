import numpy as np
import pandas as pd

filename = "geo_dataframe_1484216103.csv"

geo_df = pd.read_csv(open(filename, 'rU'), encoding='utf-8', engine='c')
geo_df = geo_df.convert_objects(convert_numeric=True)

cities_file = 'cities3.csv'
city_df = pd.read_csv(cities_file)
city_df = city_df.convert_objects(convert_numeric=True)
geo_df['country'] = geo_df['country'].str.lower().str.title()
geo_df['country'][geo_df['country'] == "United States"] = 'United States of America'
geo_df['country'][geo_df['country'] == "Korea, Republic Of"] = 'South Korea'
geo_df['country'][geo_df['country'] == "Hong Kong"] = 'Hong Kong S.A.R.'
geo_df['country'][geo_df['country'] == "Russian Federation"] = 'Russia'
geo_df['country'][geo_df['country'] == "Trinidad And Tobago"] = 'Trinidad and Tobago'
geo_df['country'][geo_df['country'] == "Bosnia And Herzegovina"] = 'Bosnia and Herzegovina'
geo_df['country'][geo_df['country'] == "Macau"] = 'Macau S.A.R'
geo_df['country'][geo_df['country'] == "Tanzania, United Republic Of"] = 'Moldova'
geo_df['country'][geo_df['country'] == "Moldova, Republic Of"] = 'Moldova'
geo_df['country'][geo_df['country'] == "France, Metropolitan"] = 'France'
geo_df['country'][geo_df['country'] == "Iran, Islamic Republic Of"] = 'Iran'
geo_df['country'][geo_df['country'] == "Cote D'Ivoire "] = 'Ivory Coast'

uni_df = geo_df.merge(city_df, on=['country','city'], how='inner')
sum_clicks = 
uni_df[cols_to_norm] = uni_df[cols_to_norm].apply(lambda x: (x - x.mean()) / (x.max() - x.min()))

for timeframe in uni_df['timeframe'].unique():
    with open("".join(['data/cities_',str(timeframe),".csv"]), 'wb') as output:
       timeframe_data = uni_df[(uni_df.timeframe == timeframe)].copy(deep=True)
       print timeframe_data.head(10)
       timeframe_data.to_csv(output, columns=['country', 'city','clicks','views','lat','lng'], index=False)


