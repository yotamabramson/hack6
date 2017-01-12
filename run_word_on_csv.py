import pandas as pd
import nltk
from nltk.corpus import stopwords

filename = 'dataframe_1484212295.csv'

df = pd.read_csv(open(filename, 'rU'), encoding='utf-8', engine='c')

stop = set(stopwords.words('english'))
df_nostop = df[~df['word'].str.lower().isin(stop)]
df_nostop['word'] = df_nostop['word'].str.capitalize()
df_nostop['cat_word'] = df_nostop.content_category + "." +  df_nostop.word

for timeframe in df['timeframe'].unique():
    with open("".join(['words_',str(timeframe),".csv"]), 'wb') as output:
        timeframe_data = df_nostop[(df.timeframe == timeframe)].copy(deep=True)
        timeframe_data.sort_values(['clicks'], inplace=True, ascending = False)
        df_top = timeframe_data.groupby(['content_category'],as_index=False)
        df_top = df_top.apply(lambda g: g.sort_index(by='clicks', ascending=False).head(10))
        idx = df_top.groupby("content_category").apply(lambda df_top: df_top.clicks.idxmin())
        # mins = df_top.loc[df_top.groupby("content_category")["clicks"].apply(idxmin)]
        mins = df_top.ix[idx, :]
        mins.sort_values(['clicks'], inplace=True, ascending = False)
        min_cats = mins.content_category.head(5)

        df_top = df_top[df_top['content_category'].isin(min_cats)]

        for category in df_top['content_category'].unique():
            category_dict = {"cat_word" : category}
            df_top = df_top.append(category_dict, ignore_index = True)
        df_top.sort_values(['cat_word'], inplace=True)
        print df_top.head(100)

        df_top = df_top.rename(columns={'cat_word': 'id', 'clicks': 'value'})
        df_top.to_csv(output,columns = ['id', 'value'], index = False)