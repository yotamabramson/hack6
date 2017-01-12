from nltk.corpus import stopwords
import bigquery_io.bigquery_wrapper as qbq
import time
from bigquery_io.gs_io import read_gs_folder
import numpy as np
import pandas as pd


word_query = """
select timeframe, word, content_category, sum(revenue) revenue, sum(click) clicks, sum(view) views
from   (
SELECT
  pv.itemId,
  timeframe,
  split(tsv.title," ") word,
  spc.content_category content_category,
  revenue,
  click,
  view,
FROM
  FLATTEN(FLATTEN((
      SELECT
        hour(msec_to_timestamp(pv.pageViewStartTime)) as timeframe,
        pv.requests.servedItems.itemId itemId,
        pv.requests.servedItems.campaignId campaignId,
        pv.requests.servedItems.revneue revenue,
        pv.requests.servedItems.clicked click,
        1 view
      FROM
      TABLE_DATE_RANGE([taboola_taz.pageviews_], TIMESTAMP('2017-01-04'), TIMESTAMP('2017-01-09'))
      WHERE
        pv.requests.servedItems.itemId IS NOT NULL
        and pv.publisherId = 1040522
        ),pv.requests.servedItems),pv.requests) AS pv
INNER JOIN (
SELECT id, MAX(title) title FROM [trc.videos] GROUP BY id
) tsv
ON
  tsv.id = pv.itemId
inner join (select id, content_category from trc.sp_campaigns_latest_snapshot_all) spc
on spc.id = pv.campaignId
  ) w2stats
where REGEXP_MATCH(word, '^[a-zA-Z]+$')
and content_category != "NULL"
group by timeframe, word,content_category
order by timeframe,views desc
"""


dataset_name = 'Rotman'
destination_folder = 'gs://rotman/hackathon_data_vis'
table_name = 'words_bubbles_%s' % abs(np.abs(hash(word_query)) + int(time.time()))
bq_wrapper = qbq.BigQueryWrapper()

start_time = time.time()
params = bq_wrapper.execute_wait(word_query, '', full_table_name=table_name,destination_dataset=dataset_name)
if params is not None:
    gs_params = bq_wrapper.export_table_and_wait(params['table_name'], table_name, compress=True, destination_folder=destination_folder)


# get data from GS to dataframe
output_path = gs_params['destination_path'].replace('/*','/')
lines = list(read_gs_folder(output_path, gzipped_content=True))
filename = "".join(['dataframe_', str(int(time.time())) , '.csv'])
with open(filename,'wb') as f:
   for line in lines:
       f.write(line)
   f.seek(0)
df = pd.read_csv(open(filename, 'rU'), encoding='utf-8', engine='c')
df = df.convert_objects(convert_numeric=True)

stop = set(stopwords.words('english'))
df_nostop = df[~df['word'].str.lower().isin(stop)]
df_nostop['word'] = df_nostop['word'].str.capitalize()
df_nostop['cat_word'] = df_nostop.content_category + '.' + df_nostop.word

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