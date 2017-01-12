import bigquery_io.bigquery_wrapper as qbq
import time
from bigquery_io.gs_io import read_gs_folder
import numpy as np
import pandas as pd


geo_query = """
select timeframe, country, city, sum(click) clicks, sum(view) views
from   (
SELECT
  timeframe,
  country,
  city,
  click,
  view
FROM
  FLATTEN(FLATTEN((
      SELECT
        hour(msec_to_timestamp(pv.pageViewStartTime)) as timeframe,
        pv.country country,
        pv.city city,
        pv.requests.servedItems.clicked click,
        1 view
      FROM
      TABLE_DATE_RANGE([taboola_taz.percentile_pageviews_], TIMESTAMP('2017-01-04'), TIMESTAMP('2017-01-09'))
      WHERE
         pv.publisherId = 1040522
        ),pv.requests.servedItems),pv.requests) AS pv
  ) w2stats
  where city is not null
group by timeframe, country,city
order by timeframe,views desc
"""


dataset_name = 'Rotman'
destination_folder = 'gs://rotman/hackathon_data_vis'
table_name = 'words_bubbles_%s' % abs(np.abs(hash(geo_query)) + int(time.time()))
bq_wrapper = qbq.BigQueryWrapper()

start_time = time.time()
params = bq_wrapper.execute_wait(geo_query, '', full_table_name=table_name, destination_dataset=dataset_name)
if params is not None:
    gs_params = bq_wrapper.export_table_and_wait(params['table_name'], table_name, compress=True, destination_folder=destination_folder)


# get data from GS to dataframe
output_path = gs_params['destination_path'].replace('/*','/')
lines = list(read_gs_folder(output_path, gzipped_content=True))
filename = "".join(['geo_dataframe_', str(int(time.time())) , '.csv'])
with open(filename,'wb') as f:
   for line in lines:
       f.write(line)
   f.seek(0)
df = pd.read_csv(open(filename, 'rU'), encoding='utf-8', engine='c')
df = df.convert_objects(convert_numeric=True)

print df.head(15)