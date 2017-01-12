import csv
import collections
import bigquery_io.bigquery_wrapper as qbq
import time
from bigquery_io.gs_io import read_gs_folder
import numpy as np

categories = {
    "health": 0,
    "life" : 0,
    "entertainment": 1,
    "humor" : 1,
    "finance":2,
    "fashion":3,
    "lifestyle":4,
    "home":5,
    "autos":6,
    "beauty":7,
    "tech":8,
    "sports":9,
    "food":10,
    "pets":11,
    "education":12,
    "travel":13,
    "india" : 13,
    "business":14,
    "family":15,
    "dating":16,
    "environment":17,
    "music":18,
    "fitness":19,
    "news":20,
    "politics":20,
    "opinions":20,
    "alcohol":21,
    "gambling":21,
    "religion":22,
    "investment products":23,
    "other":24,
    "obsolete" : 24,
    "" : 24,
    "null" : 24,
    "high": 0,
    "low": 1,
    "medium": 2,
    "premium": 3,
    "racy": 4,
    "shocking": 5,

};


category_query = """
SELECT
  hour, LOWER(main_var) AS main_var, COUNT(1) AS total
FROM (
  SELECT
    INTEGER(HOUR(MSEC_TO_TIMESTAMP(pv.pageViewStartTime))*10+ FLOOR(MINUTE(MSEC_TO_TIMESTAMP(pv.pageViewStartTime))/6)) AS hour,
    ls.content_category AS main_var
  FROM
    FLATTEN(FLATTEN(( SELECT
    pv.pageViewStartTime, pv.requests.servedItems.campaignId, pv.requests.servedItems.itemProviderType,
    pv.requests.servedItems.clicked
    FROM TABLE_DATE_RANGE([taboola_taz.msn_msn_pageviews_], TIMESTAMP('2017-01-02'), TIMESTAMP('2017-01-05'))), pv.requests),pv.requests.servedItems) ttpv
  INNER JOIN
    trc.sp_campaigns_latest_snapshot ls
  ON
    ls.id=pv.requests.servedItems.campaignId where
    pv.requests.servedItems.itemProviderType == 'SP' AND
    pv.requests.servedItems.clicked AND
    TRUE )
GROUP BY 1,2
ORDER BY 1,2
 """


outfile = "".join(['category_circles_', str(int(time.time())) , '.csv'])

dataset_name = 'Rotman'
destination_folder = 'gs://rotman/hackathon_data_vis'
table_name = 'category_circles_%s' % abs(np.abs(hash(category_query)) + int(time.time()))
bq_wrapper = qbq.BigQueryWrapper()

start_time = time.time()
params = bq_wrapper.execute_wait(category_query, '', full_table_name=table_name,destination_dataset=dataset_name)
if params is not None:
    gs_params = bq_wrapper.export_table_and_wait(params['table_name'], table_name, compress=True, destination_folder=destination_folder)


# get data from GS to dataframe
output_path = gs_params['destination_path'].replace('/*','/')
lines = list(read_gs_folder(output_path, gzipped_content=True))
filename = "".join(['category_dataframe_', str(int(time.time())) , '.csv'])
with open(filename,'wb') as f:
   for line in lines:
       f.write(line)
   f.seek(0)



input_data = collections.defaultdict(dict);
with open(filename, 'rb') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        hour = int(row['hour']);
        #main_var = row['ls_content_category']
        main_var = row['main_var']
        value = int(row['total']);
        input_data[hour][main_var] = value;


# now create the target dict - primary lines are cluster of recs (approx of users), secondary are categories they've been in

output_data = collections.defaultdict(dict);


def substract_1_user(hour):
    for cat in input_data[hour].iterkeys():
        if (input_data[hour][cat] > 0):
            input_data[hour][cat] = input_data[hour][cat]-200
            return cat
        else:
            continue
    return None



def get_user_line():
    user_line = [];
    duration = 0;
    prevCategory = ""
    for hour in range (0, 240):
        duration += 6;
        category = substract_1_user(hour)
        if category is None:
            return []
        if ((prevCategory != "" and category != prevCategory) or hour == 239):
            user_line.append((category, duration))
            duration = 0;
        prevCategory = category
    return user_line


for x in range(0, 1000):
    line = get_user_line()
    if (len(line) > 0):
        output_data[x] = line

print(output_data)

with open(outfile, 'w') as outfile:
    outfile.write("day\n")
    for person in output_data.itervalues():
        outfile.write("\"")
        for i in range(0, len(person)):
            if (i>0):
                outfile.write(",")
            cat_index = categories.get(person[i][0])
            if (cat_index is None):
                print("non existing category:" + person[i][0])
                cat_index = 24
            outfile.write('{0},{1}'.format(cat_index, person[i][1]))
        outfile.write("\"\n");
