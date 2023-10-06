import numpy as np
import os
import time
import pickle
from datetime import datetime, timedelta
import logging
import sklearn
from collections import defaultdict

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from baskervillehall.model_io import ModelIO

logger = logging.getLogger('pca')
logger.addHandler(logging.StreamHandler())
logger.setLevel('DEBUG')

s3_connection = {
    's3_access':os.environ['S3_ACCESS'],
    's3_secret':os.environ['S3_SECRET'],
    's3_endpoint': os.environ['S3_ENDPOINT'],
    's3_region':os.environ['S3_REGION']
}

model_io = ModelIO(**s3_connection, logger=logger)

model = model_io.load('s3://anton/baslervillehall/models3', 'verafiles.org')

duration = 60
hit_rate = 2
country = 'US'
url = '/'
session = {'duration': duration, 'country': country}
requests = []
num_hits = int(duration * hit_rate / 60)
ts = datetime.now()
time_increment = 60.0 / hit_rate
countries = []
for i in range(num_hits):
    requests.append({'ts': ts, 'url': url, 'query': '', 'code': 400, 'type': 'text/html', 'payload': 1000})
    ts += timedelta(seconds=time_increment)
    countries.append(country)

session['requests'] = requests
feature_map = BaskervillehallIsolationForest.calculate_features(session)

feature_map = {"request_rate": 49.35483870967742, "request_interval_average": 1.24, "request_interval_std": 1.2257242756835651, "response4xx_to_request_ratio": 0.0, "response5xx_to_request_ratio": 0.0, "top_page_to_request_ratio": 1.0, "unique_path_rate": 0.967741935483871, "unique_path_to_request_ratio": 0.0196078431372549, "unique_query_rate": 0.967741935483871, "unique_query_to_unique_path_ratio": 1.0, "image_to_html_ratio": 0.0, "js_to_html_ratio": 0.0, "css_to_html_ratio": 0.0, "path_depth_average": 1.0, "path_depth_std": 0.0, "payload_size_log_average": 10.816191860874861}
vector = BaskervillehallIsolationForest.get_vector_from_feature_map(model.feature_names,
                                                                    feature_map)

score = model.score(np.array([vector]), [[country]])

print(score)

import pdb
pdb.set_trace()