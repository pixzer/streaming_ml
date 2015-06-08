#!/usr/bin/env python

from lightning import Lightning
from time import sleep
from numpy import random
from cassandra.cluster import Cluster
from scipy.ndimage.filters import gaussian_filter
from pandas import Series

#initialize cassandra connectivity
cluster = Cluster()
cass = cluster.connect()
cass.set_keyspace("sparkml")

#initialize lightning viz. server
lgn = Lightning(host="https://spark-streaming-ml.herokuapp.com")
lgn.create_session('streaming-kmeans')
lgn.session.open()
    
while True:
	accuracy = cass.execute("select unixTimestampOf(event_time) as tm, mse, rmse from accuracy")
	tm = [row.tm for row in accuracy]
	rmse = [row.rmse for row in accuracy]
	obj = Series(rmse, index = tm)
	pts = obj.sort_index(ascending=True).values
	#viz = lgn.line(gaussian_filter(pts, 10))
	viz = lgn.line(pts)
	sleep(0.25)
	
"""rows = cass.execute("select * from predictions")
prediction = [row.prediction for row in rows]
labels = [row.label for row in rows]
viz = lgn.line(prediction, rmse)"""