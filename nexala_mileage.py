from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import math
from geospatial import haversine, vincenty, speedFunc

conf = SparkConf().setAppName('SWTrains').setMaster('yarn-client').set("spark.executor.memory", "5g").set("spark.executor.instances", "15").set("spark.executor.cores", "5")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

#channel = sqlCtx.read.json('s3n://dt1-analysis-data/swt/dbo.ChannelValue.Table.3.json')
#channel.show()
#channel.select('*').write.save('s3://dt1-analysis-results/ajarman/data/swtrains/channel.parquet', format='parquet', mode='overwrite')
channel = sqlCtx.read.parquet('s3://dt1-analysis-results/ajarman/data/swtrains/channel.parquet')
resource = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', delimiter='¦').load('s3://dt1-analysis-data/swt/genius/RESOURCES_DATA_TABLE.dsv')
fleet = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', delimiter='¦').load('s3://dt1-analysis-data/swt/genius/MILEAGE_SUMMARY_REPORT_DATA_TABLE.dsv')
fleetRes = resource.join(fleet, resource.RESGRP == fleet.RESGRP).select(fleet.FLEETID.alias('FleetID'), resource.RESGRP.alias('ResGrp'), resource.RES)
vehicle = sqlCtx.read.json('s3n://dt1-analysis-data/swt/dbo._done_org_Vehicle.Table.csv.json')
vehicleFull = vehicle.join(fleetRes, vehicle.VehicleNumber == fleetRes.RES).select(vehicle.ID,vehicle.VehicleNumber.cast('int').alias('VehicleNumber'), fleetRes.ResGrp, fleetRes.FleetID)

channel1 = channel.select(channel.VehicleID, (channel.Col3).alias('Latitude'), (channel.Col4).alias('Longitude'), date_format('TimeStamp', 'dd/MM/yyyy HH:mm:ss:SSSSSS').alias('TimeStamp'), (channel.Col6).alias('Speed')).orderBy('TimeStamp')
channel2 = channel1.join(vehicleFull, channel1.VehicleID == vehicleFull.ID).select(channel1.Latitude, channel1.Longitude, channel1.TimeStamp, channel1.Speed, vehicleFull.VehicleNumber, vehicleFull.ResGrp, vehicleFull.FleetID).where(channel1.Latitude <> 0).withColumn('TS_round',((round(unix_timestamp(channel1.TimeStamp, format='dd/MM/yyyy HH:mm:ss:SSSSSS') / 60) * 60).cast('timestamp'))).orderBy('TimeStamp')

# selects one unqiue latitude entry to remove duplicates
vehicleLatMin = channel2.groupBy([channel2.VehicleNumber.alias('VN'), channel2.TS_round]).min('Latitude').withColumnRenamed('min(Latitude)','Min_Latitude').where(channel2.TS_round.substr(0,10) == '2015-12-10').orderBy(channel2.TS_round).withColumnRenamed('TS_round','TS_rnd')

channelFull = channel2.join(vehicleLatMin, [channel2.VehicleNumber==vehicleLatMin.VN, channel2.TS_round==vehicleLatMin.TS_rnd, channel2.Latitude==vehicleLatMin.Min_Latitude]).select(channel2.VehicleNumber, channel2.ResGrp, channel2.FleetID, channel2.Latitude, channel2.Longitude, unix_timestamp(channel2.TimeStamp, format='dd/MM/yyyy HH:mm:ss:SSSSSS').alias('UnixTS'), channel2.TS_round, channel2.Speed).where(channel2.TS_round.substr(0,10) == '2015-12-10').orderBy(channel2.TS_round)

channelFull.select('*').write.save('s3://dt1-analysis-results/ajarman/data/swtrains/channelFull.parquet', format='parquet', mode='overwrite')
channelFull = sqlCtx.read.parquet('s3://dt1-analysis-results/ajarman/data/swtrains/channelFull.parquet')

vehicle_count = channelFull.groupBy(channelFull.VehicleNumber.alias('VN')).count().orderBy('count',ascending=False)
vehicle_list = vehicle_count.join(vehicleFull, vehicle_count.VN == vehicleFull.VehicleNumber, 'left_outer').select(vehicleFull.VehicleNumber, vehicleFull.ResGrp).where(vehicleFull.ResGrp >= 159000).where(vehicleFull.ResGrp <= 159022).distinct().orderBy('VehicleNumber')
#vehicle_list.select('*').write.save('s3://dt1-analysis-results/ajarman/data/swtrains/vehicle_list.parquet', format='parquet', mode='overwrite')
#vehicle_list = sqlCtx.read.parquet('s3://dt1-analysis-results/ajarman/data/swtrains/vehicle_list.parquet')
vehicleList = [x[0] for x in vehicle_list.collect()]
channelSel = channelFull.rdd.filter(lambda r:r[0] in vehicleList).toDF().orderBy('VehicleNumber').drop('FleetID')
row_with_index = Row('VehicleNumber','ResGrp','Latitude','Longitude','UnixTS','TS_round','Speed','index')

indexed = channelSel.rdd.zipWithIndex().map(lambda ri: row_with_index(*list(ri[0]) + [ri[1]])).toDF()
shifted = indexed.map(lambda r : (r.VehicleNumber, r.Latitude, r.Longitude, r.UnixTS, r.TS_round, r[7]-1)).toDF().withColumnRenamed('_1', 'VN').withColumnRenamed('_2', 'Latitude_lag').withColumnRenamed('_3', 'Longitude_lag').withColumnRenamed('_4', 'UnixTS_lag').withColumnRenamed('_5', 'TS_round_lag').withColumnRenamed('_6', 'index_lag')
joined = indexed.join(shifted, [indexed.VehicleNumber==shifted.VN, indexed.index==shifted.index_lag]).orderBy('index').drop('min_latitude').drop('VN')
distance = joined.map(haversine).toDF().withColumnRenamed('_1','ind').withColumnRenamed('_2','Distance')
vehicleDistance = joined.join(distance, joined.index==distance.ind).orderBy('index').drop('ind')
speed = vehicleDistance.map(speedFunc).toDF().withColumnRenamed('_1','ind').withColumnRenamed('_2','Speed_calc')
vehicleDistanceSpeed = vehicleDistance.join(speed, vehicleDistance.index == speed.ind).orderBy('index').where(distance.Distance <> 0).drop('ind')
vehicleDistanceSpeed.groupBy('VehicleNumber').sum('Distance').show()
vehicleDistanceSpeed.repartition(1).write.mode('overwrite').save('s3://dt1-analysis-results/ajarman/data/swtrains/vehicleAllMileage.csv', format= 'com.databricks.spark.csv', header='true')
