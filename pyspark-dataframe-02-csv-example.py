# In case you are using pycharm, first you need to create object of type SparkSession
#Déployer dans la console ou créer un objet 'SparkSession' dans IDE pycharm

spark = SparkSession. \
  builder. \
  master('local'). \
  appName('CSV Example'). \
  getOrCreate()

xxxCSV = spark.read.csv('/Users/xxx/Research/data/emodnet_db/marie_table').toDF('getAllLatestData60Days int, platformID int, latestPlatformID int, date date, depth int')

getAllLatestData60DaysCSV = spark.read.csv('/Users/xxx/Research/data/emodnet_db/AllLatest_table').toDF('getAllLatestData90Days_id int,  getAllLatestData90Days_id_platformID_id int, getAllLatestData90Days_id_latestPlatformID_id int, getAllLatestData90Days_id_date date, getAllLatestData90Days_id_depth_id int,')

from pyspark.sql.types import IntegerType, FloatType

xxx = emodnetCSV. \
  withColumn('getAllLatestData60Days', xxxCSV.getAllLatestData60Days.cast(IntegerType())). \
  withColumn('platformID', xxxCSV.platformID.cast(IntegerType()))

xxx.printSchema()
xxx.show()

getAllLatestData60Days = getAllLatestData60DaysCSV.\
    withColumn('getAllLatestData90Days_id', xxxCSV.getAllLatestData90Days_id.cast(IntegerType())). \
    withColumn('getAllLatestData90Days_id_platformID_id', xxxCSV.getAllLatestData90Days_id_platformID_id.cast(IntegerType())). \
    withColumn('getAllLatestData90Days_id_latestPlatformID_id', xxxCSV.getAllLatestData90Days_id_latestPlatformID_id.cast(IntegerType())).withColumn('getAllLatestData90Days_id_date', xxxCSV.getAllLatestData90Days_id_date.cast(IntegerType())). \
    withColumn('getAllLatestData90Days_id_depth_id', xxxCSV.getAllLatestData90Days_id_depth_id.cast(FloatType())). \
    
getAllLatestData60Days.printSchema()
getAllLatestData60Days.show()
