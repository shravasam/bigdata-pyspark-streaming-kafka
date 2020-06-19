#this is modified
spark = SparkSession. \
  builder. \
  master('local'). \
  appName('CSV Example'). \
  getOrCreate()

emodnet = spark.read. \
  format('csv'). \
  schema('getAllLatestData60Days int, platformID int, latestPlatformID int, date date, depth int'). \
  load('/Users/sinay/Research/data/emodnet_db/marie_table')

emodnet.printSchema()
emodnet.show()

getAllLatestData60Days = spark.read. \
  format('csv'). \
  schema('''getAllLatestData90Days_id int, 
            getAllLatestData90Days_id_platformID_id int, 
            getAllLatestData90Days_id_latestPlatformID_id int, 
            getAllLatestData90Days_id_date date,
            getAllLatestData90Days_id_depth_id int,
           
         '''). \
  load('/Users/sinay/Research/data/emodnet_db/AllLatest_table')

getAllLatestData60Days.printSchema()
getAllLatestData60Days.show()
