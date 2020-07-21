from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

import sys

hostname = sys.argv[1]
port = int(sys.argv[2])

conf = SparkConf().setAppName("Streaming emodnet Count").setMaster("yarn-client")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

agents = [(hostname, port)]
pollingStream = FlumeUtils.createPollingStream(ssc, agents)
messages = pollingStream.map(lambda msg: msg[1])
xxxMessages = messages.filter(lambda msg: msg.split(" ")[6].split("/")[1] == "xxx")
xxxNames = xxxMessages.map(lambda msg: (msg.split(" ")[6].split("/")[2], 1))

from operator import add
xxxCount = xxxNames.reduceByKey(add)

outputPrefix = sys.argv[3]
xxxCount.saveAsTextFiles(outputPrefix)

ssc.start()
ssc.awaitTermination()
