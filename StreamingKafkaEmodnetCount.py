#Importer les packages nécessaires
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys

#Créer un ensemble pour une liste de sujets et dict pour la liste des courtiers Kafka et autres paramètres
conf = SparkConf().setAppName("Streaming platform Count").setMaster("yarn-client")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

#Créez un flux d'entrée à l'aide des API pertinentes en passant la liste des sujets et propriétés du courtier
topics = ["emodnet"]
brokerList = {"metadata.broker.list" : "nn01.platformid,nn02.platformid,rm01.platformid"}
directKafkaStream = KafkaUtils.createDirectStream(ssc, topics, brokerList)

#Traitez les données à l'aide des API DStream pertinentes
#Télécharger les dépendances sur le nœud de passerelle
#Expédiez le code et exécutez-le sur le cluster, y compris les dépendances Kafka
messages = directKafkaStream.map(lambda msg: msg[1])
emodnetMessages = messages.filter(lambda msg: msg.split(" ")[6].split("/")[1] == "platform")
emodnetNames = emodnetMessages.map(lambda msg: (msg.split(" ")[6].split("/")[2], 1))
from operator import add
emodnetCount = emodnetNames.reduceByKey(add)

outputPrefix = sys.argv[1]
emodnetCount.saveAsTextFiles(outputPrefix)

ssc.start()
ssc.awaitTermination() 
