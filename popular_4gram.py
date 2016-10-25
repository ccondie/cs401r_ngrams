import sys
from operator import add
from pyspark import SparkContext
from pyspark import SparkConf
import logging
import re
import os


if __name__ == '__main__':
    # collect input parameters
    inputPath = sys.argv[1]
    year_to_search = sys.argv[2]
    outputFile = sys.argv[3]

    # start SparkContext
    conf = SparkConf().setAppName('popular_4gram')
    sc = SparkContext(conf=conf)

    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('***** PYSPARK SCRIPT LOGGER INITIALIZED')

    # read input
    LOGGER.info('***** READING LZO HADOOP FILE')
    # LZO indexed by row i.e. <1:ngram data, 2: ngram data, 3: ngram data>
    files = sc.sequenceFile(inputPath, "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")
    lzoRDD = files.map(lambda x: x[1])

    # # map to 3-tuples of (ngram, year, count)
    LOGGER.info('***** SPLITTING LZO INPUT')
    # allEntries = lzoRDD.map(lambda x: x.split())
    allEntries = lzoRDD.map(lambda x: re.split(r'\t+',x))

    LOGGER.info('***** GENERATING 3-TUPLES')
    # 1gram: x[0]:word - x[1]:year - x[2]:count - x[3]:pages - x[4]:books
    # 4gram: x[0]:word1 - x[1]:word2 - x[2]:word3 - x[3]:word4 - x[4]:year - x[5]:count - x[6]:pages - x[7]:books
    # 4gram: x[0]:4gram - x[1]:year - x[2]:occurrences
    formattedEntries = allEntries.map(lambda x: (x[0], x[1], x[2]))
    # formattedEntries - "word word word word", 1905, 54
    # formattedEntries = allEntries.map(lambda x: x[1])

    LOGGER.info('***** FILTERING ENTRIES TO INPUTTED YEAR')
    filteredEntries = formattedEntries.filter(lambda x: x[1] == year_to_search)
    # filteredEntries = formattedEntries.filter(lambda x: x == year_to_search)

    LOGGER.info('***** SORT BY OCCURRENCES')
    sortedEntries = filteredEntries.sortBy(lambda a: a[2])

    LOGGER.info('***** COLLECT AND WRITE TO FILE')
    sortedEntries.collect()
    sortedEntries.saveAsTextFile(outputFile)

    sc.stop()
    sys.exit(0)
