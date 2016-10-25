import sys
from operator import add
from pyspark import SparkContext
import logging
import re
import os


if __name__ == '__main__':
    # collect input parameters
    inputPath = sys.argv[1]
    year_to_search = sys.argv[2]
    outputFile = sys.argv[3]

    # start SparkContext
    sc = SparkContext("local", "popular_4gram")

    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('***** PYSPARK SCRIPT LOGGER INITIALIZED')

    # read input
    LOGGER.info('***** READING LZO HADOOP FILE')
    # LZO indexed by row i.e. <1:ngram data, 2: ngram data, 3: ngram data>
    files = sc.newAPIHadoopFile(inputPath, "com.hadoop.mapreduce.LzoTextInputFormat", "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")
    lzoRDD = files.map(lambda x: x[1])

    # # map to 3-tuples of (ngram, year, count)
    LOGGER.info('***** SPLITTING LZO INPUT')
    allEntries = lzoRDD.map(lambda x: x.split())
    # allEntries = lines.map(lambda x: x.split())
    #
    LOGGER.info('***** GENERATING 3-TUPLES')
    formattedEntries = allEntries.map(lambda x: (str(x[0] + ' ' + x[1] + ' ' + x[2] + ' ' + x[3]), x[4], x[5]))

    LOGGER.info('***** FILTERING ENTRIES TO 1910')
    filteredEntries = formattedEntries.filter(lambda x: x[1] == year_to_search)

    LOGGER.info('***** SORT BY OCCURRENCES')
    sortedEntries = filteredEntries.sortBy(lambda a: a[2])

    LOGGER.info('***** COLLECT AND WRITE TO FILE')
    sortedEntries.collect()
    sortedEntries.saveAsTextFile(outputFile)

    sc.stop()
    sys.exit(0)
