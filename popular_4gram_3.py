import sys
from operator import add
from pyspark import SparkContext
from pyspark import SparkConf
import logging
import re
import os


def filter_year(ngram):
    year = int(ngram[2])
    if year >= 2000 and year <= 2009:
        return True
    else:
        return False


def take_highest(ngrams):
    final_iterator = []
    count = 3
    for el in ngrams:
        if count == 0:
            break
        final_iterator.append(el)
        count -= 1
    return iter(final_iterator)


def sort_and_crop(ngrams):
    final_iterator = []

    middle = sorted(ngrams, key=lambda x: -int(x[0]))

    count = 10
    for el in middle:
        if count == 0:
            break
        final_iterator.append(el)
        count -= 1
    return iter(final_iterator)


if __name__ == '__main__':
    # collect input parameters
    inputPath = sys.argv[1]
    outputFile = sys.argv[2]

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
    LOGGER.info('***** SPLITTING LZO INPUT')
    allEntries = lzoRDD.map(lambda x: re.split(r'\t+', x))

    # # read input
    # LOGGER.info('***** READING TEXT FILE')
    # files = sc.textFile(inputPath)
    # # map to 3-tuples of (ngram, year, count)
    # LOGGER.info('***** SPLITTING LZO INPUT')
    # allEntries = files.map(lambda x: re.split(r'\t+', x))

    # # 4gram: x[0]:ngram - x[1]:year - x[2]:occurrences
    formattedEntries = allEntries.map(lambda x: (x[2], x[0], x[1]))

    LOGGER.info('***** FILTERING ENTRIES TO INPUTTED YEAR')
    # filteredEntries = formattedEntries.filter(lambda x: x[2] == year_to_search)
    filteredEntries = formattedEntries.filter(filter_year)

    # LOGGER.info('***** SORT BY OCCURRENCES')
    # sortedEntries = filteredEntries.sortBy(lambda x: -int(x[0]))
    #
    # LOGGER.info('***** MAP PARTITIONS')
    # highest = sortedEntries.mapPartitions(take_highest)

    cropped = filteredEntries.mapPartitions(sort_and_crop)
    cropped_sorted = cropped.sortBy(lambda x: -int(x[0]))

    LOGGER.info('***** COLLECT AND WRITE')
    cropped_sorted.collect()
    cropped_sorted.saveAsTextFile(outputFile)

    # sortedEntries.collect()
    # sortedEntries.saveAsTextFile(outputFile)

    sc.stop()
    sys.exit(0)
