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


def map_biggest_year(ngram):
    year_count = len(ngram[1])
    return ngram[1][year_count - 1], [ngram[0]]


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

    # # read input
    # LOGGER.info('***** READING LZO HADOOP FILE')
    # # LZO indexed by row i.e. <1:ngram data, 2: ngram data, 3: ngram data>
    # files = sc.sequenceFile(inputPath, "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")
    # lzoRDD = files.map(lambda x: x[1])
    # LOGGER.info('***** SPLITTING LZO INPUT')
    # allEntries = lzoRDD.map(lambda x: re.split(r'\t+', x))

    # read input
    LOGGER.info('***** READING TEXT FILE')
    files = sc.textFile(inputPath)
    # map to 3-tuples of (ngram, year, count)
    LOGGER.info('***** SPLITTING LZO INPUT')
    allEntries = files.map(lambda x: re.split(r'\t+', x))

    word_year = allEntries.map(lambda x: (x[0], [str(x[1])]))
    word_years_map = word_year.reduceByKey(lambda a, b: a + b)
    biggest_year_word = word_years_map.map(map_biggest_year)
    year_words = biggest_year_word.reduceByKey(lambda a, b: a + b)

    year_words_sorted = year_words.sortBy(lambda x: x[0])

    year_words_sorted.collect()
    year_words_sorted.saveAsTextFile(outputFile)

    sc.stop()
    sys.exit(0)
