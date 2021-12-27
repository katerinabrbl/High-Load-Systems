import time
from typing import Mapping, Any

from pyspark import SparkConf, SparkContext

programmingLanguagesList = ["JavaScript", "Java", "PHP", "Python",
                            "C#", "C++", "Ruby", "CSS", "Objective-C",
                            "Perl", "Scala", "Haskell", "MATLAB", "Clojure",
                            "Groovy"]
titleDelim = "</title><text>"


def functionTimer(function, kwargs: Mapping[str, Any]):
    startTime = time.perf_counter()
    function(kwargs)
    stopTime = time.perf_counter()
    return stopTime - startTime


def OccuranceList(rawText):
    try:
        res = list()
        for lang in programmingLanguagesList:
            a = (rawText.upper()).count(lang.upper())
            for i in range(a):
                res.append(lang)
        return res
    except ValueError:
        return ""
    return ""


def OccuranceListCount(rawText):
    try:
        res = list()
        for lang in programmingLanguagesList:
            a = (rawText.upper()).count(lang.upper())
            res.append((lang, a))
        return res
    except ValueError:
        return "", 0


def OccuranceListCountTitle(rawText):
    try:
        title = rawText[14: rawText.find(titleDelim)]
        res = list()
        for lang in programmingLanguagesList:
            langCount = (rawText.upper()).count(lang.upper())
            if langCount > 0:
                res.append((lang, [[title], langCount]))
        return res
    except ValueError:
        return "", [[], 0]



def countLang(kwargs: Mapping[str, Any]):
    conf = SparkConf().setAppName("WordCount").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    wikidata = sc.textFile('/univer/wikipedia.dat')
    LangCount = wikidata.flatMap(lambda x: OccuranceList(x)).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
    res = LangCount.collect()
    for programmingLanguage in res:
        print(
            f"{programmingLanguage[0]} repeats {programmingLanguage[1]} times.")


def countLangOptimized(kwargs: Mapping[str, Any]):
    conf = SparkConf().setAppName("WordCount").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    wikidata = sc.textFile('/univer/wikipedia.dat')
    LangCount = wikidata.flatMap(lambda x: OccuranceListCount(x)).reduceByKey(lambda x, y: x+y)
    res = LangCount.collect()
    for programmingLanguage in res:
        print(
            f"{programmingLanguage[0]} repeats {programmingLanguage[1]} times.")


def countLangOptimizedWithTitles(kwargs: Mapping[str, Any]):
    conf = SparkConf().setAppName("WordCount").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    wikidata = sc.textFile('/univer/wikipedia.dat')
    LangCount = wikidata.flatMap(lambda x: OccuranceListCountTitle(x)).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]])
    res = LangCount.collect()
    for programmingLanguage in res:
        print(
            f"{programmingLanguage[0]} repeats {programmingLanguage[1][1]} times. In {len(programmingLanguage[1][0])} articles.")


def main():
    timeSpent = functionTimer(countLangOptimizedWithTitles, None)
    print(f"Time spent on countLangOptimizedWithTitles: {timeSpent} sec")

if __name__ == "__main__":
    main()
