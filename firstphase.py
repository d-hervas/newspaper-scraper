import newspaper
import re
import sys
from datetime import datetime
import spacy
import math
import pathlib
import json
import os
import feedparser as fp
import urllib.request
import operator
from newspaper import Article
from types import SimpleNamespace

def parseArticle(article):
    error_strings = ["There may be an issue with the delivery of your newspaper."]
    try:
        article.download()
        article.parse()
        text = article.text
        w_count = len(text.split())
        for error in error_strings:
            if (error in text):
                print('Found errored article. Continuing...')
                return (article, [], 0)
        doc = nlp(text)
        tokens = list(map(lambda y: y.lemma_, filter(lambda x: x.pos_ == "NOUN", doc)))
        return (article, tokens, w_count)
    except newspaper.ArticleException as exc: 
        print('Something went wrong parsing the article.')
        print('Exception: ')
        print(exc)
        return (article, [], 0)

def processArticleFirstPhase(article, info):
    (article, tokens, w_count) = parseArticle(article)
    for token in tokens:
        if (len(token) > 1):
            freq = tokens.count(token)/w_count
            if (token in info['new_tokens']):
                info['new_tokens'][token]['count'] = info['new_tokens'][token]['count'] + 1
                info['new_tokens'][token]['tf_scores'].append(freq)
            else:
                dic = {
                    "count": 1,
                    "tf_scores": [freq]
                }
                info['new_tokens'][token] = dic

def calculateTFIDF(info):
    tfidf_map = {}
    for token in info['new_tokens']:
        tfidf = 0
        for score in info['new_tokens'][token]['tf_scores']:
            tfidf = score/math.log(info['article_count']/info['new_tokens'][token]['count']) + tfidf
        info['new_tokens'][token]['tfidf'] = tfidf
        tfidf_map[token] = tfidf
    ordered_tfidf = sorted(tfidf_map.items(), reverse=True, key=operator.itemgetter(1))
    with open ("new_tf_idf_scores.json", "w") as f:
        json.dump(tfidf_map, f)
    with open ("new_sorted_tf_idf.txt", "w", encoding='utf8') as f:
        for token, score in ordered_tfidf:
            f.write(token + ":" + str(score) + "\n")

def firstPhase(info):
    for article_url in info['crawled_urls']:
        article = Article(url=article_url, language='en', fetch_images=False)
        processArticleFirstPhase(article, info)
    calculateTFIDF(info)
    print ('Done.')

# main process
nlp = spacy.load("en_core_web_sm")
if (os.path.isfile('./script_info.json')):
    with open("./script_info.json", "r") as j:
        info = json.load(j)
    info['new_tokens'] = {}
    firstPhase(info)