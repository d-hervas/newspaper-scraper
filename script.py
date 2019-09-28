import newspaper
import re
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
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

rss=[
]

webs = [
    # 'https://www.abqjournal.com/category_/news-more',
    'https://www.bostonglobe.com/world/',
    'https://www.bostonherald.com/news/world-news/',
    'https://www.charlotteobserver.com/news/nation-world/world/',
    'https://www.cleveland.com/world/',
    'https://www.star-telegram.com/news/nation-world/world',
    'https://www.kansascity.com/news/nation-world/world',
    'https://www.miamiherald.com/news/nation-world/world/',
    'http://www.startribune.com/world/',
    'https://www.nytimes.com/section/world',
    'https://oklahoman.com/news/us-world',
    'https://www.post-gazette.com/news/world', #parece que pilla pocas noticias...
    'https://www.sacbee.com/news/nation-world/world/',
    'https://www.wsj.com/news/world',
    'https://www.washingtontimes.com/news/world/',
]

# start ElasticSearch and load SpaCy NLP module
es = Elasticsearch()
nlp = spacy.load("en_core_web_sm")
val = 100
if (len(sys.argv) > 1):
    val = int(sys.argv[1])

def parseArticle(article):
    error_strings = ["There may be an issue with the delivery of your newspaper."]
    try:
        article.download()
        article.parse()
        for error in error_strings:
            if (error in article.text):
                print('Found errored article. Continuing...')
                return (article, [])
        doc = nlp(article.text)
        tokens = list(map(lambda y: y.lemma_, filter(lambda x: x.pos_ == "NOUN", doc)))
        return (article, tokens)
    except newspaper.ArticleException as exc: 
        print('Something went wrong parsing the article.')
        print('Exception: ')
        print(exc)
        return (article, [])

def processArticleFirstPhase(article, info):
    (article, tokens) = parseArticle(article)
    for token in tokens:
        if (len(token) > 1):
            freq = tokens.count(token)/len(tokens) 
            if (token in info['tokens'] or []):
                info['tokens'][token]['count'] = info['tokens'][token]['count'] + 1
                info['tokens'][token]['tf_sum'] = info['tokens'][token]['tf_sum'] + freq
            else:
                dic = {
                    "count": 1,
                    "tf_sum": freq
                }
                info['tokens'][token] = dic

def processArticleSecondPhase(article, target_tokens, brand, saved_articles):
    (article, tokens) = parseArticle(article)
    # rudimentary way to check if article has any of the target tokens
    valid_article = False
    for token in tokens:
        if token in target_tokens:
            valid_article = True
            break
    if (valid_article == False):
        return
    es_body = {
        "newspaper_name": brand,
        "url": article.url,
        "publication_date": article.publish_date,
        "collection_date": datetime.now(),
        "headline": article.title,
        "body": article.text,
        "tokens": tokens,
    }
    formatted_publish_date = article.publish_date.isoformat() if article.publish_date else 'Unknown'
    es_JSON_body = {
        #I'm going straight to hell for this
        "newspaper_name": brand,
        "url": article.url,
        "publication_date": formatted_publish_date,
        "collection_date": datetime.now().isoformat(),
        "headline": article.title,
        "body": article.text,
        "tokens": tokens,
    }
    saved_articles.append(es_JSON_body)
    es.index(index='articles', body=es_body) #save to elasticSearch
    
def buildWeb(web, first_phase):
    class Category(object):
        #Hack - reverse engineered newspaper3k to introduce custom sources
        def __init__(self, url):
            self.url = url
            self.html = None
            self.doc = None
    paper = newspaper.Source(web, language='en', fetch_images = False)
    paper.categories = [Category(web)]
    paper.download_categories()
    paper.parse_categories()
    paper.generate_articles()
    print ('Newspaper ' + paper.brand + ' built. Size: ' + str(paper.size()))
    return paper

def calculateTFIDF(info):
    tfidf_map = {}
    for token in info['tokens']:
        tfidf = info['tokens'][token]['tf_sum']/math.log(info['article_count']/info['tokens'][token]['count'])
        info['tokens'][token]['tfidf'] = tfidf
        tfidf_map[token] = tfidf
    ordered_tfidf = sorted(tfidf_map.items, key=operator.itemgetter(1))
    with open ("tf_idf_scores.json", "w") as f:
        json.dump(tfidf_map, j)
    with open ("sorted_tf_idf.txt", "w") as f:
        for token, score in ordered_tfidf:
            f.write(token + ":" + score + "\n")

def firstPhase(info):
    if (info.get('execution_count') == 0):
        # create elastic search instance
        body_settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "members": {
                    "dynamic": "strict",
                    "properties": {
                        "newspaper_name": { "type": "text" },
                        "url": { "type": "text" },
                        "publication_date": { "type": "date" },
                        "collection_date": { "type": "date" },
                        "headline": { "type": "text" },
                        "body": { "type": "text" },
                        "tokens": { "type": "keyword" }
                    }
                }
            }
        }
        es.indices.create(index="articles", ignore=400, body=body_settings)
    # crawl webs
    for web in webs:
        paper = buildWeb(web, True)
        try:
            for article in paper.articles:
                info['article_count'] = info['article_count'] + 1
                es.index(index='articles', body={
                    "url": article.url,
                    "newspaper_name": paper.brand,
                    "collection_date": datetime.now()
                }) #save link to elasticSearch
                if (article.url not in info['crawled_urls']):
                    info['crawled_urls'].append(article.url)
                    processArticleFirstPhase(article, info)
        except AttributeError as att:
            print('Attribute error on web ' + web + '\n')
            print('This probably means the web has no articles or they all have been crawled already. Continuing... /n')

    # crawl RSS feeds
    for url_feed in rss:
        feed = urllib.request.urlretrieve(url_feed)
        parsed_feed = fp.parse(feed) 
        for entry in parsed_feed.entries:
            info.article_count += 1
            es.index(index='articles', body={
                "url": entry.link,
                "collection_date": datetime.now()
            }) #save link to elasticSearch
            processArticleFirstPhase(Article(entry.link), info)

    #sum 1 to ex.count
    info['execution_count'] = info.get('execution_count') + 1

    if (info.get('execution_count') == 7):
        calculateTFIDF(info)


    with open("./script_info.json", "w") as j:
        json.dump(info, j)
    
    print ('Done.')

def secondPhase():
    print ('Loading tokens...')
    target_tokens = []
    with open ("sorted_tf_idf.txt", "r") as file:
        i = 0
        for word in file:
            i += 1
            target_tokens.append(word.split(":")[0])
            if (i > val):
                break
    print ('Read ' + str(i) + ' lines.')
    date = datetime.now().strftime("%m-%d")
    os.mkdir('./'+date)
    for web in webs:
        try:
            paper = buildWeb(web, False)
            saved_articles = []
            for article in paper.articles:
                processArticleSecondPhase(article, target_tokens, paper.brand, saved_articles)
                with open('./'+date+'/'+paper.brand+".json", "w") as wrt_article:
                    json.dump(saved_articles, wrt_article)
        except AttributeError as att:
            print('Attribute error on web ' + web + '\n')
            print('This probably means the web has no articles or they all have been crawled already. Continuing... /n')
    print ('Done.')

# main process
info = {
    "tokens": {},
    "execution_count": 0,
    "article_count": 0,
    "crawled_urls": []
}
if (os.path.isfile('./script_info.json')):
    with open("./script_info.json", "r") as j:
        info = j.load()
if (info.get('execution_count') == 7):
    secondPhase()
else:
    firstPhase(info)