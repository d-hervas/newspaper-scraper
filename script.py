import newspaper
import re
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
import spacy
import pathlib
import json
import os
import feedparser as fp
import urllib.request
from newspaper import Article

rss=[
]

webs = [
    'https://www.abqjournal.com/category/news-more',
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

def processArticleFirstPhase(article, token_count, brand):
    (article, tokens) = parseArticle(article)
    for token in tokens:
        if (token in token_count):
            token_count[token] = token_count[token] + 1
        else:
            token_count[token] = 1

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
    if (first_phase and paper.size() < 10):
        print('Error, size too low')
        return
    return paper

def firstPhase(ex_count):
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
    token_count = {}
    # crawl webs
    for web in webs:
        paper = buildWeb(web, True)
        for article in paper.articles:
            processArticleFirstPhase(article, token_count, paper.brand)
    # crawl RSS feeds
    for url_feed in rss:
        feed = urllib.request.urlretrieve(url_feed)
        parsed_feed = fp.parse(feed) 
        for entry in parsed_feed.entries:
            processArticleFirstPhase(Article(entry.link), token_count, 'todo')

    sorted_word_dict = sorted(token_count, key=token_count.get, reverse=True)
    if (ex_count == 7):
        print ('Writing found tokens to file target_tokens.txt...')
        with open ("target_tokens.txt", "w") as file:
            i = 0
            for word in sorted_word_dict:
                if (len(word) > 1):
                    i += i
                    file.write(word + ' ' + str(token_count[word]) + '\n')
                    if i > val:
                        break
    print ('Done.')

def secondPhase():
    print ('Loading tokens from target_tokens.txt...')
    target_tokens = []
    with open ("target_tokens.txt", "r") as file:
        i = 0
        for word in file:
            i += 1
            target_tokens.append(word.split()[0])
            if (i > val):
                break
    print ('Read ' + str(i) + ' lines.')
    date = datetime.now().strftime("%m-%d")
    os.mkdir('./'+date)
    for web in webs:
        paper = buildWeb(web, False)
        saved_articles = []
        for article in paper.articles:
            processArticleSecondPhase(article, target_tokens, paper.brand, saved_articles)
            with open('./'+date+'/'+paper.brand+".json", "w") as wrt_article:
                json.dump(saved_articles, wrt_article)
    print ('Done.')

# main process
target_token_file = pathlib.Path("target_tokens.txt")
if (not (pathlib.Path("./count.txt").exists())):
    with open ("count.txt", "w") as file:
        file.write(0)
if (target_token_file.exists()):
    secondPhase()
else:
    ex_count = 0
    with open ("count.txt") as f:
        ex_count = int(f.read()) + 1
        f.write(str(ex_count))
    firstPhase(ex_count)