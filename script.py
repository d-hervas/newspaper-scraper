import newspaper
import re
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
import spacy
import pathlib

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

failing_webs = [
    # 'https://www.mysanantonio.com/news/us-world/world/', FALLA porque pide cookies al entrar
    # 'https://www.sfgate.com/world/', FALLA porque te pide cookies al entrar (mismo sistema que mysanantonio)
    # 'https://www.chron.com/news/nation-world/world/', FALLA porque te pide cookies al entrar (mismo sistema que mysanantonio) 
    # 'https://www.washingtonpost.com/world/?noredirect=on', FALLA porque tiene un paywall al principio (evitable)
]

# start ElasticSearch and load SpaCy NLP module
es = Elasticsearch()
nlp = spacy.load("en_core_web_sm")
val = 100
if (sys.argv[1]):
    val = int(sys.argv[1])

def parseArticle(article):
    article.download()
    article.parse()
    doc = nlp(article.text)
    tokens = list(map(lambda y: y.lemma_, filter(lambda x: x.pos_ == "NOUN", doc)))
    return (article, tokens)

def saveToElasticSearch(article, brand, tokens):
    es_body = {
        "newspaper_name": brand,
        "url": article.url,
        "publication_date": article.publish_date,
        "collection_date": datetime.now(),
        "headline": article.title,
        "body": article.text,
        "tokens": tokens,
    }
    es.index(index='articles', body=es_body)

def processArticleFirstPhase(article, token_count, brand):
    (article, tokens) = parseArticle(article)
    for token in tokens:
        if (token in token_count):
            token_count[token] = token_count[token] + 1
        else:
            token_count[token] = 1

def processArticleSecondPhase(article, target_tokens, brand):
    (article, tokens) = parseArticle(article)
    # rudimentary way to check if article has any of the target tokens
    valid_article = False
    for token in tokens:
        if token in target_tokens:
            valid_article = True
            break
    if (valid_article == False):
        return
    saveToElasticSearch(article, brand, tokens)
    
def buildWeb(web):
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
    if (paper.size() < 10):
        print('Error, size too low')
        return
    return paper

def firstPhase():
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
    for web in webs:
        paper = buildWeb(web)
        for article in paper.articles:
            processArticleFirstPhase(article)
    sorted_word_dict = sorted(token_count, key=token_count.get, reverse=True)
    print ('Writing found tokens to file target_tokens.txt...')
    with open ("target_tokens.txt", "w") as file:
        i = 0
        for word in sorted_word_dict:
            i += i
            file.write(word + '\n')
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
            target_tokens.append(word.replace('\n', ''))
            if (i > val):
                break
    print ('Read ' + str(i) + ' lines.')
    for web in webs:
        paper = buildWeb(web)
        for article in paper.articles:
            processArticleFirstPhase(article)
    print ('Done.')

# main process
target_token_file = pathlib.Path("target_tokens.txt")
if (target_token_file.exists()):
    secondPhase()
else:
    firstPhase()