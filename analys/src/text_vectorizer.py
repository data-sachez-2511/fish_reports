from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from gensim.models import Word2Vec
from gensim.models.word2vec import LineSentence
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from gensim import corpora
from nltk.corpus import stopwords
import re
import os.path
import spacy
# from server.wrapper import SqlWrapper
import joblib
import numpy as np

from ..config import Cfg as cfg


class BagsOfTheWords():
    def __init__(self, min_df=cfg.bags_min_df, ngram_range=cfg.bags_ngram_range):
        self.stop_list = stopwords.words('russian')
        self.vectorizer = CountVectorizer(min_df=min_df, ngram_range=ngram_range, stop_words=self.stop_list)
        # self.db = SqlWrapper()

    def save(self):
        joblib.dump(self.vectorizer, cfg.vectorizer_path)

    def load(self):
        self.vectorizer = joblib.load(cfg.vectorizer_path)

    def fit(self, batch):
        '''
        :param batch: list of strings size of N - batch size
        :return: None
        '''
        self.vectorizer.fit(batch)

    def transform(self, batch):
        '''
        :param batch: list of strings size of N - batch size
        :return: numpy array size of [N x vocab_size]
        '''
        return self.vectorizer.transform(batch).toarray()


class Td_idf():
    def __init__(self, min_df, ngram_range):
        self.stop_list = stopwords.words('russian')
        self.td = TfidfVectorizer(min_df=min_df, ngram_range=ngram_range, stop_words=self.stop_list)
        # self.db = SqlWrapper()

    def save(self):
        joblib.dump(self.td, '/analys/td.pkl')

    def load(self):
        self.td = joblib.load('/analys/td.pkl')

    def fit(self, butch_size):
        n = self.db.length()
        for i in range(n // butch_size):
            doc = [self.db.select(j)['features'] for j in range(i, butch_size * (i + 1))]
            self.td.fit(doc)
        doc = [self.db.select(j)['features'] for j in range((n - n % butch_size), n)]
        self.td.fit(doc)

    def transform(self, doc, butch_size, size):
        n = len(doc) if size == True else self.db.length()
        doc_transform = []
        for i in range(n // butch_size):
            docx = [self.db.select(j)['features'] for j in range(i, butch_size * (i + 1))]
            doc_transform.append(self.td.transform(docx))
        docx = [self.db.select(j)['features'] for j in range((n - n % butch_size), n)]
        doc_transform.append(self.td.transform(docx))
        return doc_transform


class MyWord2Vec():
    def __init__(self):
        self.stop_list = stopwords.words('russian')
        self.vectorizer = Word2Vec(min_count=cfg.w2vec_min_count,
                                   size=cfg.w2vec_size,
                                   alpha=cfg.w2vec_alpha,
                                   min_alpha=cfg.w2vec_min_alpha,
                                   window=cfg.w2vec_window)
        self.averager = cfg.w2vec_averager
        if self.averager == 'tf_idf':
            self.tf_idf = None # TODO

    def save(self):
        self.vectorizer.save(cfg.vectorizer_path)

    def load(self):
        self.vectorizer = Word2Vec.load(cfg.vectorizer_path)

    def fit(self, batch):
        words_batch = [_.split(' ') for _ in batch]
        self.vectorizer.build_vocab(words_batch)
        self.vectorizer.train(words_batch, total_examples=(len(batch)), epochs=cfg.w2vec_epochs)

    def transform(self, batch):
        words_batch = [_.split(' ') for _ in batch]
        vectors_batch = [[self.vectorizer.wv[word] for word in sentence] for sentence in words_batch]
        if self.averager == 'mean':
            vectors_batch = [np.mean(sentence) for sentence in vectors_batch]
        elif self.averager == 'tf_idf':
        #     TODO
            pass
        return vectors_batch
