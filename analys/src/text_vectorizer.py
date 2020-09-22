from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from gensim.models import Word2Vec
from nltk.corpus import stopwords
import joblib
import numpy as np

from ..config import Cfg as cfg


class BagsOfTheWords():
    def __init__(self, min_df=cfg.bags_min_df, ngram_range=cfg.bags_ngram_range):
        self.stop_list = stopwords.words('russian')
        self.vectorizer = CountVectorizer(min_df=min_df, ngram_range=ngram_range, stop_words=self.stop_list)

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


class TdIdf():
    def __init__(self, min_df=cfg.tf_idf_min_df, ngram_range=cfg.tf_idf_ngram_range):
        self.stop_list = stopwords.words('russian')
        self.vectorizer = TfidfVectorizer(min_df=min_df, ngram_range=ngram_range, stop_words=self.stop_list)

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
            self.tf_idf = TfidfVectorizer(min_df=cfg.w2vec_bags_min_df, ngram_range=cfg.w2vec_bags_ngram_range)

    def save(self):
        self.vectorizer.save(cfg.vectorizer_path)

    def load(self):
        self.vectorizer = Word2Vec.load(cfg.vectorizer_path)

    def fit(self, batch):
        '''
        :param batch: list of strings size of N - batch size
        :return: None
        '''
        words_batch = [_.split(' ') for _ in batch]
        self.vectorizer.build_vocab(words_batch)
        self.vectorizer.train(words_batch, total_examples=(len(batch)), epochs=cfg.w2vec_epochs)

    def transform(self, batch):
        '''
        :param batch: list of strings size of N - batch size
        :return: numpy array size of [N x vocab_size]
        '''
        words_batch = [_.split(' ') for _ in batch]
        vectors_batch = []
        if self.averager == 'mean':
            vectors_batch = [[self.vectorizer.wv[word] for word in sentence] for sentence in words_batch]
        elif self.averager == 'tf_idf':
            batch_index = self.tf_idf.fit_transform(batch).toarray()
            words = [self.tf_idf.get_feature_names()]
            vectors_batch = words_batch
            for i in range(len(words_batch)):
                for word in words_batch[i]:
                    if word in words:
                        vectors_batch[i] = batch_index[i][words.index(word)] * self.vectorizer.wv[word]
                    else:
                        vectors_batch[i] = 0
        vectors_batch = [np.mean(sentence) for sentence in vectors_batch]
        return vectors_batch
