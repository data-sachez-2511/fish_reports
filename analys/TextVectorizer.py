from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from gensim.models import Word2Vec
from gensim.models.word2vec import LineSentence
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from gensim import corpora
from nltk.corpus import stopwords
import re
import os.path
import spacy
from server.wrapper import SqlWrapper
import joblib


class TextHandler(object):
    def __init__(self, data):
        self.stop_list = stopwords.words('russian')
        self.data = data

    def handle(self):
        def bag_of_the_words():
            vect = CountVectorizer(min_df=5, ngram_range=(2, 2), stop_words=self.stop_list)
            vect.fit(self.data)
            return vect.transform(self.data)

        def td_idf():
            td = TfidfVectorizer(min_df=5, ngram_range=(2, 2), stop_words=self.stop_list)
            td.fit(self.data)
            return td.transform(self.data)

        def word2vec():
            regexp = re.compile('(?u)\\b\\w\\w+\\b')
            doc = []
            for i in self.data:
                text_words = set(regexp.findall(i))
                for stop_word in self.stop_list:
                    if stop_word in text_words:
                        text_words.remove(stop_word)
                doc.append(list(text_words))

            w2v_model = Word2Vec(min_count=5,
                                 size=100,
                                 alpha=0.03,
                                 min_alpha=0.0007)
            w2v_model.build_vocab(doc)
            w2v_model.train(doc, total_examples=w2v_model.corpus_count, epochs=30)
            return w2v_model.wv

        def doc2vec():
            doc = [TaggedDocument(doc, [i]) for i, doc in enumerate(self.data)]
            d2v_model = Doc2Vec(doc, vector_size=400, workers=4, epochs=50)
            return d2v_model.wv

        return 1


class Bag_of_the_words():
    def __init__(self, min_df=5, ngram_range=(2, 2)):
        self.stop_list = stopwords.words('russian')
        self.vect = CountVectorizer(min_df=min_df, ngram_range=ngram_range, stop_words=self.stop_list)
        self.db = SqlWrapper()

    def save(self):
        joblib.dump(self.vect, '/analys/vect.pkl')

    def load(self):
        self.vect = joblib.load('/analys/vect.pkl')

    def fit(self, butch_size):
        n = self.db.length()
        for i in range(n // butch_size):
            doc = [self.db.select(j)['features'] for j in range(i, butch_size * (i + 1))]
            self.vect.fit(doc)
        doc = [self.db.select(j)['features'] for j in range((n - n % butch_size), n)]
        self.vect.fit(doc)

    def transform(self, doc, butch_size, size):
        n = len(doc) if size == True else self.db.length()
        doc_transform = []
        for i in range(n // butch_size):
            docx = [self.db.select(j)['features'] for j in range(i, butch_size * (i + 1))]
            doc_transform.append(self.vect.transform(docx))
        docx = [self.db.select(j)['features'] for j in range((n - n % butch_size), n)]
        doc_transform.append(self.vect.transform(docx))
        return doc_transform


class Td_idf():
    def __init__(self, min_df, ngram_range):
        self.stop_list = stopwords.words('russian')
        self.td = TfidfVectorizer(min_df=min_df, ngram_range=ngram_range, stop_words=self.stop_list)
        self.db = SqlWrapper()

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
    def __init__(self, ):
        self.stop_list = stopwords.words('russian')
        self.w2v_model = Word2Vec(min_count=5,
                             size=100,
                             alpha=0.03,
                             min_alpha=0.0007)
        self.db = SqlWrapper()

    def save(self):
        self.w2v_model.save('/analys/w2v.model')

    def load(self):
        self.w2v_model = Word2Vec.load('/analys/w2v.model')

    def fit(self, butch_size):
        pass

    def transform(self, doc, butch_size, size):
        return self.w2v_model.wv