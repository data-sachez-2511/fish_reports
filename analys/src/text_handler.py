from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from gensim.models import Word2Vec
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import nltk
from nltk.corpus import stopwords
import re
import os.path
import spacy


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