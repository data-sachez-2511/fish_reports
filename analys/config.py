from easydict import EasyDict

Cfg = EasyDict()

Cfg.vectorizer = 'bags' # ['bags', 'tf_idf', 'word2vec']
Cfg.vectorizer_path = '/analys/vect.pkl'

# ------------Bags of the words-----------------------
Cfg.bags_min_df = 5
Cfg.bags_ngram_range = (1, 2)


# ------------Tf-Idf-----------------------



# ------------Word2Vec-----------------------

