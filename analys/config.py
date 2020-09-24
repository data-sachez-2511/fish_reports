from easydict import EasyDict
from nltk.corpus import stopwords

Cfg = EasyDict()

Cfg.vectorizer = 'bags'  # ['bags', 'tf_idf', 'word2vec']
Cfg.vectorizer_path = '/analys/vect.pkl'
Cfg.batch_size = 32

# ------------Bags of the words-----------------------
Cfg.tf_idf_min_df = 1
Cfg.tf_idf_ngram_range = (1, 1)

# ------------Tf-Idf-----------------------
Cfg.bags_min_df = 1
Cfg.bags_ngram_range = (1, 1)

# ------------Word2Vec-----------------------

Cfg.w2vec_min_count = 1
Cfg.w2vec_size = 300
Cfg.w2vec_alpha = 0.03
Cfg.w2vec_min_alpha = 0.0007
Cfg.w2vec_window = 5
Cfg.w2vec_epochs = 30
Cfg.w2vec_bags_min_df = 1
Cfg.w2vec_bags_ngram_range = (1, 1)
Cfg.w2vec_averager = 'mean' # ['mean', 'tf_idf']

# ------------TextFilter-----------------------
Cfg.filt_stopwords = stopwords.words('russian')
Cfg.filt_form = ' {} '