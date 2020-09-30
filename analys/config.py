from easydict import EasyDict
from nltk.corpus import stopwords
import pandas as pd
import sqlite3

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
Cfg.regex = '^[0-9]*[.,]?[0-9]+$'

# ------------TextFilter-----------------------
Cfg.data = pd.read_sql('SELECT * from reports', sqlite3.connect('C:/fish_reports/analys/reports.db'))
Cfg.gen_signs = ['.', ':', ',', ';', '-', '?', '!', '(', ')']
Cfg.gen_letters = ['а', 'б', 'в', 'г', 'д', 'е', 'ё', 'ж', 'з', 'и', 'й', 'к', 'л', 'м', 'н', 'о', 'п', 'р', 'с', 'т', 'у', 'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ь', 'ы', 'ъ', 'э', 'ю', 'я']
Cfg.gen_numbers = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
Cfg.features = ['ь', 'й', 'т', 'к', 'а', '.', ',', ':', '-']