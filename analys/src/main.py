from server.sql_wrapper import SqlWrapper
from analys.src.text_handler import TextFilter
from analys.src.text_vectorizer import BagsOfTheWords, TdIdf, MyWord2Vec
import numpy as np
import sqlite3
from time import time

from analys.config import Cfg as cfg


def preparing(file_path, batch_size, vec_type=cfg.w2vec_averager):
    with open('vocab.txt', 'w') as f:
        pass

    db = SqlWrapper(file_path)
    db_pre = SqlWrapper(file_path)
    db_pre.set_table('pre_reports', 'id')
    db.set_table('reports', 'id')

    for i in range(0, len(db), batch_size):
        if i % 500 == 0:
            print('Progress - {}, processed - {}'.format(str(round((i*100)/len(db), 2)) + '%', i))

        data = [t[5] for t in db[i:i + batch_size]]

        f = TextFilter(data)
        text, names, features, vocab_batch = f.classic_filter()

        if i == -1:
            for name in names:
                db_pre.add_column(name, 'INTEGER')

        db_pre.extend([{names[q]: features[w][q] for q in range(len(names))} for w in range(len(features))])
        db.commit()

        with open('vocab.txt', 'a') as f:
            for j in vocab_batch:
                f.write(str(j) + ';')

    with open('vocab.txt', 'r') as f:
        vocab = f.read().split(';')

    vec = BagsOfTheWords()
    vec.fit(vocab)
    vocab = list(vec.vectorizer.vocabulary_.keys())

    for name in vocab:
        db_pre.add_column(name, 'INTEGER')
    db_pre.commit()
    for i in range(0, len(db), batch_size):
        print(i)

        data = [t[5] for t in db[i:i + batch_size]]
        vectors_batch = vec.transform(data)

        db_pre.extend([{vocab[q]: vectors_batch[w][q] for q in range(len(vocab))} for w in range(len(vectors_batch))])
        db_pre.commit()

file_path = 'C:/fish_reports/analys/reports.db'
preparing(file_path, 100)
