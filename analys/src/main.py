from server.sql_wrapper import SqlWrapper
from analys.src.text_handler import TextFilter
from analys.src.text_vectorizer import BagsOfTheWords, TdIdf, MyWord2Vec
from time import time

from analys.config import Cfg as cfg


def preparing(file_path, table_name, column_name, batch_size, file_names, vec_type=cfg.vectorizer):
    with open(file_names['vocab'], 'w', encoding='utf-8') as f:
        pass
    with open(file_names['features'], 'w', encoding='utf-8') as file:
        pass
    with open(file_names['vectors'], 'w', encoding='utf-8') as vec_file:
        pass

    db = SqlWrapper(file_path, True)
    db.set_table(table_name, 'id')

    len_ = len(db)
    for i in range(0, len_, batch_size):
        if i % 2560 == 0:
            print('Progress - {}, processed - {}'.format(str(round((i*100)/len_, 2)) + '%', i))

        data = [t[column_name] for t in db[i:i + batch_size]]

        f = TextFilter(data)
        text, names, features, vocab_batch = f.classic_filter()

        with open(file_names['features'], 'a', encoding='utf-8') as file:
            if i == 0:
                for name in names[0:-1]:
                    file.write(name + ';')
                file.write(names[-1] + '\n')

            for feature in features:
                for fx in feature[0:-1]:
                    file.write(str(fx) + ';')
                file.write(str(feature[-1]) + '\n')

        with open(file_names['vocab'], 'a', encoding='utf-8') as f:
            for j in vocab_batch:
                f.write(str(j) + ';')

    with open(file_names['vocab'], 'r', encoding='utf-8') as f:
        vocab = set(f.read().split(';')[0:-1])

    if vec_type == 'word2vec':
        vec = MyWord2Vec(vocab)
        vec.build_vocab()
    else:
        vec = BagsOfTheWords() if vec_type == 'bags' else TdIdf()
        vec.fit(vocab)
        vocab = list(vec.vectorizer.vocabulary_.keys())


    for i in range(0, len_, batch_size):
        if i % 2560 == 0:
            print('Progress_v - {}, processed_v - {}'.format(str(round((i * 100) / len_, 2)) + '%', i))

        data = [t[column_name] for t in db[i:i + batch_size]]

        f = TextFilter(data)
        data, names, features, vocab_batch = f.classic_filter()

        if vec_type == 'word2vec':
            vec.fit(data)
            vectors_batch = vec.transform(data)
        else:
            vectors_batch = vec.transform(data)

        with open(file_names['vectors'], 'a', encoding='utf-8') as file:
            if i == 0:
                for word in vocab[0:-1]:
                    file.write(word + ';')
                file.write(vocab[-1] + '\n')
            l = len(vectors_batch[0])
            for vectors in vectors_batch:
                for v in range(0, l, 512):
                    if v + 512 < l:
                        file.write(str(vectors[v:v + 512].tolist())[1:-1].replace(', ', ';') + ';')
                    else:
                        file.write(str(vectors[v:v + 512].tolist())[1:-1].replace(', ', ';'))

file_path = 'C:/fish_reports/analys/reports.db'
t = time()
preparing(file_path, 'reports', 5, 512, {'vocab': 'vocab.txt', 'features': 'features.txt', 'vectors': 'vectors.txt'})
print(time()-t)
print((time()-t)/565672)