from analys.src.text_handler import TextFilter
from analys.src.text_vectorizer import BagsOfTheWords
import pandas as pd
import numpy as np
import sqlite3

text, features, f_names = [], [], []
batch_size = 10
vocab = []
df = pd.read_sql('SELECT * from reports', sqlite3.connect('C:/fish_reports/analys/reports.db')).drop(['id', 'date', 'main_place', 'place', 'emoticons', 'is_report'], axis=1)[:100]

for i in range(0, len(df), batch_size):
    print(i)

    data = df[i:i+batch_size].values.tolist()
    f = TextFilter(data)
    text1, f_names, features1, vocab1 = f.classic_filter()
    vocab.extend(vocab1)

    if i == 0:
        text, features = text1, features1
    else:
        text, features = np.vstack((text, text1)), np.vstack((features, features1))

f_names.append('text')
df = pd.DataFrame(np.hstack((np.array(features), np.array(text))), columns=f_names)
print(df)

vec = BagsOfTheWords()
vec.fit(vocab)
vectors = []

for i in range(0, len(df), batch_size):
    print(i)

    data = df.text[i:i+batch_size].values.tolist()
    vectors_batch = vec.transform(data)

    if i == 0:
        vectors = vectors_batch
    else:
        vectors = np.vstack((vectors, vectors_batch))

vocab = list(vec.vectorizer.vocabulary_.keys())
print(vocab)
for i in range(len(vocab)):
    df['{}'.format(vocab[i])] = vectors[:, i]
print(df)