import copy
from pymystem3 import Mystem
from nltk.stem.snowball import SnowballStemmer
import pandas as pd
import re
import numpy as np
import time



from analys.config import Cfg as cfg


class TextFilter():
    def __init__(self, batch, stopwords=cfg.filt_stopwords):
        self.stop_list = stopwords
        self.batch = batch
        self.signs = cfg.gen_signs
        self.features = cfg.features

    def lower(self):
        '''
        :param batch: None
        :return: lowercase text
        '''
        small_text = copy.deepcopy(self.batch)
        for i in range(len(self.batch)):
            small_text[i] = self.batch[i].lower()
        return small_text

    def sign(self, form=cfg.filt_form):
        '''
        :param batch: form of separated
        :return: text with separated signs
        '''
        new_text = copy.deepcopy(self.batch)
        for j in range(len(self.batch)):
            for i in self.signs:
                new_text[j] = new_text[j].replace(i, form.format(i))
                new_text[j] = self.del2spaces(new_text[j])
        return new_text

    def sign_del(self):
        '''
        :param batch: None
        :return: text without signs
        '''
        new_text = copy.deepcopy(self.batch)
        for j in range(len(self.batch)):
            for i in self.signs:
                new_text[j] = new_text[j].replace(i, '')
                new_text[j] = self.del2spaces(new_text[j])
        return new_text

    def slist(self, form):
        '''
        :param batch: form
        :return: signs, list with number of signs
        '''
        new_text = [[0 for i in form] for j in self.batch]
        for j in range(len(self.batch)):
            for i in range(len(form)):
                list = [i for i in self.batch[j][0]]
                new_text[j][i] = list.count(form[i])
        return form, new_text

    def stopwords(self):
        '''
        :param batch: None
        :return: text without signs, list of deleted signs
        '''
        new_text = [' '.join([i for i in sentense.split(' ') if i not in self.stop_list]) for sentense in self.batch]
        return new_text

    def del2spaces(self, sentense):
        pre_text = sentense
        while pre_text != pre_text.replace('  ', ' '):
            pre_text = pre_text.replace('  ', ' ')
        sentense = pre_text
        return sentense

    def classic_filter(self):
        '''
        :param batch: None
        :return: batch with full processing
        '''
        print('start')
        new_text = [i[0] for i in self.batch]
        features = [[0 for i in range(len(self.features)+2)] for j in self.batch]
        vocab = []
        regex = cfg.regex
        regex = re.compile(regex)
        for j in range(len(self.batch)):
            if j % 1000 == 0:
                print(j, str(round(j*100/len(self.batch))) + '%')
            for i in range(len(self.features)):
                list = self.batch[j][0]
                features[j][i] = list.count(self.features[i])
            features[j][-1] = len(regex.findall(self.batch[j][0]))
            print(regex.findall(self.batch[j][0]))
            for i in self.signs:
                new_text[j] = new_text[j].replace(i, '')
                new_text[j] = self.del2spaces(new_text[j])
            new_text[j] = stemmer(new_text[j])
            new_text[j] = ' '.join([i for i in new_text[j].split(' ') if i not in self.stop_list])
            features[j][-2] = len(new_text[j])
            vocab = [i for i in new_text[j].split(' ') if i not in vocab and i != '' and len(i) > 1]
            new_text[j] = [new_text[j]]
        feat = copy.deepcopy(self.features)
        feat.append('len')
        feat.append('re')
        return new_text, feat, features, vocab


def lemma(sentence):
    m = Mystem()
    start = time.time()
    sentence = m.lemmatize(sentence)
    if len(sentence) >= 1:
        sentence.pop(-1)
    sentence = ''.join(sentence)
    end = time.time()
    print(end-start)
    return sentence


def stemmer(sent):
    #start = time.time()
    stem = SnowballStemmer('russian')
    sent = stem.stem(sent)
    #end = time.time()
    #print(end-start)
    return sent


class FeatureGenerator():
    def __init__(self, data=cfg.data):
        self.data = data
        self.signs = cfg.gen_signs

    def sign_corr(self):
        data = self.data.drop(['id', 'date', 'main_place', 'place', 'emoticons'], axis=1)
        filter = TextFilter(data.drop(['is_report'], axis=1).values.tolist())
        data = data.drop(['text'], axis=1).values
        names, signs_table = filter.slist(cfg.gen_signs)
        signs_table = np.array(signs_table)
        table = np.hstack((data, signs_table))
        names = ['is_report'] + names
        df = pd.DataFrame(table, columns=names)
        return df

    def letter_corr(self, n_letters=5):
        data = self.data.drop(['id', 'date', 'main_place', 'place', 'emoticons'], axis=1)
        filter = TextFilter(data.drop(['is_report'], axis=1).values.tolist())
        data = data.drop(['text'], axis=1).values
        names, signs_table = filter.slist(cfg.gen_letters)
        signs_table = np.array(signs_table)
        table = np.hstack((data, signs_table))
        names = ['is_report'] + names
        df = pd.DataFrame(table, columns=names)
        df_corr = df.corr().is_report.values.reshape(len(df.keys()), 1)[1:].tolist()

        maxs = []
        for i in range(n_letters):
            list = cfg.gen_letters
            a = max(df_corr)
            maxs.append([list[df_corr.index(a)], a])
            list.pop(df_corr.index(a))
            df_corr.remove(a)
        df = df.drop([i for i in cfg.gen_letters if i not in maxs[:][0]], axis=1)
        return df
    #def re_corr(self, regexp):
