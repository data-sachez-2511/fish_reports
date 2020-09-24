import copy
from pymystem3 import Mystem
from nltk.stem.snowball import SnowballStemmer


from analys.config import Cfg as cfg


class TextFilter():
    def __init__(self, batch, stopwords=cfg.filt_stopwords):
        self.stop_list = stopwords
        self.batch = batch

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
            for i in range(len(self.batch[j])):
                if not self.batch[j][i].isalnum() and self.batch[j][i] != ' ':
                    new_text[j] = new_text[j].replace(self.batch[j][i], form.format(self.batch[j][i]))
                    new_text[j] = self.del2spaces(new_text[j])
        return new_text

    def sign_del(self):
        '''
        :param batch: None
        :return: text without signs, list of deleted signs
        '''
        new_text = copy.deepcopy(self.batch)
        signs = [[] for i in self.batch]
        for j in range(len(self.batch)):
            for i in range(len(self.batch[j])):
                if not self.batch[j][i].isalnum() and self.batch[j][i] != ' ':
                    new_text[j] = new_text[j].replace(self.batch[j][i], '')
                    new_text[j] = self.del2spaces(new_text[j])
                    signs[j].append(self.batch[j][i])
        return new_text, signs

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
        filter = TextFilter(self.batch)
        filter.batch = filter.lower()
        filter.batch = filter.sign_del()[0]
        filter.batch = filter.stopwords()
        return filter.batch


def lemma(batch):
    m = Mystem()
    text = []
    for sentense in batch:
        sentense = m.lemmatize(sentense)
        sentense.pop(-1)
        text.append(''.join(sentense))
    return text


def stemmer(batch):
    stem = SnowballStemmer('russian')
    text = []
    for sent in batch:
        sent = stem.stem(sent)
        text.append(sent)
    return text
