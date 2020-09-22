from nltk.corpus import stopwords
from ..config import Cfg as cfg

class TextFilter():
    def __init__(self, batch):
        self.stop_list = stopwords.words('russian')
        self.batch = batch

    def lower(self):
        small_text = self.stop_list
        for i in range(len(self.stop_list)):
            small_text[i] = self.stop_list[i].lower()
        return small_text

    def del_stop(self):
        batch_stop = self.batch
        for i in range(len(self.batch)):
            for word in self.stop_list:
                batch_stop[i].replace(word)
        return batch_stop