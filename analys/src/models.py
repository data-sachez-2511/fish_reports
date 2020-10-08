from analys.config import Cfg as cfg


class Model:
    def __init__(self):
        self.pretrained_path = cfg.pretrained_path
        self.model_type = cfg.model_type
        ...
        self.vectorizer = ...
        self.model = ...

    def __call__(self, texts):
        '''
        Predict targets by texts
        :param texts: list of strings
        :return: list of int/float
        '''
        vectors = self.vectorizer(texts)
        return self.model.predict(vectors)

    def fit(self, texts, targets):
        '''
        Fiting model
        :param texts: list of string
        :param targets: list of int/float
        :return: None
        '''
        vectors = self.vectorizer(texts)
        self.model.fit(vectors, targets)


# model = Model()
#
# model(['Я был на рыбалке и поймал щуку!'])  # -> [1]
#
# model.fit(['Я был на рыбалке и поймал щуку!', 'всем привет'], [1, 0])
