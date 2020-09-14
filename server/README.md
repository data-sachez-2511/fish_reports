# TODO
```python
class SqlWrapper(object):
Параметры инициализации:
user, host, password, bd_nmae

def select(self, idx):
    return {'features':..., 'score'...} - строка с индексом idx

def len(self):
    return длина базы данных

def insert(self, row):
    запись в бд

def delete(self, idx):
    удаляет строку с индексом idx

def update(self, row, idx):
    обновляет строку с индексом idx данными из row
```