import pymysql


class SqlWrapper(object):
    def __init__(self, host, user, password, bd_name, port):
        self.conn = pymysql.Connect(host, user, password, bd_name, port)
        self.curs = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def select(self, idx):
        if not isinstance(idx, int) or idx < 1:
            raise ValueError
        self.curs.execute('SELECT report, score FROM reports WHERE id = ' + str(idx))
        data = self.curs.fetchone()
        return {'features': data[0], 'score': data[1]}

    def length(self):
        self.curs.execute('SELECT COUNT(id) FROM reports')
        return self.curs.fetchone()[0]

    def insert(self, row):
        self.curs.execute(f'INSERT INTO reports VALUES (default, {row["features"]}, {row["score"]})')

    def delete(self, idx):
        if not isinstance(idx, int) or idx < 1:
            raise ValueError
        self.curs.execute('DELETE FROM reports WHERE id = ' + str(idx))

    def update(self, row, idx):
        if not isinstance(idx, int) or idx < 1:
            raise ValueError
        self.curs.execute(f'UPDATE reports SET report = "{row["features"]}", score = {row["score"]} WHERE id = {idx}')
        self.conn.commit()


# db = SqlWrapper('host', 'username', 'password', 'db_name', 0)
# print(db.select(1))
# print(db.length())
# db.insert({'features': 'да да да вы поняли', 'score': 0.5})
# db.delete(1)
# db.update({'features': 'че-то на языке рыбалки', 'score': 0.7}, 1)
