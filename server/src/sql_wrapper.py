import sqlite3


class SqlWrapper(object):
    """Interface for sqlite databases, similar to built-in list."""

    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.curs = self.conn.cursor()
        self.table = None
        self.pk = None

    def __del__(self):
        """Close database connection on object deletion."""

        self.conn.close()

    def __len__(self):
        """Return table row count."""

        self.curs.execute(f'SELECT COUNT({self.pk}) FROM {self.table}')
        return self.curs.fetchone()[0]

    def __getitem__(self, idx):
        """Return tuple or list of tuples containing row data from the table, by index or slice.

            Raises IndexError if idx is out of range, TypeError if idx is not integer or slice, ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if self.__len__() == 0:
            return None
        if isinstance(idx, int):
            if idx >= self.__len__() or idx < -self.__len__():
                raise IndexError
            if -self.__len__() <= idx < 0:
                idx += self.__len__()
            return self.curs.execute(f'SELECT * FROM {self.table} WHERE {self.pk} = {idx + 1}').fetchone()
        elif isinstance(idx, slice):
            start = idx.start
            stop = idx.stop
            if start is None:
                start = 0
            if stop is None:
                stop = self.__len__()
            if start >= self.__len__():
                return []
            if start < 0:
                start += self.__len__()
            while stop < 0:
                stop += self.__len__()
            if start >= stop:
                return []
            return self.curs.execute(
                f'SELECT * FROM {self.table} WHERE {self.pk} BETWEEN {start + 1} AND {stop}'
            ).fetchall()[0:self.__len__():idx.step]
        else:
            raise TypeError

    def __setitem__(self, idx, row):
        """Update table row to passed dictionary by index.

            Raises TypeError if idx is not an integer or row is not a dictionary, ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if self.__len__() == 0:
            return
        if isinstance(idx, int):
            if isinstance(row, dict):
                if idx >= self.__len__() or idx < -self.__len__():
                    raise IndexError
                if -self.__len__() <= idx < 0:
                    idx += self.__len__()
                self.curs.execute(
                    f'UPDATE {self.table} SET {", ".join([f"{list(row)[i]} = ?" for i in range(len(row))])} WHERE {self.pk} = {idx + 1}', [list(row.values())[i] for i in range(len(row))])
                self.conn.commit()
            else:
                raise TypeError
        else:
            raise TypeError

    def __delitem__(self, idx):
        """Delete table row(s) by index or slice.

            Raises IndexError if index idx is out of range, ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if self.__len__() == 0:
            return
        if isinstance(idx, int):
            if idx >= self.__len__() or idx < -self.__len__():
                raise IndexError
            if -self.__len__() <= idx < 0:
                idx += self.__len__()
            self.curs.execute(f'DELETE FROM {self.table} WHERE {self.pk} = {idx + 1}')
            self.conn.commit()
        elif isinstance(idx, slice):
            start = idx.start
            stop = idx.stop
            step = idx.step
            if start is None:
                start = 0
            if stop is None:
                stop = self.__len__()
            if step is None:
                step = 1
            if start >= self.__len__():
                return []
            if start < 0:
                start += self.__len__()
            while stop < 0:
                stop += self.__len__()
            if start >= stop:
                return []
            self.curs.execute(
                f'DELETE FROM {self.table} WHERE {self.pk} BETWEEN {start + 1} AND {stop} AND {self.pk} % {step} = 1')
            self.conn.commit()
        else:
            raise TypeError
        ids = [list(row) for row in self.curs.execute(
            f'SELECT {self.pk} FROM {self.table} WHERE {self.pk} > {start}').fetchall()]
        for i in enumerate(ids):
            ids[i[0]].append(i[0] + 1)
        ids = [[id2, id1] for id1, id2 in ids]
        self.curs.executemany(f'UPDATE {self.table} SET {self.pk} = ? WHERE {self.pk} = ?', ids)
        self.conn.commit()

    def __enter__(self):
        """Return self if used in 'with ... as' statement."""

        return self

    def __exit__(self):
        """Close database connection when exiting 'with ... as' statement."""

        self.conn.close()

    def set_table(self, table_name, pk):
        """Set table and primary key."""

        self.table = table_name
        self.pk = pk

    def append(self, row):
        """Insert row into table.

            Raises TypeError if row is not a dictionary, ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if isinstance(row, dict):
            if self.pk in list(row):
                del row[self.pk]
            row.update({self.pk: self.__len__() + 1})
            self.curs.execute(
                f'INSERT INTO {self.table} ({", ".join([str(list(row)[i]) for i in range(len(row))])}) VALUES ({", ".join(["?" for i in range(len(row))])})', [list(row.values())[i] for i in range(len(row))])
            self.conn.commit()
        else:
            raise TypeError
