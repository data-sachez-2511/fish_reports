import sqlite3
from collections.abc import Iterable


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
        """Return table row count.

            Raises ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
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
            return self.curs.execute(f'SELECT * FROM {self.table} WHERE {self.pk} = {idx}').fetchone()
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
                f'SELECT * FROM {self.table} WHERE {self.pk} BETWEEN {start} AND {stop - 1}'
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
                    f'UPDATE {self.table} SET {", ".join([f"{list(row)[i]} = ?" for i in range(len(row))])} WHERE {self.pk} = {idx}', [list(row.values())[i] for i in range(len(row))])
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
        start = 0
        stop = self.__len__() - 1
        step = 1
        if isinstance(idx, int):
            if idx >= self.__len__() or idx < -self.__len__():
                raise IndexError
            if -self.__len__() <= idx < 0:
                idx += self.__len__()
            self.curs.execute(f'DELETE FROM {self.table} WHERE {self.pk} = {idx}')
            self.conn.commit()
        elif isinstance(idx, slice):
            if idx.start is not None:
                start = idx.start
            if idx.stop is not None:
                stop = idx.stop
            if idx.step is not None:
                step = idx.step
            if start >= self.__len__():
                return []
            if start < 0:
                start += self.__len__()
            while stop < 0:
                stop += self.__len__()
            if start >= stop:
                return []
            self.curs.execute(
                f'DELETE FROM {self.table} WHERE {self.pk} BETWEEN {start} AND {stop - 1} AND {self.pk} % {step} = {start % step}')
            self.conn.commit()
        else:
            raise TypeError
        ids = [list(row) for row in self.curs.execute(
            f'SELECT {self.pk} FROM {self.table} WHERE {self.pk} > {start - 1}').fetchall()]
        for i in enumerate(ids):
            ids[i[0]].append(i[0] + start)
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
            row.update({self.pk: self.__len__()})
            self.curs.execute(
                f'INSERT INTO {self.table} ({", ".join([str(list(row)[i]) for i in range(len(row))])}) VALUES ({", ".join(["?" for i in range(len(row))])})', [list(row.values())[i] for i in range(len(row))])
            self.conn.commit()
        else:
            raise TypeError

    def extend(self, rows):
        """Insert rows into table.

            Raises TypeError if rows is not an iterable, ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if not isinstance(rows, Iterable):
            raise TypeError
        rows = list(rows)
        for row in rows:
            if not isinstance(row, dict):
                raise TypeError
            self.append(row)

    def pop(self, idx=-1):
        """Remove and return row at index (default last).

            Raises TypeError if index is not an integer, ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if isinstance(idx, int):
            row = self.__getitem__(idx)
            self.__delitem__(idx)
            return row
        else:
            raise TypeError

    def remove(self, row):
        """Remove first occurrence of row.

            Raises TypeError if row is not a dictionary, ValueError if the row is not present, if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if isinstance(row, dict):
            if self.pk in list(row):
                del row[self.pk]
            index = self.curs.execute(f'SELECT {self.pk} FROM {self.table} WHERE {" AND ".join([f"{list(row)[i]} = ?" for i in range(len(row))])} ORDER BY {self.pk} ASC LIMIT 1', [list(row.values())[i] for i in range(len(row))]).fetchone()
            if index:
                index = index[0]
            else:
                raise ValueError
            self.curs.execute(f'DELETE FROM {self.table} WHERE {self.pk} = {index}')
            self.curs.execute(f'UPDATE {self.table} SET {self.pk} = {self.pk} - 1 WHERE {self.pk} > {index}')
            self.conn.commit()
        else:
            raise TypeError

    def index(self, row, start=0, stop=9223372036854775807):
        """Return first index of row.

            Raises TypeError if row is not a dictionary, start or stop are not integers, ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if isinstance(start, int):
            if isinstance(stop, int):
                if isinstance(row, dict):
                    if self.pk in list(row):
                        del row[self.pk]
                    index = self.curs.execute(
                        f'SELECT {self.pk} FROM {self.table} WHERE {" AND ".join([f"{list(row)[i]} = ?" for i in range(len(row))])} AND {self.pk} BETWEEN {start} AND {stop - 1} LIMIT 1', [list(row.values())[i] for i in range(len(row))]).fetchone()
                    if index:
                        return index[0]
                    else:
                        raise ValueError
                else:
                    raise TypeError
            else:
                raise TypeError
        else:
            raise TypeError

    def add_column(self, column_name, datatype, not_null=False, default=None):
        """Add column with name specified name, datatype and constraints 'NOT NULL' and 'DEFAULT'.

        Raises ValueError if datatype is not 'NULL', 'INTEGER', 'REAL', 'TEXT', 'BLOB' or 'NUMERIC' or self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if datatype.upper() in ('NULL', 'INTEGER', 'REAL', 'TEXT', 'BLOB', 'NUMERIC'):
            if isinstance(default, str):
                default = '"' + default + '"'
            elif default is True:
                default = 1
            elif default is False:
                default = 0
            elif default is None:
                default = 'NULL'
            self.curs.execute(f'ALTER TABLE {self.table} ADD {column_name} {datatype}{" NOT NULL" * not_null} DEFAULT {default}')
            self.conn.commit()
        else:
            raise ValueError
