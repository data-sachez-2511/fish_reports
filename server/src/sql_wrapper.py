import sqlite3
from collections.abc import Iterable


class SqlWrapper(object):
    """API for sqlite databases similar to built-in list.
    This API is NOT suitable for multi-threading.
    Using it with multi-threading may lead to errors and data loss, use that way at your own risk."""

    def __init__(self, filename, store_len):
        """Initialize self.

            Setting store_len to True switches API to 'fast mode', meaning row count is fetched only on set_table() call.
            Setting store_len to False switches API to 'multi-instance mode', meaning that row count is fetched on every __len__() call:
            it is slower, but suited to work with several instances connected to the same DB at a time."""

        self.conn = sqlite3.connect(filename)
        self.curs = self.conn.cursor()
        self.table = None
        self.pk = None
        self.store_len = store_len
        self.len = 0

    def __del__(self):
        """Commit changes and close database connection on object destruction."""

        self.conn.commit()
        self.conn.close()

    def __len__(self):
        """Return table row count.

            Raises ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if self.store_len:
            return self.len
        else:
            return self.curs.execute(f'SELECT COUNT("{self.pk}") FROM "{self.table}"').fetchone()[0]

    def __getitem__(self, idx):
        """Return tuple or list of tuples containing row data from the table, by index or slice.

            Raises IndexError if idx is out of range,
            TypeError if idx is not integer, slice or iterable,
            ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if self.__len__() == 0:
            return None
        if isinstance(idx, int):
            if idx >= self.__len__() or idx < -self.__len__():
                raise IndexError
            if -self.__len__() <= idx < 0:
                idx += self.__len__()
            return self.curs.execute(f'SELECT * FROM "{self.table}" WHERE "{self.pk}" = {idx}').fetchone()
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
                f'SELECT * FROM "{self.table}" WHERE "{self.pk}" BETWEEN {start} AND {stop - 1}'
            ).fetchall()[0:self.__len__():idx.step]
        elif isinstance(idx, Iterable):
            rows = ['"' + i.strip() + '"' for i in idx if i.strip()]
            return self.curs.execute(f'SELECT {", ".join(rows)} FROM "{self.table}"').fetchall()
        else:
            raise TypeError

    def __setitem__(self, idx, row):
        """Update table row to passed dictionary by index.

            Raises TypeError if idx is not an integer or row is not a dictionary,
            ValueError if self.table or self.pk is None."""

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
                res_row = {}
                for i in range(len(list(row))):
                    res_row.update({'"' + list(row)[i] + '"': list(row.values())[i]})
                row = res_row
                self.curs.execute(
                    f'UPDATE "{self.table}" SET {", ".join([f"{list(row)[i]} = ?" for i in range(len(row))])} WHERE "{self.pk}" = {idx}', [i for i in row.values()])
            else:
                raise TypeError
        else:
            raise TypeError

    def __delitem__(self, idx):
        """Delete table row(s) by index or slice.

            Raises IndexError if idx is out of range,
            ValueError if self.table or self.pk is None."""

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
            self.curs.execute(f'DELETE FROM "{self.table}" WHERE "{self.pk}" = {idx}')
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
                f'DELETE FROM "{self.table}" WHERE "{self.pk}" BETWEEN {start} AND {stop - 1} AND "{self.pk}" % {step} = {start % step}')
        else:
            raise TypeError
        ids = [list(row) for row in self.curs.execute(
            f'SELECT "{self.pk}" FROM "{self.table}" WHERE "{self.pk}" > {start - 1}').fetchall()]
        for i in enumerate(ids):
            ids[i[0]].append(i[0] + start)
        ids = [[id2, id1] for id1, id2 in ids]
        self.curs.executemany(f'UPDATE "{self.table}" SET "{self.pk}" = ? WHERE "{self.pk}" = ?', ids)
        self.len = self.curs.rowcount

    def __enter__(self):
        """Return self if used in 'with ... as' statement."""

        return self

    def __exit__(self):
        """Commit changes and close database connection when exiting 'with ... as' statement."""

        self.conn.commit()
        self.conn.close()

    def commit(self):
        """Commit changes."""

        self.conn.commit()

    def set_table(self, table_name, pk):
        """Set table and primary key. Set self.len if in 'fast mode'."""

        self.table = table_name
        self.pk = pk
        if self.store_len:
            self.len = self.curs.execute(f'SELECT COUNT("{self.pk}") FROM "{self.table}"').fetchone()[0]

    def append(self, row):
        """Insert row into table.

            Raises TypeError if row is not a dictionary,
            ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if isinstance(row, dict):
            if self.pk in list(row):
                del row[self.pk]
            row.update({self.pk: self.__len__()})
            res_row = {}
            for i in range(len(list(row))):
                res_row.update({'"' + list(row)[i] + '"': list(row.values())[i]})
            row = res_row
            self.curs.execute(
                f'INSERT INTO "{self.table}" ({", ".join([str(list(row)[i]) for i in range(len(row))])}) VALUES ({", ".join(["?"] * len(row))})', [i for i in row.values()])
        else:
            raise TypeError
        self.len += 1

    def extend(self, rows):
        """Insert rows into table.

            Raises TypeError if rows is not an iterable,
            ValueError if self.table or self.pk is None."""

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

            Raises TypeError if idx is not an integer,
            ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if isinstance(idx, int):
            row = self.__getitem__(idx)
            self.__delitem__(idx)
            return row
        else:
            raise TypeError
        self.len -= 1

    def remove(self, row):
        """Remove first occurrence of row.

            Raises TypeError if row is not a dictionary,
            ValueError if row is not present, if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if isinstance(row, dict):
            if self.pk in list(row):
                del row[self.pk]
            res_row = {}
            for i in range(len(list(row))):
                res_row.update({'"' + list(row)[i] + '"': list(row.values())[i]})
            row = res_row
            index = self.curs.execute(f'SELECT "{self.pk}" FROM "{self.table}" WHERE {" AND ".join([f"{list(row)[i]} = ?" for i in range(len(row))])} ORDER BY "{self.pk}" ASC LIMIT 1', [i for i in row.values()]).fetchone()
            if index:
                index = index[0]
            else:
                raise ValueError
            self.curs.execute(f'DELETE FROM "{self.table}" WHERE "{self.pk}" = {index}')
            self.curs.execute(f'UPDATE "{self.table}" SET "{self.pk}" = "{self.pk}" - 1 WHERE "{self.pk}" > {index}')
        else:
            raise TypeError
        self.len -= 1

    def index(self, row, start=0, stop=9223372036854775807):
        """Return index of the first occurrence of row.

            Raises TypeError if row is not a dictionary, start or stop are not integers,
            ValueError if self.table or self.pk is None."""

        if self.table is None or self.pk is None:
            raise ValueError
        if isinstance(start, int):
            if isinstance(stop, int):
                if isinstance(row, dict):
                    if self.pk in list(row):
                        del row[self.pk]
                    res_row = {}
                    for i in range(len(list(row))):
                        res_row.update({'"' + list(row)[i] + '"': list(row.values())[i]})
                    row = res_row
                    index = self.curs.execute(
                        f'SELECT "{self.pk}" FROM "{self.table}" WHERE {" AND ".join([f"{list(row)[i]} = ?" for i in range(len(row))])} AND "{self.pk}" BETWEEN {start} AND {stop - 1} LIMIT 1', [i for i in row.values()]).fetchone()
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
        """Add column with specified name, datatype and constraints 'NOT NULL' and 'DEFAULT'.

        Raises ValueError if datatype is not 'NULL', 'INTEGER', 'REAL', 'TEXT', 'BLOB' or 'NUMERIC',
        if default isn't string True, False, None or number, if self.table or self.pk is None."""

        if self.table is None or self.pk is None or (not_null and default is None):
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
            elif not isinstance(default, int) and not isinstance(default, float):
                raise ValueError
            self.curs.execute(f'ALTER TABLE "{self.table}" ADD "{column_name}" {datatype}{" NOT NULL" * not_null} DEFAULT {default}')
        else:
            raise ValueError