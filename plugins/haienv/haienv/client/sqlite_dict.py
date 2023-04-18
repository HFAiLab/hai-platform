import sqlite3
from pickle import loads, dumps


class SqliteDict:
    """
    仅用于haienv的sqlite_dict，不保证线程安全
    """
    def __init__(self, filename, tablename):
        self.conn = sqlite3.connect(filename)
        self.tablename = tablename
        self.w_execute(f'CREATE TABLE IF NOT EXISTS "{self.tablename}" (key TEXT PRIMARY KEY, value BLOB)')

    @staticmethod
    def encode(data):
        return sqlite3.Binary(dumps(data, protocol=4))

    @staticmethod
    def decode(data):
        return loads(bytes(data))

    def w_execute(self, sql, args=()):
        self.conn.execute(sql, args)
        return self.conn.commit()

    def r_execute(self, sql, args=()):
        return self.conn.execute(sql, args)

    def __setitem__(self, key, value):
        self.w_execute(f'REPLACE INTO "{self.tablename}" (key, value) VALUES (?,?)', (key, self.encode(value)))

    def __getitem__(self, key):
        rst = self.r_execute(f'SELECT value FROM "{self.tablename}" WHERE key = ?', args=(key,)).fetchone()
        assert rst is not None, f"未找到key {key}"
        return self.decode(rst[0])

    def __contains__(self, key):
        rst = self.r_execute(f'SELECT 1 FROM "{self.tablename}" WHERE key = ?', args=(key,)).fetchone()
        return rst is not None

    def __len__(self):
        rst = self.r_execute(f'SELECT COUNT(*) FROM "{self.tablename}"').fetchone()
        return rst[0] if rst is not None else 0

    def __bool__(self):
        return len(self) > 0

    def get(self, key, default=None):
        return self[key] if key in self else default

    def keys(self):
        rst = self.r_execute(f'SELECT key FROM "{self.tablename}" ORDER BY rowid').fetchall()
        for item in rst:
            yield item[0]

    def values(self):
        rst = self.r_execute(f'SELECT value FROM "{self.tablename}" ORDER BY rowid').fetchall()
        for item in rst:
            yield self.decode(item[0])

    def items(self):
        rst = self.r_execute(f'SELECT key, value FROM "{self.tablename}" ORDER BY rowid').fetchall()
        for item in rst:
            yield item[0], self.decode(item[1])

    def pop(self, key, default=None):
        if key in self:
            result = self[key]
            self.w_execute(f'DELETE FROM "{self.tablename}" WHERE key = ?', args=(key, ))
            return result
        return default

    def __iter__(self):
        for item in self.keys():
            yield item

    def __del__(self):
        try:
            self.conn.close()
        except:
            pass
