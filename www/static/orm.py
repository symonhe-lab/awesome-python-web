import logging; logging.basicConfig(level=logging.INFO)
import asyncio
import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql.replace('?', '%s'))
    logging.info('Args: %s' % args)

# asyncio function, create a public mysql connect pool
# parm user, password, db, loop must needed
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool   # this is a global variable
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),       # host, if don't exist then use localhost
        port=kw.get('port', 3306),              # MySql port
        user=kw['user'],                        # username
        password=kw['password'],                # password
        db=kw['db'],                            # database
        charset=kw.get('charset', 'utf8'),      # charset
        autocommit=kw.get('autocommit', True),  # autocommit mode
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop                               # asyncio event loop instance
    )

# asyncio function, run mysql SELECT command
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        # create dict cursor, which returns results as a dictionary
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs

# asyncio function, run mysql INSERT, UPDATE, DELETE
async def execute(sql, args, autocommit=True):
    log(sql, args)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

# create and return str as '?, ?, ?', count and length according to parm num
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

# Base attribute class of Model class
class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, column_type='VARCHAR(100)'):
        super().__init__(name, column_type, primary_key, default)

class BooleanField(Field):
    def __init__(self, name=None, primary_key=False, default=False, column_type='BOOLEAN'):
        super().__init__(name, column_type, primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0, column_type='BIGINT'):
        super().__init__(name, column_type, primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0, column_type='REAL'):
        super().__init__(name, column_type, primary_key, default)

class TextField(Field):
    def __init__(self, name=None, primary_key=False, default=None, column_type='TEXT'):
        super().__init__(name, column_type, primary_key, default)

# metaclass
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # exclude Model class, we will not change base class
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # get table name, this is the mysql table name in pointed database, if not this attribute, use class name
        tableName = attrs.get('__table__', None) or name
        logging.info(' Found model: %s (table: %s)' % (name, tableName))

        # get all Field and primary key
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(' Found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # find primary key
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')

        # remove all Field attribute in class
        for k in mappings.keys():
            attrs.pop(k)

        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings        # attribute relation mapping
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey   # primary key
        attrs['__fields__'] = fields            # other attribute except for primary key
        # default mysql command: SELECT, INSERT, UPDATE and DELETE
        attrs['__select__'] = 'SELECT `%s`, %s FROM `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'INSERT INTO `%s` (%s, `%s`) VALUES (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'UPDATE `%s` SET %s WHERE `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'DELETE FROM `%s` WHERE `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

# base class
class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('Using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # class method
    # use： cls().function()
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' 根据条件查询 '
        # 将sql装配成一个列表，用于下列的拼接操作
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        # 将args装配成一个空列表，用于下列的拼接操作（存放limit参数）
        if args is None:
            args = []
        orderBy = kw.get('order by', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            # limit接受一个或两个数字参数，否则抛出异常
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                # extend也类似于拼接，用新列表追加到原来的列表后
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        # 调用select方法并传入拼接好的sql语句和参数，其中sql列表用空格间隔
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' 查询数据条数 '
        # 其中_num_是列名的代替名，返回一条数据时适用，如果返回多条数据建议去掉（同时去掉返回值中的['_num_']）
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        # sql = ['select %s from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        # 因为输出的数据条数在一行显示，所以传入数值1
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        # rs[0]返回 列名:条数，例如：{'_num_': 15}
        # rs[0]['_num_']返回 {'_num_': 15}中'_num_'的数据，运行结果为15
        return rs[0]['_num_']
        # return rs[0]

    @classmethod
    async def find(cls, pk):
        ' 根据主键查询 '
        # 此处直接引用metaclass定义过的__select__语句拼接where条件语句
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows == 0:
            logging.info('Failed to update by primary key')
        else:
            logging.info('Succeed to update by primary key: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows == 0:
            logging.info('Failed to update by primary key')
        else:
            logging.info('Succeed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows == 0:
            logging.info('Failed to update by primary key')
        else:
            logging.info('Succeed to update by primary key: affected rows: %s' % rows)
