import logging; logging.basicConfig(level=logging.INFO)
import asyncio
import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称:
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名:
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

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
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

class Model(dict, metaclass=ModelMetaclass):
    ' 定制类 '
    def __init__(self, **kw):
        ' 初始化 '
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        ' 获取值，如果取不到值抛出异常 '
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        ' 根据Key,Value设置值 '
        self[key] = value

    def getValue(self, key):
        ' 根据Key获取Value '
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        ' 获取某个属性的值，如果该对象的该属性还没有赋值，就去获取它对应的列的默认值 '
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # @classmethod表明该方法是类方法，类方法不需要实例化类就可以被类本身调用，第一个参数必须是cls，cls表示自身类，可以来调用类的属性、类的方法、实例化对象等
    # cls调用类方法时必须加括号，例如：cls().function()
    # 不使用@classmethod也可以被类本身调用，前提是方法不传递默认self参数，例如：def function()
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
        ' 新增 '
        # 使用map将每个fields属性传入getValueOrDefault方法，获取值后返回成列表
        args = list(map(self.getValueOrDefault, self.__fields__))
        # 单独将主键传入getValueOrDefault方法，获取值后拼接
        args.append(self.getValueOrDefault(self.__primary_key__))
        # 传入插入语句和参数并执行
        rows = await execute(self.__insert__, args)
        if rows == 0:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
        else:
            logging.info('succeed to update by primary key: affected rows: %s' % rows)

    async def update(self):
        ' 更新 '
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows == 0:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
        else:
            logging.info('succeed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        ' 删除 '
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows == 0:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
        else:
            logging.info('succeed to update by primary key: affected rows: %s' % rows)
