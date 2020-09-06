import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web
from jinja2 import Environment, FileSystemLoader

import orm
from coroweb import add_routes, add_static

from config import configs

from handlers import cookie2user, COOKIE_NAME

# 初始化jinja2模板
def init_jinja2(app, **kw):
    logging.info('Init jinja2...')
    # 初始化模板配置，包括模板运行代码的开始和结束标识符，变量的开始结束标识符等
    options = dict(
        autoescape = kw.get('autoescape', True),                        # 自动转义，渲染模板时自动把变量中<>&等字符转换为&lt;&gt;&amp;
        block_start_string = kw.get('block_start_string', '{%'),        # 运行代码开始标识符
        block_end_string = kw.get('block_end_string', '%}'),            # 运行代码结束标识符
        variable_start_string = kw.get('variable_start_string', '{{'),  # 变量开始标识符
        variable_end_string = kw.get('variable_end_string', '}}'),      # 变量结束标识符
        auto_reload = kw.get('auto_reload', True)                       # 当模板有修改时，是否重新加载模板
    )
    # 尝试从参数中获取path信息，即模板文件位置
    path = kw.get('path', None)
    # 参数中没有路径信息，则默认当前文件目录下的templates目录
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    logging.info('Set jinja2 template path: %s' % path)
    # Environment是jinja2中的一个核心类，它的实例用来保存配置、全局对象，以及从本地文件系统或其它位置加载模板
    env = Environment(loader=FileSystemLoader(path), **options)
    # 尝试从参数中获取filters字段
    filters = kw.get('filters', None)
    # 如果有filters字段，则设置为env的过滤器集合
    if filters is not None:
        for name, f in filters.items():
            env.filters[name] = f
    app['__templating__'] = env

async def logger_factory(app, handler):
    async def logger(request):
        logging.info('Request: %s' % (request))
        logging.info('Request method: %s' % (request.method))
        logging.info('Request path: %s' % (request.path))
        # await asyncio.sleep(0.3)
        return (await handler(request))
    return logger

# 验证cookie
async def auth_factory(app, handler):
    async def auth(request):
        logging.info('check user: %s %s' % (request.method, request.path))
        request.__user__ = None
        cookie_str = request.cookies.get(COOKIE_NAME)
        if cookie_str:
            user = await cookie2user(cookie_str)
            if user:
                logging.info('set current user: %s' % user.email)
                request.__user__ = user
        if request.path.startswith('/manage/') and (request.__user__ is None or not request.__user__.admin):
            return web.HTTPFound('/signin')
        return (await handler(request))
    return auth

async def data_factory(app, handler):
    async def parse_data(request):
        if request.method == 'POST':
            if request.content_type.startswith('application/json'):
                request.__data__ = await request.json()
                logging.info('request json: %s' % str(request.__data__))
            elif request.content_type.startswith('application/x-www-form-urlencoded'):
                request.__data__ = await request.post()
                logging.info('request form: %s' % str(request.__data__))
        return (await handler(request))
    return parse_data

async def response_factory(app, handler):
    async def response(request):
        logging.info('Response handler...')
        r = await handler(request)
        logging.info('End response handler')
        logging.info('Return: %s' % r)
        if isinstance(r, web.StreamResponse):
            return r
        if isinstance(r, bytes):
            resp = web.Response(body=r)
            resp.content_type = 'application/octet-stream'
            return resp
        if isinstance(r, str):
            logging.info('this is str')
            if r.startswith('redirect:'):
                return web.HTTPFound(r[9:])
            resp = web.Response(body=r.encode('utf-8'))
            resp.content_type = 'text/html;charset=utf-8'
            return resp
        if isinstance(r, dict):
            logging.info('this is dict')
            template = r.get('__template__')
            if template is None:
                resp = web.Response(body=json.dumps(r, ensure_ascii=False, default=lambda o: o.__dict__).encode('utf-8'))
                resp.content_type = 'application/json;charset=utf-8'
                return resp
            else:
                resp = web.Response(body=app['__templating__'].get_template(template).render(**r).encode('utf-8'))
                resp.content_type = 'text/html;charset=utf-8'
                logging.info('resp: %s' % resp)
                return resp
        if isinstance(r, int) and r >= 100 and r < 600:
            return web.Response(r)
        if isinstance(r, tuple) and len(r) == 2:
            t, m = r
            if isinstance(t, int) and t >= 100 and t < 600:
                return web.Response(t, str(m))
        # default:
        resp = web.Response(body=str(r).encode('utf-8'))
        resp.content_type = 'text/plain;charset=utf-8'
        return resp
    return response

def datetime_filter(t):
    delta = int(time.time() - t)
    dt = datetime.fromtimestamp(t)
    return u'%s, %s, %s' % (dt.year, dt.month, dt.day)
'''
    if delta < 60:
        return u'1 minute ago'
    if delta < 3600:
        return u'%s minutes ago' % (delta // 60)
    if delta < 86400:
        return u'%s hours ago' % (delta // 3600)
    if delta < 604800:
        return u'%s days ago' % (delta // 86400)
    dt = datetime.fromtimestamp(t)
    return u'%s, %s, %s' % (dt.year, dt.month, dt.day)
'''

async def init(loop):
    # 创建数据库连接池，db参数传配置文件里的配置db
    # await orm.create_pool(loop=loop, host='127.0.0.1', port=3306, user='root', password='ming123', db='awesome')
    await orm.create_pool(loop=loop, **configs.db)
    # middlewares设置两个中间处理函数
    # middlewares处理函数接受两个参数，app和handler，按照顺序执行，前一个函数的handler是后一个函数
    # middlewares最后一个处理函数的handler会通过routes查找到相应的注册的hander
    app = web.Application(loop=loop, middlewares=[
        logger_factory, auth_factory, response_factory
    ])
    # 初始化jinja2模板
    init_jinja2(app, filters=dict(datetime=datetime_filter))
    # 添加请求的handlers，即各请求相对应的处理函数
    add_routes(app, 'handlers')
    # 添加静态文件所在地址
    add_static(app)
    # 启动
    # srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    # return srv
    app_runner = web.AppRunner(app)
    await app_runner.setup()
    site = web.TCPSite(app_runner, '127.0.0.1', 9000)
    logging.info('Server started at http://127.0.0.1:9000...')
    await site.start()

# 入口，固定写法
# 获取eventloop，加入运行事件
loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()