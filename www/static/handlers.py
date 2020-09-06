#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

' url handlers '

import re, time, json, logging, hashlib, base64, asyncio

from aiohttp import web

from coroweb import get, post
from apis import APIError, APIValueError, APIResourceNotFoundError

from models import User, Comment, Blog, next_id
from config import configs

COOKIE_NAME = 'awesession'
_COOKIE_KEY = configs.session.secret

# 计算加密cookie
def user2cookie(user, max_age):
    '''
    Generate cookie str by user.
    '''
    # build cookie string by: id-expires-sha1
    expires = str(int(time.time() + max_age))
    s = '%s-%s-%s-%s' % (user.id, user.passwd, expires, _COOKIE_KEY)
    L = [user.id, expires, hashlib.sha1(s.encode('utf-8')).hexdigest()]
    return '-'.join(L)

# 解析cookie
async def cookie2user(cookie_str):
    '''
    Parse cookie and load user if cookie is valid.
    '''
    if not cookie_str:
        return None
    try:
        L = cookie_str.split('-')
        if len(L) != 3:
            return None
        uid, expires, sha1 = L
        if int(expires) < time.time():
            return None
        user = await User.find(uid)
        if user is None:
            return None
        s = '%s-%s-%s-%s' % (uid, user.passwd, expires, _COOKIE_KEY)
        if sha1 != hashlib.sha1(s.encode('utf-8')).hexdigest():
            logging.info('invalid sha1')
            return None
        user.passwd = '******'
        return user
    except Exception as e:
        logging.exception(e)
        return None

# 这里我们要注意，处理函数被修饰器get修饰，所以会带有'__method__'和'__route__'属性
# 这里是默认主页
@get('/')
async def index(request):
    logging.info('Run index: %s' % request)
    summary = 'Hello,World.'
    #users = await User.findAll()
    blogs = [
        Blog(id='1', name='测试博客', summary="Transfer/Migrate Web App from Old Server to New Server on AWS EC2. 迁移网站从旧亚马逊服务器到新亚马逊服务器。", create_at=time.time() - 120),
        Blog(id='2', name='新奇发现', summary="We are always living in present, which prevents us to understand the changes through time. This project experiments with virtual reality and narrative to interpret landscape process in Providence.", create_at=time.time() - 3600),
        Blog(id='3', name='如何高效学习Swift', summary="Website design for a small high-tech oriented landscape professional practice -- LOOP Studio. LOOP工作室网站设计。LOOP工作室是一家以未来科技为主导的小型景观公司。", create_at=time.time() - 7200)
    ]
    return {
        '__template__': 'blogs.html',
        'blogs': blogs
    }

# 注册界面
@get('/register')
async def register(request):
    return {
        '__template__': 'register.html',
    }

_RE_EMAIL = re.compile(r'^[a-z0-9\.\-\_]+\@[a-z0-9\-\_]+(\.[a-z0-9\-\_]+){1,4}$')
_RE_SHA1 = re.compile(r'^[0-9a-f]{40}$')

# 注册，获取并验证注册信息，保存到数据库
@post('/register')
async def api_register_user(*, email, name, passwd):
    if not name or not name.strip():
        raise APIValueError('name')
    if not email or not _RE_EMAIL.match(email):
        raise APIValueError('email')
    if not passwd or not _RE_SHA1.match(passwd):
        raise APIValueError('passwd')
    users = await User.findAll('email=?', [email])
    if len(users) > 0:
        raise APIError('register:failed', 'email', 'Email is already in use.')
    uid = next_id()
    sha1_passwd = '%s:%s' % (uid, passwd)
    user = User(id=uid, name=name.strip(), email=email, passwd=hashlib.sha1(sha1_passwd.encode('utf-8')).hexdigest(), image='http://www.gravatar.com/avatar/%s?d=mm&s=120' % hashlib.md5(email.encode('utf-8')).hexdigest())
    await user.save()
    # make session cookie:
    r = web.Response()
    r.set_cookie(COOKIE_NAME, user2cookie(user, 86400), max_age=86400, httponly=True)
    user.passwd = '******'
    r.content_type = 'application/json'
    r.body = json.dumps(user, ensure_ascii=False).encode('utf-8')
    return r

# 登录界面
@get('/signin')
async def register(request):
    return {
        '__template__': 'signin.html',
    }

# 登录
@post('/api/authenticate')
async def authenticate(*, email, passwd):
    if not email:
        raise APIValueError('email', 'Invalid email.')
    if not passwd:
        raise APIValueError('passwd', 'Invalid password.')
    users = await User.findAll('email=?', [email])
    if len(users) == 0:
        raise APIValueError('email', 'Email not exist.')
    user = users[0]
    # check passwd:
    sha1 = hashlib.sha1()
    sha1.update(user.id.encode('utf-8'))
    sha1.update(b':')
    sha1.update(passwd.encode('utf-8'))
    if user.passwd != sha1.hexdigest():
        raise APIValueError('passwd', 'Invalid password.')
    # authenticate ok, set cookie:
    r = web.Response()
    r.set_cookie(COOKIE_NAME, user2cookie(user, 86400), max_age=86400, httponly=True)
    user.passwd = '******'
    r.content_type = 'application/json'
    r.body = json.dumps(user, ensure_ascii=False).encode('utf-8')
    return r