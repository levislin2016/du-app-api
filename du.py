import logging
import time

import common.conf as conf
import common.function as myFunc
import hashlib
import arrow
import requests, traceback
import aiohttp, asyncio, aiomysql, pymysql, pymongo

# header头设置
HEADERS = {
    'duuuid': '309c23acc4954021',
    'duv': '3.5.5',
    'duplatform': 'android',
}
COOKIES = {}
# 用户设置
USER = {
    'userName': '手机号',
    'password': '密码',
    'type': 'pwd',
    'sign': '用户登录sign、固定值',
    'sourcePage': '',
    'countryCode': '86',
}
# 域名设置
URL = {
    # 域名
    'domain': 'https://du.hupu.com',
    # 登录url
    'login': '/users/unionLogin',
    # 商品列表地址
    'list': '/search/list',
    # 详情地址
    'detail': '/product/detail',
    # 尺码
    'size': '/product/lastSoldList',
}

# 商品爬取配置
PRODUCT = {
    'isSellDate': False,
    'sellDate': '2018'
}
# 当前时间设置
now_time = arrow.now().timestamp

sem = asyncio.Semaphore(conf.async_num)

# 连接mongodb
myclient = pymongo.MongoClient("mongodb://" + conf.mongo['user'] + ':' + conf.mongo['passwd'] + '@' + conf.mongo['host'] + ':' + conf.mongo['port'])
# myclient = pymongo.MongoClient("mongodb://" + conf.mongo['host'] + ':' + conf.mongo['port'])

mydb = myclient["du"]

db_product = mydb["du_product"]
db_size = mydb["du_size"]
db_sold = mydb['du_sold']
db_sold_record = mydb['du_sold_record']
db_price = mydb['price']
db_login = mydb['login']
db_change = mydb['change']


# 登录状态测试
def tokenTest():
    i = 1
    while i <= 3:
        try:
            url = getApiUrl(URL['detail'], {
                'productId': str(9670),
                'isChest': str(0),
            })

            # 等待返回结果
            data = requests.get(url, headers=HEADERS)
            data = data.json()
            if data['status'] == 700:
                msg = "登录已失效 需要重新登录！"
                print(msg)
                logging.info(msg)
                getToken(True)
            else:
                msg = "保持登录！"
                print(msg)
                logging.info(msg)

            return
        except:
            time.sleep(5)
            print("[尝试重连] 第 " + str(i) + ' 尝试重连URL:' + url)
            logging.error("[尝试重连] 第 " + str(i) + ' 尝试重连URL:' + url)
            i += 1


# 获取用户登录的token
def getToken(force=False):
    try:
        db = pymysql.connect(host=conf.database['host'], port=conf.database['port'],
                             user=conf.database['user'], password=conf.database['passwd'],
                             db=conf.database['db'], charset='utf8')
        cursor = db.cursor()
        mysql_data = {}
        # 获取数据库token
        sql = myFunc.selectSql(conf.TABLE['token'], {'id': 2}, ['val', 'spiderTime'])
        cursor.execute(sql)
        ret_token = cursor.fetchone()
        # 都有数据的情况下  爬取时间不超三天则不重新登录
        if not (ret_token[1] is None) and not (ret_token[0] is None):
            mysql_data['token'] = ret_token[0]

        # 获取数据库cookie
        sql = myFunc.selectSql(conf.TABLE['token'], {'id': 3}, ['val', 'spiderTime'])
        cursor.execute(sql)
        ret_cookie = cursor.fetchone()
        # 都有数据的情况下  爬取时间不超三天则不重新登录
        if not (ret_cookie[1] is None) and not (ret_cookie[0] is None):
            mysql_data['cookie'] = ret_cookie[0]

        if 'token' in mysql_data and 'cookie' in mysql_data and not force:
            HEADERS['duloginToken'] = mysql_data['token']
            HEADERS['Cookie'] = mysql_data['cookie']
            print('获取数据库 token，cookie', HEADERS)
            return

        # 重置
        HEADERS['Cookie'] = ''
        HEADERS['duloginToken'] = ''
        # 重新登录
        ret = requests.post(URL['domain'] + URL['login'], data=USER, headers=HEADERS)

        if ret.status_code != 200:
            print("获取用户token失败")
            return

        ret_data = ret.json()
        if ret_data['status'] != 200:
            print(ret_data['msg'])
            return

        # 设置cookie
        HEADERS['Cookie'] = ret.headers['Set-Cookie']
        sql = myFunc.updateSql(conf.TABLE['token'], {
            'val': HEADERS['Cookie'],
            'spiderTime': now_time,
        }, {'key': 'cookie'})
        cursor.execute(sql)

        # 设置用户登录token
        HEADERS['duloginToken'] = ret_data['data']['loginInfo']['loginToken']
        sql = myFunc.updateSql(conf.TABLE['token'], {
            'val': ret_data['data']['loginInfo']['loginToken'],
            'spiderTime': now_time,
        }, {'key': 'token'})
        cursor.execute(sql)
        db.close()

        msg = "重新登录！"
        print(msg)
        logging.info(msg)
    except:
        traceback.print_exc()
        logging.error(traceback.format_exc())


# 获取签名p
def getSign(api_params):
    hash_map = {
        "uuid": HEADERS["duuuid"],
        "platform": HEADERS["duplatform"],
        "v": HEADERS["duv"],
        "loginToken": HEADERS["duloginToken"],
    }

    for k in api_params:
        hash_map[k] = api_params[k]

    hash_map = sorted(hash_map.items(), key=lambda x: x[0])

    str = ''
    for v in hash_map:
        str += v[0] + v[1]

    str += "重要参数用于接口sign加密。"

    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(str.encode("GBK"))
    sign = m1.hexdigest()
    return sign


# 生成带签名的url
def getApiUrl(api_url, api_params):
    url = URL['domain']
    # 拼接域名
    url += api_url

    # 拼接参数
    url += '?'
    for k in api_params:
        url += k + '=' + api_params[k] + '&'
    # 获取sign
    sign = getSign(api_params)
    url += 'sign=' + sign

    return url


# 组装最终访问链接
async def fetch(client, url):
    async with sem:
        i = 1
        while i <= 3:
            try:
                async with client.get(url, headers=HEADERS, timeout=30) as res:
                    assert res.status == 200
                    # <coroutine object ClientResponse.text at 0x109b8ddb0>
                    # 要获取HTML页面的内容, 必须在 resp.json() 前面使用 await
                    res_json = await res.json()
                    if res_json['status'] != 200:
                        print(res_json)
                        return
                    print('URL: ', url)
                    return res_json
            except:
                await asyncio.sleep(5)
                print("[尝试重连] 第 " + str(i) + ' 尝试重连URL:' + url)
                logging.error("[尝试重连] 第 " + str(i) + ' 尝试重连URL:' + url)
                i += 1
        logging.error('[尝试重连] 失败！ URL:' + url)
        return False


# 获取列表
async def getList(client, page):
    try:
        url = getApiUrl(URL['list'], {
            "size": "[]",
            "title": "",
            "typeId": "0",
            "catId": "0",
            "unionId": "0",
            "sortType": "0",
            "sortMode": "1",
            "page": str(page),
            "limit": "20",
        })

        # 等待返回结果
        data = await fetch(client, url)
        if not data:
            return

        productList = data['data']['productList']


        # 如果商品列表为空不再爬取
        if len(productList) == 0:
            return

        for v in productList:
            asyncio.ensure_future(getDetail(client, v['productId']))
    except:
        traceback.print_exc()
        logging.error("[爬取列表] error:" + traceback.format_exc())


# 获取商品详情
async def getDetail(client, productId):
    try:

        url = getApiUrl(URL['detail'], {
            'productId': str(productId),
            'isChest': str(0),
        })
        ret_data = await fetch(client, url)
        if not ret_data:
            return

        # 插入对象赋值
        info = ret_data['data']
        info_arr = {
            'articleNumber': info['detail']['articleNumber'],
            'productId': info['detail']['productId'],
            'authPrice': str(info['detail']['authPrice']),
            'logoUrl': pymysql.escape_string(info['detail']['logoUrl']),
            'title': pymysql.escape_string(info['detail']['title']),
            'soldNum': info['detail']['soldNum'],
            'sellDate': info['detail']['sellDate'],
            'spiderTime': now_time,
            'updateTime': now_time,
        }

        asyncio.ensure_future(insert(info_arr, info['sizeList']))

    except:
        traceback.print_exc()
        logging.error("[爬取详情] error!:" + str(traceback.format_exc()))


async def insert(info_arr, sizeList):
    try:
        # 只记录2018年的新款商品
        # if PRODUCT['isSellDate']:
        #     if str(info_arr['sellDate'][0:4]) != '2018':
        #         return

        where = {'articleNumber': info_arr['articleNumber']}
        ret = db_product.find_one(where)

        if ret is not None:
            is_spider = arrow.now().floor('day').timestamp - ret['updateTime']
            # 判断今天是否已经爬取过   今日凌晨时间-爬取时间 < 0 则未爬取过
            if is_spider < 0:
                print("[今日已爬取]：", info_arr['articleNumber'])
                return

            ret_edit = db_product.update_one(where, {'$set': {
                'authPrice': info_arr['authPrice'],
                'soldNum': info_arr['soldNum'],
                'updateTime': info_arr['spiderTime'],
            }})
            if ret_edit.modified_count == 1:
                print("[修改成功]：", info_arr['articleNumber'])
            else:
                print("没有任何修改")

        else:
            # 添加商品
            ret_add = db_product.insert_one(info_arr)

            msg = [info_arr['articleNumber']]
            if ret_add:
                print("[插入成功]：", " ".join('%s' % id for id in msg))
            else:
                print("[插入失败]：", " ".join('%s' % id for id in msg))

        # 记录鞋子的各类尺码
        size_arr = []
        for v in sizeList:
            if 'price' in v['item'] and v['item']['price'] != 0:
                size_arr.append({
                    'articleNumber': info_arr['articleNumber'],
                    'size': v['size'],
                    'formatSize': v['formatSize'],
                    'price': v['item']['price'],
                    'spiderTime': now_time,
                })

        asyncio.ensure_future(insertSize(size_arr))

    except:
        traceback.print_exc()
        logging.error("[插入商品] error!:" + str(traceback.format_exc()))


# 记录尺码信息
async def insertSize(size_arr):
    try:
        if not size_arr:
            print("尺码信息为空")
            return

        ret_add = db_size.insert_many(size_arr)

        if ret_add.acknowledged:
            for v in size_arr:
                print('[插入尺码成功]：', v['articleNumber'], v['size'])
        else:
            for v in size_arr:
                print('[插入尺码失败]：', v['articleNumber'], v['size'])
    except:
        traceback.print_exc()
        logging.error("[插入尺码] error!:" + str(traceback.format_exc()))


async def main():
    try:
        # 清除超过30天的数据，只保留30天的数据
        day_30 = arrow.now().floor('day').timestamp - 3600 * 24 * conf.clear_day

        ret_del = db_size.delete_many({'spiderTime': {'$lt': day_30}})
        msg = "[清除30天数据] " + " 条件时间： " + str(
            arrow.get(day_30).to('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')) + "已清除： " + str(
            ret_del.deleted_count) + ' 条'
        print(msg)
        logging.info(msg)

        # 建立 client request
        async with aiohttp.ClientSession() as client:
            for page in range(1, 400):
                task = asyncio.create_task(getList(client, page))
                await asyncio.sleep(5)

            done, pending = await asyncio.wait({task})

            if task in done:
                print('[爬取完成]所有爬取进程已经全部完成')
                logging.info("[爬取完成]所有爬取进程已经全部完成")
    except:
        traceback.print_exc()


if __name__ == '__main__':
    start_time = arrow.now().timestamp

    # 日志配置
    log_name = "log/mongo_du.log"
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=log_name, filemode='a')

    try:
        # 获取用户token
        getToken()
        tokenTest()

        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(main())
        loop.run_until_complete(task)

    except:
        logging.error(traceback.format_exc())

    end_time = arrow.now().timestamp
    use_time = end_time - start_time

    msg = '总耗时: ' + str(use_time) + " 开始时间: " + str(
        arrow.get(start_time).to('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')) + "  结束时间: " + str(
        arrow.get(end_time).to('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss'))
    print(msg)
    logging.info(msg)
