import mongo_du as du
import common.conf as conf
import arrow, logging as logging_size

now_time = arrow.now().timestamp


# 统计数组中各个元素出现的次数
def all_list(arr):
    result = {}
    for i in set(arr):
        result[i] = arr.count(i)
    return result


# 获取尺码销量
async def getSizeSoldNum(client, productInfo):
    try:
        productId = productInfo['productId']
        articleNumber = productInfo['articleNumber']

        url = du.getApiUrl(du.URL['size'], {
            'productId': str(productId),
            'lastId': '',
            'limit': '20',
        })
        res = await du.fetch(client, url)

        # 获取数据库中尺码销量 上次爬取时间
        where = {'articleNumber': articleNumber}
        ret = du.db_sold.find_one(where)
        if ret is None:
            lastId = 0
        else:
            # 判断今天是否已经爬取过   今日凌晨时间-爬取时间 < 0 则未爬取过
            is_spider = arrow.now().floor('day').timestamp - int(ret['updateTime'])
            if is_spider < 0:
                print("今日已爬取")
                return
            lastId = ret['lastId']

        # 用来统计各个尺码卖出去了多少
        sizeSold = []
        # 用来统计最大的lastId
        lastId_arr = []

        # 如果还有下一页  并且获取的数据的时间小于最终爬取时间
        while res['data']['lastId'] != '' and int(res['data']['lastId']) > int(lastId):
            lastId_arr.append(res['data']['lastId'])
            for v in res['data']['list']:
                temp_size = v['item']['size']
                sizeSold.append(temp_size)

            url = du.getApiUrl(du.URL['size'], {
                'productId': str(productId),
                'lastId': res['data']['lastId'],
                'limit': '20',
            })
            await du.asyncio.sleep(1)
            res = await du.fetch(client, url)

        print('articleNumber:', articleNumber, "  爬取完毕 开始统计各尺码销量")

        # 统计后的结果
        if len(sizeSold) != 0:
            new_arr = all_list(sizeSold)
            total = 0
            for k, v in new_arr.items():
                total += v
                data = {
                    'productId': productId,
                    'articleNumber': articleNumber,
                    'size': k,
                    'soldNum': v,
                    'spiderTime': now_time,
                    'updateTime': now_time,
                    'lastId': max(lastId_arr),
                }
                du.asyncio.ensure_future(insertSizeSold(data))

    except:
        logging_size.error("[尺码销量] error!:" + str(du.traceback.format_exc()))
        du.traceback.print_exc()


async def insertSizeSold(data):
    try:

        # 判断数据是否已经存在
        where = {
            'articleNumber': data['articleNumber'],
            'size': data['size'],
        }
        ret = du.db_sold.find_one(where)

        if ret is None:
            # 添加销量记录
            ret_add = du.db_sold.insert_one(data)
            ret_add2 = du.db_sold_record.insert_one({
                'articleNumber': data['articleNumber'],
                'size': data['size'],
                'soldNum': data['soldNum'],
                'spiderTime': now_time,
            })
            print("[插入销量]：", ret_add.inserted_id)
            print("[插入销量记录]：", ret_add2.inserted_id)
            msg = "articleNumber:" + str(data['articleNumber']) + ' size: ' + str(
                data['size']) + ' soldNum:' + str(data['soldNum'])
            print(msg)
        else:
            # 加上今天爬取的销量
            soldNum = ret['soldNum'] + data['soldNum']
            # 修改商品销量,lastId
            ret_edit = du.db_sold.update_one(where, {'$set': {
                'soldNum': soldNum,
                'updateTime': data['updateTime'],
                'lastId': data['lastId']
            }})
            if ret_edit.modified_count == 1:
                print("修改成功: ", data['articleNumber'], ' 尺码：', data['size'])
                ret_add = du.db_sold_record.insert_one({
                    'articleNumber': data['articleNumber'],
                    'size': data['size'],
                    'soldNum': soldNum,
                    'add': data['soldNum'],
                    'spiderTime': now_time,
                })

                print("[插入新销量记录]： ", ret_add.inserted_id)
                msg = "articleNumber:" + str(data['articleNumber']) + ' size: ' + str(
                    data['size']) + ' soldNum:' + str(ret['soldNum']) + " add: +" + str(data['soldNum'])
                print(msg)
            else:
                print("没有任何修改")
    except:
        logging_size.error("[插入尺码销量] error!:" + str(du.traceback.format_exc()))
        du.traceback.print_exc()


# 获取所有商品列表
async def getAllList(client):
    try:

        ret = du.db_product.find()
        if ret is None:
            print("商品列表为空")
            return
        for v in ret:
            task = du.asyncio.create_task(
                getSizeSoldNum(client, {'productId': v['productId'], 'articleNumber': v['articleNumber']}))
            await du.asyncio.sleep(2)

        done, pending = await du.asyncio.wait({task})

        if task in done:
            print('[主程2]所有商品列表size统计完毕')
            logging_size.info("[主程2]所有商品列表size统计完毕")
    except:
        logging_size.error("[爬取详情] error!:" + str(du.traceback.format_exc()))
        du.traceback.print_exc()


async def main(loop):
    # 清除超过30天的数据，只保留30天的数据
    day_30 = arrow.now().floor('day').timestamp - 3600 * 24 * conf.clear_day

    ret_del = du.db_sold_record.delete_many({'spiderTime': {'$lt': day_30}})

    msg = "[清除30天数据] " + " 条件时间： " + str(
        arrow.get(day_30).to('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')) + "已清除： " + str(
        ret_del.deleted_count) + ' 条'
    print(msg)
    logging_size.info(msg)

    # 建立 client request
    async with du.aiohttp.ClientSession() as client:
        task = du.asyncio.create_task(getAllList(client))

        done, pending = await du.asyncio.wait({task})

        if task in done:
            print('[主程]所有商品列表size统计完毕')
            logging_size.info("[主程]所有商品列表size统计完毕")


if __name__ == '__main__':
    start_time = arrow.now().timestamp

    # 日志配置
    log_name = "log/mongo_du_sold.log"
    logging_size.basicConfig(level=logging_size.DEBUG,
                             format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                             datefmt='%a, %d %b %Y %H:%M:%S', filename=log_name, filemode='a')
    # 获取用户token
    du.getToken()

    loop = du.asyncio.get_event_loop()
    task = du.asyncio.ensure_future(main(loop))
    loop.run_until_complete(task)

    end_time = arrow.now().timestamp
    use_time = end_time - start_time

    msg = '总耗时: ' + str(use_time) + " 开始时间: " + str(
        arrow.get(start_time).to('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')) + "  结束时间: " + str(
        arrow.get(end_time).to('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss'))
    print(msg)
    logging_size.info(msg)
