import websocket
import zlib    #压缩相关的库
import json
import threading
import hashlib
import time
from pymongo import MongoClient

#输入OKEx账户的api key与secret key（v1版本）
api_key=''
secret_key =''


#所有返回数据都进行了压缩，需要用户将接收到的数据进行解压。解压函数如下
def inflate(data):
    decompress = zlib.decompressobj(-zlib.MAX_WBITS)
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

#签名函数，订阅个人信息，买卖等都需要签名
def buildMySign(params,secretKey):
    sign = ''
    for key in sorted(params.keys()):
        sign += key + '=' + str(params[key]) +'&'
    return  hashlib.md5((sign+'secret_key='+secretKey).encode("utf-8")).hexdigest().upper()

#返回签名的信息
def wsGetAccount(channel,api_key,secret_key):
    params = {
      'api_key':api_key,
    }
    sign = buildMySign(params,secret_key)
    return "{'event':'addChannel','channel':'"+channel+"','parameters':{'api_key':'"+api_key+"','sign':'"+sign+"'}}"


#每当有消息推送时，就会触发，信息包含为message，注意在这里也可以使用ws.send()发送新的信息。
def on_message(ws, message):
    try:
        inflated = inflate(message).decode('utf-8')  #将okex发来的数据解压
    except Exception as e:
        print(e)
    if inflated == '{"event":"pong"}':  #判断推送来的消息类型：如果是服务器的心跳
        print("Pong received.")

    print(inflated)
    # 转换为字典形式
    k_inflated = json.loads(inflated)

    # 获取到开始时间、开盘价格、最高价格、最低价格、收盘价格、交易量(张)、按币折算的交易量、合约的列表
    k_data_candle = k_inflated['data'][0]['candle']

    # 数据存入mongodb数据库
    EOS_quarn.insert({'timestamp': k_data_candle[0],'open': k_data_candle[1],'high': k_data_candle[2],'low': k_data_candle[3],
                      'close': k_data_candle[4],'volume': k_data_candle[5],'currency_volume': k_data_candle[6],'instrument_id':k_inflated['data'][0]['instrument_id']})


#出现错误时执行
def on_error(ws, error):
    print(error)

#关闭连接时执行
def on_close(ws):
    print("### closed ###")

#开始连接时执行，需要订阅的消息和其它操作都要在这里完成
#连接上ws后30s未订阅或订阅后30s内服务器未向用户推送数据，系统会自动断开连接
def on_open(ws):
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:BTC-USD-190927"]}')
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:LTC-USD-190927"]}')
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:ETH-USD-190927"]}')
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:ETC-USD-190927"]}')
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:XRP-USD-190927"]}')
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:EOS-USD-190927"]}')
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:BCH-USD-190927"]}')
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:BSV-USD-190927"]}')
    ws.send('{"op": "subscribe", "args": ["futures/candle60s:TRX-USD-190927"]}')



    #如果需要订阅多条数据，可以在下面使用ws.send方法来订阅
    # 其中 op 的取值为 1--subscribe 订阅； 2-- unsubscribe 取消订阅 ；3--login 登录
    # args: 取值为频道名，可以定义一个或者多个频道


#发送心跳数据
def sendHeartBeat(ws):
    ping = '{"event":"ping"}'
    while(True):
        time.sleep(30) #每隔30秒交易所服务器发送心跳信息
        sent = False
        while(sent is False): #如果发送心跳包时出现错误，则再次发送直到发送成功为止
            try:
                ws.send(ping)
                sent = True
                print("Ping sent.")
            except Exception as e:
                print(e)

#创建websocket连接
def ws_main():
    websocket.enableTrace(True)
    host = "wss://real.okex.com:10442/ws/v3"
    ws = websocket.WebSocketApp(host,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    threading.Thread(target=sendHeartBeat, args=(ws,)).start() #新建一个线程来发送心跳包
    ws.run_forever()    #开始运行


if __name__ == "__main__":
    print('正在连接数据库...')
    client = MongoClient(host='localhost', port=27017)
    db = client.OKEX
    EOS_quarn = db.EOS_quarn
    print('数据库连接成功...')

    trade = 0
    threading.Thread(target=ws_main).start()

    while True:
        #这里是需要进行的任务，下单的策略可以安排在这里
        time.sleep(3)
        # print(trade[3], trade[1]) #打印价格和时间信息到控制台
        # Do something





