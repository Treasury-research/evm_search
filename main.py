import requests
import os
import json
import time
from dbutils.pooled_db import PooledDB
import pymysql
from datetime import datetime, timedelta
import uuid
import logging
import random



def scarper(url):
    # Define the URL of the API
    my_headers = [
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
    'Opera/9.25 (Windows NT 5.1; U; en)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 ",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/118.0.0 Safari/537.36",
    ]
    # Define the headers, typically content type is set to application/json for POST requests with JSON body
    headers = {
        'Content-Type': 'application/json',
        'Origin': 'https://evm.ink',
        'Referer': 'https://evm.ink/',
        'User-Agent': random.choice(my_headers)
    }

    # BSC\Eth\Pol
    network_ids = ["eip155:56","eip155:1","eip155:137"]
    data_json = {}
    # Define the JSON payload for the POST request
    for nid in network_ids:
        data_json[nid] = []
        offset = 0
        limit = 1000
        flag = True 
        while flag:
            payload = {
            "query": "query GetBrc20Tokens($limit: Int = 10, $offset: Int = 0, $order_by: [brc20_tokens_order_by!] = {}, $where: brc20_tokens_bool_exp = {}) {\n  brc20_tokens(limit: $limit, offset: $offset, order_by: $order_by, where: $where) {\n    decimal_digits\n    decimals\n    max_supply\n    mint_limit\n    minted_total\n    protocol\n    network_id\n    created_at\n    stats {\n      holders\n    }\n    tick\n  }\n}",
            "variables": {
                "limit": limit,
                "offset": offset,
                "where": {
                    "max_supply": {
                        "_neq": "0"
                    },
                    "network_id": {
                        "_eq": nid
                    }
                },
                "order_by": [
                    {
                        "created_at": "asc"
                    }
                ]
            },
            "operationName": "GetBrc20Tokens"
            }

            # Make the POST request
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            offset += limit
            # Check if the request was successful
            if response.status_code == 200:
                # Process the response here
                data = response.json()
                data_json[nid].extend(data['data']['brc20_tokens'])
                # last get data
                if len(data['data']['brc20_tokens'])<1000:
                    flag = False
            else:
                print("Error:", response.status_code, response.text)
                flag = False
    return data_json


def updateSQL(data_json, pool):
    chainId2name = {"eip155:56":"BNB Smart Chain","eip155:1":"Ethereum","eip155:137":"Polygon"}
    batch_data = []
    for key,value in data_json.items():
        # key is token name; value is all data
        for i in range(len(value)):
            item = value[i]
            token = item['tick']
            chain_id = item['network_id']
            chain = chainId2name[chain_id]
            protocol = item['protocol']
            total_supply = int(item['max_supply'])/int(item['decimal_digits'])
            minted = float(int(item['minted_total'])/int(item['max_supply']))
            mint_limit = int(item['mint_limit'])/int(item['decimal_digits'])
            owners = 0
            try:
                if item['stats']!=None and item['stats'].get('holders')!= None :
                    owners = item['stats']['holders']
            except:
                print(i,item)
            total_minted = int(item['minted_total'])/int(item['decimal_digits'])
            created_at = item['created_at']
            generated_uuid = str(uuid.uuid4())
            batch_data.append((token, chain, chain_id, protocol, total_minted, total_supply, minted, mint_limit, owners, created_at,generated_uuid))
    # insert or update the data
    batch_size = 1000
    conn = pool.connection()
    cursor = conn.cursor()
    for i in range(0, len(batch_data), batch_size):
        new_data = batch_data[i:i + batch_size]
        # sql = """
        # INSERT INTO evm_ink (Token, Chain, Chain_id, Protocol, Total_Minted, Total_Supply, Minted, Mint_Limit, Owners, Update_At)
        # VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        # """
        sql = """
                INSERT INTO evm_ink (Token, Chain, Chain_id, Protocol, Total_Minted, Total_Supply, Minted, Mint_Limit, Owners,CreatedAt,id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Chain = VALUES(Chain),
                    Total_Minted = VALUES(Total_Minted),
                    Total_Supply = VALUES(Total_Supply),
                    Minted = VALUES(Minted),
                    Mint_Limit = VALUES(Mint_Limit),
                    Owners = VALUES(Owners),
                    CreatedAt = VALUES(CreatedAt),
                    id = VALUES(id)
            """
        cursor.executemany(sql, new_data)
        conn.commit()
    # # get the old data
    # cursor.execute("SELECT MIN(updatedAt) FROM evm_ink")
    # latest_updated_at = cursor.fetchone()[0]
    # # 删除非最新updatedAt时间的记录
    # delete_query = "DELETE FROM evm_ink WHERE updatedAt = %s"
    # cursor.execute(delete_query, (latest_updated_at,))

    conn.commit()
    cursor.close()
    conn.close()
    print("Update data success!")

    
if __name__ =="__main__":
    username = os.environ["DBusername"]
    password = os.environ["DBpassword"]
    host = os.environ["DBhostname"]
    port = os.environ["DBport"] 
    database = os.environ["DBdbname"] 
    config = {
        'user': username,
        'password': password,
        'host': host,
        'port': port,
        'database': database
    }
    # 连接池配置
    pool = PooledDB(
        creator=pymysql,  # 使用的数据库模块
        maxconnections=10,  # 连接池最大连接数量
        mincached=2,       # 初始化时，连接池中至少创建的空闲的连接
        maxcached=5,       # 连接池中最多闲置的连接
        maxshared=3,       # 连接池中最多共享的连接数量
        blocking=True,     # 连接池中如果没有可用连接后是否阻塞等待
        host=host,
        port=int(port),
        user=username,
        password=password,
        database=database,
        ssl={"ssl_mode":"VERIFY_IDENTITY",
            "ssl_accept":"strict"
        }
    )
    # 配置日志记录
    logging.basicConfig(filename='evm.log', level=logging.DEBUG, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    url = 'https://api.evm.ink/v1/graphql/' 
    while True:
        data_json = scarper(url)
        logging.info('scarper data success!')
        logging.info('update data ing')
        updateSQL(data_json,pool)
        logging.info('Update data success!')
        logging.info('sleep 1 hour now!')
        time.sleep(3600)  # 休眠1小时 (3600秒)
