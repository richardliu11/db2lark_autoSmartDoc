# -*- coding: utf-8 -*-
"""
@Time ： 2024/3/18 16:42
@Auth ： Richard
@File ：updateLarkTable.py
@IDE ：PyCharm
"""
import time
import requests
import json
import pandas as pd
import sqlalchemy as sa
from urllib import parse
import datetime
import sqlite3
from sqlalchemy import create_engine
import numpy as np

#base wheels

#sql 取数函数
def get_sql_data(sql):
    bad_password = 'Yunying@LaiPic2021!'
    passowrd = parse.quote_plus(bad_password)

    # 配置链接数据库信息(Fixed)
    db_config = {
        'host': '121.37.225.154',
        'port': '3306',
        'database': 'laihua_new',
        'username': 'yunying',
        'password': passowrd
    }
    # 数据库链接地址
    db_url = 'mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8'.format(**db_config)
    # 创建数据库引擎
    engine = sa.create_engine(db_url)

    sql = sa.text(sql)
    df = pd.read_sql(sql, engine)
    return df


#project functions
#用于获取来画数据库数据
def get_laipicDB_df():
    get_valid_sql = '''
    SELECT CR.pid AS '合并后企业ID',C.id AS '原企业ID',NAME as'企业名称',C.userId ,U.endDay AS '当前会员到期时间',ULN.levelName1 AS '当前会员等级'
        FROM `COMPANY` C
        LEFT JOIN `USER` U ON C.userId = U.id
        LEFT JOIN `USERLEVELNEW` ULN ON ULN.id = U.level                                           
        LEFT JOIN `COMPANYRELATION` CR ON C.id = CR.sid 
        WHERE C.state=1 AND C.type = 41
        AND FROM_UNIXTIME(U.endDay) >= '%s'
        AND ULN.levelType <> 0 
        ORDER BY FROM_UNIXTIME(U.endDay),CR.pid,C.id
    '''

    today = datetime.date.today()
    seven_days_ago = today - datetime.timedelta(days=7)

    valid_user_df = get_sql_data(sql=get_valid_sql % (str(today) + ' 23:59:59'))
    valid_user_tuple = tuple(set(valid_user_df['userId']))

    # 获取目标用户活跃数据
    get_active_sql1 = f'''
    SELECT userId,COUNT(id)AS '近7日登陆数'
    from ACTIVERECORDS 
    WHERE userId in {valid_user_tuple}
    and FROM_UNIXTIME(lastActiveTime) BETWEEN '%s' AND '%s'
    GROUP BY userId
    '''

    get_active_sql2 = f'''
    SELECT userId,MAX(lastActiveTime)AS'最近登录时间' from ACTIVERECORDS 
    WHERE userId in {valid_user_tuple}
    GROUP BY userId
    '''

    active_df1 = get_sql_data(sql=get_active_sql1 % ((str(seven_days_ago) + ' 00:00:00'), (str(today) + ' 23:59:59')))
    active_df2 = get_sql_data(sql=get_active_sql2)

    # 获取目标用户草稿数据
    get_draft_sql1 = f'''
    SELECT userId, COUNT(id) AS '近7日创建草稿数', SUM(times)'近7日创建草稿时长'
    FROM DRAFT 
    WHERE userId in {valid_user_tuple}
    and FROM_UNIXTIME(date) BETWEEN '%s' AND '%s'
    GROUP BY userId
    '''
    get_draft_sql2 = f'''
    SELECT userId,MAX(date) as '最近创作草稿时间'
    FROM DRAFT 
    WHERE userId in {valid_user_tuple}
    GROUP BY userId
    '''
    draft_df1 = get_sql_data(sql=get_draft_sql1 % (str(seven_days_ago) + ' 00:00:00', str(today) + ' 23:59:59'))
    draft_df2 = get_sql_data(sql=get_draft_sql2)

    # 获取目标用户导出数据
    get_video_sql1 = f'''
    SELECT userId,COUNT(id) AS '近7日导出视频数',sum(duration) AS '近7日导出总时长'
    FROM VIDEO
    WHERE userId in {valid_user_tuple}
    AND from_unixtime(createTime) BETWEEN '%s' AND '%s'
    GROUP BY userId
    '''
    get_video_sql2 = f'''
    SELECT userId,max(createTime) as '最近导出视频时间'
    FROM VIDEO
    WHERE userId in {valid_user_tuple}
    GROUP BY userId
    '''

    video_df1 = get_sql_data(sql=get_video_sql1 % (str(seven_days_ago) + ' 00:00:00', str(today) + ' 23:59:59'))
    video_df2 = get_sql_data(sql=get_video_sql2)

    df_all1 = pd.merge(active_df1, draft_df1, how='left', on='userId')
    df_all1 = pd.merge(df_all1, video_df1, how='left', on='userId')

    df_all2 = pd.merge(active_df2, draft_df2, how='left', on='userId')
    df_all2 = pd.merge(df_all2, video_df2, how='left', on='userId')

    dfall = pd.merge(df_all1, df_all2, how='left', on='userId')
    dfall = pd.merge(valid_user_df, dfall, how='left', on='userId')

    # 创建一个只包含除了'合并后企业ID'列之外的列名的列表
    columns_to_fill1 = dfall.columns.difference(['合并后企业ID'])

    # 将除了'合并后企业ID'列之外的空值替换为 0
    dfall[columns_to_fill1] = dfall[columns_to_fill1].fillna(0)

    dfall['合并后企业ID'] = dfall['合并后企业ID'].fillna(dfall['原企业ID'])

    dfall['合并后企业ID'] = dfall['合并后企业ID'].astype('str')
    dfall['近7日导出总时长'] = dfall['近7日导出总时长'].astype('int')

    columns_to_fill2 = dfall.columns.difference(['企业名称','当前会员等级'])

    #dfall[columns_to_fill2] = dfall[columns_to_fill2].astype(int)
    # dfall.to_csv('dfall.csv')
    # 将所有数字列转换为整数类型，保持缺失值不变
    for column in dfall.select_dtypes(include=['number']):
        dfall[column] = pd.to_numeric(dfall[column], errors='coerce').astype('Int64')

    dfall['当前会员到期时间'] = dfall['当前会员到期时间']*1000
    dfall['最近登录时间'] = dfall['最近登录时间']*1000
    dfall['最近创作草稿时间'] = dfall['最近创作草稿时间']*1000
    dfall['最近导出视频时间'] = dfall['最近导出视频时间']*1000

    return dfall

#获取访问凭证
def get_token():
    # 获取访问凭证
    data1 = {
        "app_id": 'cli_a552f5a7087dd00e',
        "app_secret": 'QGl5rF4wvHGrt6TjLpmIXcNPsSQ5OmLz'
    }
    response = requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/', data=data1)
    # print(response.json())
    tenant_access_token = response.json()['tenant_access_token']
    return tenant_access_token

#用于转换数据格式
def convert_df_format(df):
    records = []
    for index, row in df.iterrows():
        record = {"fields": {}}
        for column in df.columns:
            record["fields"][column] = row[column]
        records.append(record)
    return {"records": records}


#用于分割数据框
def split_dataframe(df, max_length=500):
    '''
    :param df: 输入的数据框
    :param max_length:最大行数
    :return:返回一个列表，可以通过[n]提取分割后的数据框
    '''
    num_rows = len(df)
    num_splits = (num_rows - 1) // max_length + 1  # 计算需要切分的片段数

    # 切分DataFrame
    split_dfs = []
    for i in range(num_splits):
        start_idx = i * max_length
        end_idx = min((i + 1) * max_length, num_rows)
        split_dfs.append(df.iloc[start_idx:end_idx])

    return split_dfs


#用于创建多维表格-数据表
def create_larkTable1(app_token,tenant_access_token):
    '''
    :param doc_url: 文档的url链接
    :param tenant_access_token:token鉴权返回值
    :return: 返回创建结果
    '''
    #app_token = doc_url[doc_url.find('base/') + len('base/'):doc_url.find('?')]

    # 创建数据表
    # 请求头
    createTable_header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + tenant_access_token,
    }

    # 请求体
    createTable_data = {
        "table": {
            "default_view_name": "源数据数据(每日24点更新)",
            "fields": [
                {
                    "field_name": "合并后企业ID",
                    "type": 1
                }, {
                    "field_name": "原企业ID",
                    "type": 2
                }, {
                    "field_name": "企业名称",
                    "type": 1
                }, {
                    "field_name": "userId",
                    "type": 2
                }, {
                    "field_name": "当前会员等级",
                    "type": 1
                },
                {
                    "field_name": "当前会员到期时间",
                    "type": 5
                },
                {
                    "field_name": "近7日登陆数",
                    "type": 2
                }, {
                    "field_name": "近7日创建草稿数",
                    "type": 2
                }, {
                    "field_name": "近7日创建草稿时长",
                    "type": 2
                }, {
                    "field_name": "近7日导出视频数",
                    "type": 2
                }, {
                    "field_name": "近7日导出总时长",
                    "type": 2
                }, {
                    "field_name": "最近登录时间",
                    "type": 5
                }, {
                    "field_name": "最近创作草稿时间",
                    "type": 5
                }, {
                    "field_name": "最近导出视频时间",
                    "type": 5
                }

            ],
            "name": "用户维度源数据"
        }
    }
    # 将数据编码为 UTF-8
    createTable_encoded_data = json.dumps(createTable_data, ensure_ascii=False).encode('utf-8')

    createTable_url_post = 'https://open.feishu.cn/open-apis/bitable/v1/apps/%s/tables' % app_token
    # print(createTable_url_post)
    response1 = requests.post(createTable_url_post,
                              headers=createTable_header, data=createTable_encoded_data)
    # print('创建表请求',response1.json())
    return response1.json()['data']['table_id']
def create_larkTable2(app_token,tenant_access_token):
    '''
    :param doc_url: 文档的url链接
    :param tenant_access_token:token鉴权返回值
    :return: 返回创建结果
    '''
    #app_token = doc_url[doc_url.find('base/') + len('base/'):doc_url.find('?')]

    # 创建数据表
    # 请求头
    createTable_header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + tenant_access_token,
    }

    # 请求体
    createTable_data = {
        "table": {
            "default_view_name": "源数据数据(每日24点更新)",
            "fields": [
                {
                    "field_name": "企业ID",
                    "type": 1
                }, {
                    "field_name": "企业名称",
                    "type": 1
                }, {
                    "field_name": "当前会员等级",
                    "type": 1
                },
                {
                    "field_name": "当前会员到期时间",
                    "type": 5
                },
                {
                    "field_name": "近7日登陆数",
                    "type": 2
                }, {
                    "field_name": "近7日创建草稿数",
                    "type": 2
                }, {
                    "field_name": "近7日创建草稿时长",
                    "type": 2
                }, {
                    "field_name": "近7日导出视频数",
                    "type": 2
                }, {
                    "field_name": "近7日导出总时长",
                    "type": 2
                }, {
                    "field_name": "最近登录时间",
                    "type": 5
                }, {
                    "field_name": "最近创作草稿时间",
                    "type": 5
                }, {
                    "field_name": "最近导出视频时间",
                    "type": 5
                }

            ],
            "name": "企业维度源数据"
        }
    }
    # 将数据编码为 UTF-8
    createTable_encoded_data = json.dumps(createTable_data, ensure_ascii=False).encode('utf-8')

    createTable_url_post = 'https://open.feishu.cn/open-apis/bitable/v1/apps/%s/tables' % app_token
    # print(createTable_url_post)
    response1 = requests.post(createTable_url_post,
                              headers=createTable_header, data=createTable_encoded_data)
    return response1.json()['data']['table_id']

#用于写入多维表格-数据表新增多条数据
def write_larkTable_record(app_token,laipicDf,tenant_access_token,table_id):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + tenant_access_token,
    }
    data = convert_df_format(df=laipicDf)
    createTable_encoded_data = json.dumps(data, ensure_ascii=False).encode('utf-8')

    createTable_url_post = 'https://open.feishu.cn/open-apis/bitable/v1/apps/%s/tables/%s/records/batch_create' % (app_token,table_id)
    response = requests.post(createTable_url_post,
                              headers=header, data=createTable_encoded_data)
    # print(response.json())

    record_ids = [record['record_id'] for record in response.json()['data']['records']]

    return record_ids


def delete_larkTable_record(app_token,data,tenant_access_token,table_id):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + tenant_access_token,
    }

    createTable_url_post = 'https://open.feishu.cn/open-apis/bitable/v1/apps/%s/tables/%s/records/batch_delete' % (
    app_token, table_id)
    response = requests.post(createTable_url_post,
                             headers=header,data=data)

    # print('删除上次更新的内容')
    # print(response.json())


import numpy as np


def topsis(decision_matrix, weights):
    # 将决策矩阵转换为浮点数类型
    decision_matrix = decision_matrix.astype(float)

    # Step 1: Normalize the decision matrix
    norm_matrix = decision_matrix / np.sqrt(np.sum(decision_matrix ** 2, axis=0))

    # Step 2: Weight the normalized decision matrix
    weighted_matrix = norm_matrix * weights

    # Step 3: Determine the ideal and anti-ideal solutions
    ideal_solution = np.max(weighted_matrix, axis=0)
    negative_ideal_solution = np.min(weighted_matrix, axis=0)

    # Step 4: Calculate the distance to ideal and negative-ideal solutions
    d_pos = np.sqrt(np.sum((weighted_matrix - ideal_solution) ** 2, axis=1))
    d_neg = np.sqrt(np.sum((weighted_matrix - negative_ideal_solution) ** 2, axis=1))

    # Step 5: Calculate TOPSIS score
    topsis_score = d_neg / (d_pos + d_neg)

    return topsis_score*100


if __name__ == "__main__":
    #每日更新脚本的操作流程
    #Start-获取token-sql取数-删除老数据-写入新数据-新数据record_id入库-End

    #获取截止今日23:59:59的有效期内企业会员数据表
    todayLaipicDF = get_laipicDB_df()
    # print(todayLaipicDF)
    decision_matrix = todayLaipicDF[['近7日登陆数','近7日创建草稿数','近7日导出视频数','最近登录时间']].values
    weights = np.array([0.5, 0.25, 0.15, 0.1])
    # 计算 TOPSIS 分数
    todayLaipicDF['活跃得分']=topsis(decision_matrix, weights)
    #排序便于阅读
    todayLaipicDF=todayLaipicDF.sort_values(by=["当前会员到期时间", "活跃得分"], ascending=[True, False])
    print(todayLaipicDF)


    # #获取token
    token = get_token()
    #
    url1 = 'https://iwfrvtzly5n.feishu.cn/base/PXXzbw3avaYim2sPPoccAGZMn4O?table=tbleN2m4jdgFVSS0&view=vewi7xrxP6'
    app_token = 'PXXzbw3avaYim2sPPoccAGZMn4O'
    table_id1 = 'tblCEX2SiIeXsNCA'
    table_id2 = 'tbl91hA1AdB2Wjm3'

    # 运营数据库链接配置
    db_config = {
        'host': '116.205.228.171',
        'port': '3306',
        'database': 'larkAutoDoc',
        'username': 'yunying_admin',
        'password': 'LwaK%^6xMRwD'
    }
    # 数据库链接地址
    db_url1 = 'mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8'.format(
        **db_config)
    engine_yuyin = create_engine(db_url1)
    #获取上次写入记录id
    delete1 = pd.read_sql('select * from filed1', con=engine_yuyin)
    delete2 = pd.read_sql('select * from filed2', con=engine_yuyin)

    #分段（api上限500）
    delete1_list1 = split_dataframe(df=delete1['field_id'], max_length=500)
    #删除账号维度表内数据
    for i in range(len(delete1_list1)):
        series = pd.Series(delete1_list1[i], name='field_id')
        records = series.tolist()
        data = {"records": records}
        json_string = json.dumps(data, indent=4)  # 重塑数据格式
        delete_larkTable_record(app_token=app_token,data=json_string,tenant_access_token=token,table_id=table_id1) #执行删除操作

    # 写入新数据
    # 写入数据表1：账号维度
    # 单次调用最多500条数据
    df_list1 = split_dataframe(df=todayLaipicDF, max_length=500)
    # 循环处理分批写入
    records_id_list1 = []
    for i in range(len(df_list1)):
        # print('第', i, '次写入')
        returnData1 = write_larkTable_record(app_token=app_token,
                                                 laipicDf=df_list1[i],
                                                 tenant_access_token=token,
                                                 table_id=table_id1)
        records_id_list1.append(returnData1)

    # 记录写入记录
    df1 = pd.DataFrame(records_id_list1[0] + records_id_list1[1], columns=['field_id'])
    engine_yuyin = create_engine(db_url1)

    df1.to_sql('filed1', engine_yuyin, if_exists='replace', index=False)

    #分段（api上限500）
    delete2_list1 = split_dataframe(df=delete2['field_id'], max_length=500)
    #删除账号维度表内数据
    for i in range(len(delete2_list1)):
        series = pd.Series(delete2_list1[i], name='field_id')
        records = series.tolist()
        data = {"records": records}
        json_string = json.dumps(data, indent=4)  # 重塑数据格式
        delete_larkTable_record(app_token=app_token,data=json_string,tenant_access_token=token,table_id=table_id2)
        time.sleep(1)

    #写入数据表2：企业维度
    conn = sqlite3.connect(':memory:')
    todayLaipicDF.to_sql('todayLaipicDF', conn, index=False)


    query='''
    select 合并后企业ID as '企业ID',企业名称,当前会员等级,MAX(当前会员到期时间)as '当前会员到期时间' ,
    SUM(近7日登陆数) as '近7日登陆数',SUM(近7日创建草稿数) as '近7日创建草稿数',SUM(近7日创建草稿时长) as '近7日创建草稿时长',
    SUM(近7日导出视频数) as '近7日导出视频数' ,SUM(近7日导出总时长) as '近7日导出总时长',MAX(最近登录时间)  as '最近登录时间',
    MAX(最近创作草稿时间) as '最近创作草稿时间',MAX(最近导出视频时间) as '最近导出视频时间'
    from todayLaipicDF GROUP BY 合并后企业ID
    '''
    company_df = pd.read_sql_query(query, conn)
    # print(company_df)
    # 关闭数据库连接
    conn.close()
    df_list2=split_dataframe(df=company_df,max_length=500)
    # print('注意！bug',len(df_list2))
    records_id_list2=[]
    for i in range(len(df_list2)):
        returnData2 = write_larkTable_record(app_token=app_token,
                               laipicDf=df_list2[i],
                               tenant_access_token=token,
                               table_id=table_id2)
        records_id_list2.append(returnData2)
        time.sleep(1)

    # 记录写入记录
    df2=pd.DataFrame(records_id_list2[0]+records_id_list2[1],columns=['field_id'])

    df2.to_sql('filed2', engine_yuyin, if_exists='replace', index=False)




