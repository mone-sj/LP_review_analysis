#-*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import pymssql

server = 'ASC-AI.iptime.org'
database = 'living_paradise'
username = 'living_paradise '
password = 'asc1234pw!'

# DB 연결
def insert_conn():
    ''' charset = 'UTF8' '''
    try:
        conn = pymssql.connect(server, username, password, database, charset="utf8")
    except Exception as e:
        print("Error: ", e)
    return conn

def select_conn():
    ''' charset = 'CP949' '''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
    except Exception as e:
        print("Error: ", e)
    return conn

'''DB select'''

def TB_REIVEW_qb():
    '''TB_REVIEW all select func.(cp949)'''
    try:
        conn=pymssql.connect(server, username, password, database, charset="cp949")
        sql="exec dbo.P_MNG_CRW004 @section = 'QB'"
        col_name=["site_gubun","url_addr","review_doc_no","isrt_date","anal_code","company_gubun","review_user","review_dttm","review_grade","review_option_name","review","remark","isrt_user","updt_user","isrt_dttm","updt_dttm"]
        ori_df=pd.read_sql(sql,conn)
        ori_df.columns=col_name
        df=ori_df[['site_gubun','review_doc_no','anal_code','review']]
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return df

def TB_REIVEW_qa(from_date, to_date):
    ''' TB_REVIEW 날짜별 select '''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor = conn.cursor()
        sql="exec dbo.P_MNG_CRW004 @section = 'QA', @from_date=%s, @to_date=%s"
        cursor.execute(sql,(from_date, to_date))
        row=cursor.fetchall()
        col_name=["site_gubun","url_addr","review_doc_no","isrt_date","anal_code","company_gubun","review_user","review_dttm","review_grade","review_option_name","review","remark","isrt_user","updt_user","isrt_dttm","updt_dttm"]
        ori_df=pd.DataFrame(row, columns=col_name)
        isrt_dttm=ori_df.iloc[0,14]
        isrt_dttm_format=isrt_dttm.strftime('%Y-%m-%d %H:%M:%S')
        df=ori_df[['site_gubun','review_doc_no','anal_code','review']]
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return df, isrt_dttm_format

def TB_UNUSE_KEYWORD():
    ''' 불용어(stopwords) 불러오기 '''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor = conn.cursor()
        sql = "select KEY_WORD from TB_UNUSE_KEYWORD"
        cursor.execute(sql)
        row=[item[0] for item in cursor.fetchall()]
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return row

def ANAL00_AanlCode():
    '''TB_ANAL00에 등록된 anal_code list'''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor = conn.cursor()
        sql = "select ANAL_CODE from TB_REVIEW_ANAL_00"
        cursor.execute(sql)
        row=[item[0] for item in cursor.fetchall()]
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return row

# TB_REVIEW anal_code별 select
# 필요한 정보는 site_gubun, review, empathy_score
def TB_REIVEW_join(anal_code):
    '''TB_REVIEW & TB_REVIEW_ANAL_00 JOIN'''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor = conn.cursor()
        sql="select A.SITE_GUBUN, A.REVIEW_DOC_NO, A.ANAL_CODE, B.RLT_VALUE_03,A.REVIEW from TB_REVIEW A inner join TB_REVIEW_ANAL_00 B on A.ANAL_CODE=%s and A.REVIEW_DOC_NO=B.REVIEW_DOC_NO"
        cursor.execute(sql, anal_code)
        row=cursor.fetchall()
        col_name=["SITE_GUBUN","REVIEW_DOC_NO","ANAL_CODE","RLT_VALUE_03","REVIEW"]
        df=pd.DataFrame(row, columns=col_name)
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return df

def TB_REVIEW_A(anal_code):
    '''select TB_REVIEW per anal_code for TB_REVIEW & TB_REVIEW_ANAL00 join'''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor = conn.cursor()
        sql="select SITE_GUBUN, REVIEW_DOC_NO, ANAL_CODE, REVIEW from TB_REVIEW where ANAL_CODE=%s "
        cursor.execute(sql, anal_code)
        row=cursor.fetchall()
        col_name=["SITE_GUBUN","REVIEW_DOC_NO","ANAL_CODE","REVIEW"]
        df_A=pd.DataFrame(row, columns=col_name)
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return df_A

def TB_REVIEW_B(anal_code):
    '''select TB_REVIEW_ANAL00 per anal_code for TB_REVIEW & TB_REVIEW_ANAL00 join'''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor = conn.cursor()
        sql="select REVIEW_DOC_NO,RLT_VALUE_03 from TB_REVIEW_ANAL_00 where ANAL_CODE=%s "
        cursor.execute(sql, anal_code)
        row=cursor.fetchall()
        col_name=["REVIEW_DOC_NO","RLT_VALUE_03"]
        df_B=pd.DataFrame(row, columns=col_name)
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return df_B

''' DB insert'''
# TB_anal_00 column
def TB_anal_00_insert(conn, df):    
    for i, row in df.iterrows():
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA000 @section='SA', @review_doc_no=%s, @anal_code=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s"
        cursor.execute(sql, tuple(row))
        print("record inserted")
        conn.commit()
    print('anal00완료')


def TB_anal_01_insert():
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA001 @section = 'SB'"
        cursor.execute(sql)
        print("anal_01 완료")
    except Exception as e:
        print(e)
    finally:
        conn.close()

def TB_anal_02_insert(df):
    try:
        conn = pymssql.connect(server, username, password, database, charset="utf8")
        for i, row in df.iterrows():
            cursor = conn.cursor()
        
            for col in range(len(df.columns)):
            # float 값 체크,..
                if type(row[col]) is np.float:
                    row[col] = ''

            if type(row[4]) is np.float:
                print('null')
                continue
            else:
                if row[1] == 0:  # 키워드
                    sql = "exec dbo.P_MNG_ANA002 @section = 'SA', @anal_code=%s,@keyword_gubun=%s,@keyword_positive=%s,@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s,@rlt_value_06=%s,@rlt_value_07=%s,@rlt_value_08=%s,@rlt_value_09=%s,@rlt_value_10=%s"
                    #sql = "INSERT INTO TB_REVIEW_ANAL_03 (SITE_GUBUN,PART_GROUP_ID, PART_SUB_ID,PART_ID,KEYWORD_GUBUN,RLT_VALUE_01,RLT_VALUE_02,RLT_VALUE_03, RLT_VALUE_04,RLT_VALUE_05,RLT_VALUE_06,RLT_VALUE_07,RLT_VALUE_08,RLT_VALUE_09,RLT_VALUE_10) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    # the connection is not autocommitted by default, so we must commit to save our changes
                    conn.commit()

                elif row[1] == 1:  # 핵심문장
                    sql = "exec dbo.P_MNG_ANA002 @section = 'SA', @anal_code=%s,@keyword_gubun=%s,@keyword_positive=%s,@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s"
                    #sql = "INSERT INTO TB_REVIEW_ANAL_03 (SITE_GUBUN, PART_GROUP_ID,PART_SUB_ID,PART_ID,KEYWORD_GUBUN,RLT_VALUE_01,RLT_VALUE_02,RLT_VALUE_03, RLT_VALUE_04,RLT_VALUE_05) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()
    print('DB 저장 끝')

def TB_anal_03_insert(df):
    try:
        conn = pymssql.connect(server, username, password, database, charset="utf8")
        for i, row in df.iterrows():
            cursor = conn.cursor()
            for col in range(len(df.columns)):
            # float 값 체크,..
                if type(row[col]) is np.float:
                    row[col] = ''

            if type(row[3]) is np.float:
                print('null')
                continue
            else:
                if row[1] == 0:  # 키워드
                    sql = "exec dbo.P_MNG_ANA003 @section = 'SA', @anal_code=%s,@keyword_gubun=%s@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s,@rlt_value_06=%s,@rlt_value_07=%s,@rlt_value_08=%s,@rlt_value_09=%s,@rlt_value_10=%s"
                    #sql = "INSERT INTO TB_REVIEW_ANAL_03 (SITE_GUBUN,PART_GROUP_ID, PART_SUB_ID,PART_ID,KEYWORD_GUBUN,RLT_VALUE_01,RLT_VALUE_02,RLT_VALUE_03, RLT_VALUE_04,RLT_VALUE_05,RLT_VALUE_06,RLT_VALUE_07,RLT_VALUE_08,RLT_VALUE_09,RLT_VALUE_10) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    # the connection is not autocommitted by default, so we must commit to save our changes
                    conn.commit()

                elif row[1] == 1:  # 핵심문장
                    sql = "exec dbo.P_MNG_ANA003 @section = 'SA', @anal_code=%s,@keyword_gubun=%s,@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s"
                    #sql = "INSERT INTO TB_REVIEW_ANAL_03 (SITE_GUBUN, PART_GROUP_ID,PART_SUB_ID,PART_ID,KEYWORD_GUBUN,RLT_VALUE_01,RLT_VALUE_02,RLT_VALUE_03, RLT_VALUE_04,RLT_VALUE_05) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()
    print('DB 저장 끝')


def last_isrt_dttm():
    with open('./etc/last_isrt_dttm.txt','r',encoding='utf8') as f:
        lines=f.readlines() # 모든줄을 읽어서 한 라인씩 리스트로 값 반환
        last_line=len(lines)-1
        isrt_date=lines[last_line].split('\t')[1]
    return isrt_date
