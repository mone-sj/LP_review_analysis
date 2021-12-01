#-*- coding: utf-8 -*-
import pandas as pd
import pymssql
#from datetime import datetime 

# DB 연결
def insert_conn():
    try:
        server = '211.223.132.46'
        database = 'living_paradise'
        username = 'living_paradise '
        password = 'asc1234pw!'
        conn = pymssql.connect(server, username, password, database, charset="utf8")
    except Exception as e:
        print("Error: ", e)
    return conn

def select_conn():
    try:
        server = '211.223.132.46'
        database = 'living_paradise'
        username = 'living_paradise '
        password = 'asc1234pw!'
        conn = pymssql.connect(server, username, password, database, charset="cp949")
    except Exception as e:
        print("Error: ", e)
    
    return conn

'''DB select'''
# TB_REIVEW all select
# select_conn 사용
def TB_REIVEW_qb(conn):
    sql="exec dbo.P_MNG_CRW004 @section = 'QB'"
    col_name=["site_gubun","url_addr","review_doc_no","isrt_date","anal_code","company_gubun","review_user","review_dttm","review_grade","review_option_name","review","remark","isrt_user","updt_user","isrt_dttm","updt_dttm"]
    ori_df=pd.read_sql(sql,conn)
    ori_df.columns=col_name
    df=ori_df[['site_gubun','review_doc_no','anal_code','review']]
    print(df.head(2))    
    return df

def TB_UNUSE_KEYWORD(conn):
    cursor = conn.cursor()
    sql = "select KEY_WORD from TB_UNUSE_KEYWORD"
    cursor.execute(sql)
    row=cursor.fetchall()
    col_name=["word"]
    df=pd.DataFrame(row,columns=col_name)
    list_df = df.values.tolist()
    list_df = sum(list_df,[])
    return list_df

# TB_REVIEW 날짜별 select
# select_conn 사용
def TB_REIVEW_qa(conn, from_date, to_date):
    cursor = conn.cursor()
    sql="exec dbo.P_MNG_CRW004 @section = 'QA', @from_date=%s, @to_date=%s"
    cursor.execute(sql,(from_date, to_date))
    row=cursor.fetchall()
    col_name=["site_gubun","url_addr","review_doc_no","isrt_date","anal_code","company_gubun","review_user","review_dttm","review_grade","review_option_name","review","remark","isrt_user","updt_user","isrt_dttm","updt_dttm"]
    ori_df=pd.DataFrame(row, columns=col_name)
    df=ori_df[['site_gubun','review_doc_no','anal_code','review']]
    return df

# TB_REVIEW anal_code별 select
# select_conn 사용
# keyword&sentence 결과값 반환을 위한 select문으로 감정점수도 함께 select되어 나와야함
# 필요한 정보는 site_gubun, review, empathy_score
def TB_REIVEW_join(conn, anal_code):
    cursor = conn.cursor()
    sql="select A.SITE_GUBUN, A.REVIEW_DOC_NO, A.ANAL_CODE, B.RLT_VALUE_03, REVIEW from TB_REVIEW A inner join TB_REVIEW_ANAL_00 B on A.ANAL_CODE=%s and A.REVIEW_DOC_NO=B.REVIEW_DOC_NO"
    cursor.execute(sql, anal_code)
    row=cursor.fetchall()
    col_name=["SITE_GUBUN","REVIEW_DOC_NO","ANAL_CODE","RLT_VALUE_03","REVIEW"]
    df=pd.DataFrame(row, columns=col_name)

    # 감정점수도 select 필요
    #df=ori_df[['site_gubun','review_doc_no','anal_code','review']]
    return df


''' DB insert'''
# TB_anal_00 column
# REVIEW_DOC_NO, ANAL_CODE, RLT_VALUE_01, RLT_VALUE_02, RLT_VALUE_03
def TB_anal_00_insert(conn, df):    
    for i, row in df.iterrows():
        cursor=conn.cursor()
        #sql="insert TB_REVIEW_ANAL_00 (REVIEW_DOC_NO, ANAL_CODE, RLT_VALUE_01, RLT_VALUE_02, RLT_VALUE_03) values(%s, %s, %s, %s, %s)"
        cursor.execute(sql, tuple(row))
        print("inserted")
        conn.commit()
    print("anal00_insert완료")


# 프로시저 호출
def TB_anal_01_insert(conn):
    cursor=conn.cursor()
    sql="exec "
    cursor.execute(sql )
    print("anal_01 완료")

def TB_anal_02_insert(conn,df):
    for i, row in df.iterrows():
        cursor=conn.cursor()
        if row[1]==0: #키워드
            sql="exec dbo.P_MNG_ANA002 @section='SA', @anal_code=%s, @keyword_gubun=%s, @keyword_positive=%s, @site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s, @rlt_value_06=%s, @rlt_value_07=%s, @rlt_value_08=%s, @rlt_value_09=%s, @rlt_value_10=%s"
            cursor.execute(sql,tuple(row))
            print("recode inserted")
            conn.commit()

        elif row[1]==1: #핵심문장
            sql="exec dbo.P_MNG_ANA002 @section='SA', @anal_code=%s, @keyword_gubun=%s, @keyword_positive=%s, @site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s"
            cursor.execute(sql, tuple(row))
            print("record inserted")
            conn.commit()
    print("anal_02_완료")

def TB_anal_03_insert(conn,df):
    for i, row in df.iterrows():
        cursor=conn.cursor()
        if row[1]==0: #키워드
            sql="exec dbo.P_MNG_ANA003 @section='SA', @anal_code=%s, @keyword_gubun=%s, @site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s, @rlt_value_06=%s, @rlt_value_07=%s, @rlt_value_08=%s, @rlt_value_09=%s, @rlt_value_10=%s"
            cursor.execute(sql,tuple(row))
            print("recode inserted")
            conn.commit()

        elif row[1]==1: #핵심문장
            sql="exec dbo.P_MNG_ANA003 @section='SA', @anal_code=%s, @keyword_gubun=%s, @site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s"
            cursor.execute(sql, tuple(row))
            print("record inserted")
            conn.commit()
    print("완료")

