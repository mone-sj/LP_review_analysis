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
        col_name=["SITE_GUBUN","URL_ADDR","REVIEW_DOC_NO","ISRT_DATE","ANAL_CODE","COMPANY_GUBUN","REVIEW_USER","REVIEW_DTTM","REVIEW_GRADE","REVIEW_OPTION_NAME","REVIEW","REMARK","ISRT_USER","UPDT_USER","ISRT_DTTM","UPDT_DTTM"]
        ori_df=pd.read_sql(sql,conn)
        ori_df.columns=col_name
        df=ori_df[['SITE_GUBUN','REVIEW_DOC_NO','ANAL_CODE','REVIEW']]
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
        col_name=["SITE_GUBUN","URL_ADDR","REVIEW_DOC_NO","ISRT_DATE","ANAL_CODE","COMPANY_GUBUN","REVIEW_USER","REVIEW_DTTM","REVIEW_GRADE","REVIEW_OPTION_NAME","REVIEW","REMARK","ISRT_USER","UPDT_USER","ISRT_DTTM","UPDT_DTTM"]
        ori_df=pd.DataFrame(row, columns=col_name)
        isrt_dttm=ori_df.iloc[0,14]
        isrt_dttm_format=isrt_dttm.strftime('%Y-%m-%d %H:%M:%S')
        df=ori_df[['SITE_GUBUN','REVIEW_DOC_NO','ANAL_CODE','REVIEW']]
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
        sql = "select distinct ANAL_CODE from TB_REVIEW_ANAL_00"
        cursor.execute(sql)
        row=[item[0] for item in cursor.fetchall()]
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return row


def TB_REVIEW_join():
    '''select TB_REVIEW per anal_code for TB_REVIEW & TB_REVIEW_ANAL00 join'''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        sql="select SITE_GUBUN, REVIEW_DOC_NO, ANAL_CODE, REVIEW from TB_REVIEW (nolock) where ANAL_CODE like '%ANAL%'"
        df_A=pd.read_sql(sql,conn)
        col_nameA=["SITE_GUBUN","REVIEW_DOC_NO","ANAL_CODE","REVIEW"]
        df_A.columns=col_nameA
    
        sql2="select REVIEW_DOC_NO, ANAL_CODE,RLT_VALUE_03 from TB_REVIEW_ANAL_00 (nolock) where ANAL_CODE like '%ANAL%'"
        df_B=pd.read_sql(sql2,conn)
        col_nameB=['REVIEW_DOC_NO', 'ANAL_CODE','RLT_VALUE_03']
        df_B.columns=col_nameB
    except Exception as e:
        print(e)
    finally:
        conn.close()

    df=pd.merge(df_A,df_B,on=['REVIEW_DOC_NO','ANAL_CODE'])
    df=df[["SITE_GUBUN","REVIEW_DOC_NO","ANAL_CODE","REVIEW","RLT_VALUE_03"]]
    return df

def site_gubun_list():
    '''REVIEW_TABLE에 등록된 site_gubun list'''
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor = conn.cursor()
        sql = "select distinct SITE_GUBUN from TB_REVIEW (nolock)"
        cursor.execute(sql)
        row=[item[0] for item in cursor.fetchall()]
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return row

''' DB insert'''
# TB_anal_00 column
def TB_anal_00_insert(df):
    try:
        conn = pymssql.connect(server, username, password, database, charset="utf8")
        for i, row in df.iterrows():
            cursor=conn.cursor()
            sql="exec dbo.P_MNG_ANA000 @section='SA', @review_doc_no=%s, @anal_code=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s"
            cursor.execute(sql, tuple(row))
            print("record inserted")
            conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()
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
        
            # for col in range(len(df.columns)):
            # # float 값 체크,..
            #     if type(row[col]) is np.float:
            #         row[col] = ''

            if type(row[4]) is np.float:
                print('null')
                continue
            elif row[4]=='오류':
                continue
            else:
                if row[1] == 0:  # 키워드
                    sql = "exec dbo.P_MNG_ANA002 @section = 'SA', @anal_code=%s,@keyword_gubun=%s,@keyword_positive=%s,@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s,@rlt_value_06=%s,@rlt_value_07=%s,@rlt_value_08=%s,@rlt_value_09=%s,@rlt_value_10=%s"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    conn.commit()

                elif row[1] == 1:  # 핵심문장
                    sql = "exec dbo.P_MNG_ANA002 @section = 'SA', @anal_code=%s,@keyword_gubun=%s,@keyword_positive=%s,@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()
    print('anal02 DB 저장 끝')

def TB_anal_03_insert(df):
    try:
        conn = pymssql.connect(server, username, password, database, charset="utf8")
        for i, row in df.iterrows():
            cursor = conn.cursor()
            
            if type(row[3]) is np.float:
                print('null')
                continue
            elif row[3]=='오류':
                continue
            else:
                if row[1] == 0:  # 키워드
                    sql = "exec dbo.P_MNG_ANA003 @section = 'SA', @anal_code=%s,@keyword_gubun=%s@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s,@rlt_value_06=%s,@rlt_value_07=%s,@rlt_value_08=%s,@rlt_value_09=%s,@rlt_value_10=%s"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    conn.commit()

                elif row[1] == 1:  # 핵심문장
                    sql = "exec dbo.P_MNG_ANA003 @section = 'SA', @anal_code=%s,@keyword_gubun=%s,@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()
    print('anal03 DB 저장 끝')


def last_isrt_dttm():
    with open('./etc/last_isrt_dttm.txt','r',encoding='utf8') as f:
        lines=f.readlines()
        last_line=len(lines)-1
        isrt_date=lines[last_line].split('\t')[1]
    return isrt_date

def save_txt(content_list,file_path):
    file_name=f'{file_path}.txt'
    with open(file_name,'a',encoding='utf8') as f:
        for line in content_list:
            f.write(f'{line}\n')

def time_txt(content_list,file_path):
    file_name=f'{file_path}.txt'
    if not os.path.exists(file_name):
        with open(file_name,'a',encoding='utf8') as f:
            f.write('분석날짜\t분석모델\t분석제품수\t총 리뷰수\t분석시간\n')
            
    with open(file_name,'a',encoding='utf8') as f:
        for line in content_list:
            f.write(f'{line}\t')
        f.write("\n")