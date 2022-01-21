#-*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import pymssql, os, smtplib, traceback, time
from datetime import datetime
from email.mime.text import MIMEText
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase

host = 'ASC-AI.iptime.org'
database = 'living_paradise'
username = 'living_paradise'
password = 'asc1234pw!'

# DB 연결
def insert_conn():
    ''' charset = 'UTF8' '''
    try:
        conn = pymssql.connect(host, username, password, database, charset="utf8")
    except Exception as e:
        print("Error: ", e)
    return conn

def select_conn():
    ''' charset = 'CP949' '''
    try:
        conn = pymssql.connect(host, username, password, database, charset="cp949")
    except Exception as e:
        print("Error: ", e)
    return conn

'''DB select'''
def TB_REIVEW_qb():
    '''TB_REVIEW all select func.(cp949)'''
    try:
        conn=pymssql.connect(host, username, password, database, charset="cp949")
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
        conn = pymssql.connect(host, username, password, database, charset="cp949")
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
        conn = pymssql.connect(host, username, password, database, charset="cp949")
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
    conn = pymssql.connect(host, username, password, database, charset="cp949")
    try:
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
        conn = pymssql.connect(host, username, password, database, charset="cp949")
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
        conn = pymssql.connect(host, username, password, database, charset="cp949")
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
        conn = pymssql.connect(host, username, password, database, charset="utf8")
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
    '''procesure execute'''
    try:
        conn = pymssql.connect(host, username, password, database, charset="cp949")
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA001 @section = 'SB'"
        cursor.execute(sql)
        conn.commit()
        print("anal_01 완료")
    except Exception as e:
        print(e)
    finally:
        conn.close()

def TB_anal_02_insert(df):
    try:
        conn = pymssql.connect(host, username, password, database, charset="utf8")
        for i, row in df.iterrows():
            cursor = conn.cursor()
        
            # for col in range(len(df.columns)):
            # # float 값 체크,..
            #     if type(row[col]) is np.float:
            #         row[col] = ''

            # if type(row[4]) is np.float:
            #     print('null')
            #     continue
            if row[4]=='오류' or row[4]=='': 
                continue
            else:
                if row[1] == '0':  # 키워드 / csv 파일은 0을 숫자로 인식, 분석결과의 DF는 0을 문자로 인식
                    sql = "exec dbo.P_MNG_ANA002 @section = 'SA', @anal_code=%s,@keyword_gubun=%s,@keyword_positive=%s,@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s,@rlt_value_06=%s,@rlt_value_07=%s,@rlt_value_08=%s,@rlt_value_09=%s,@rlt_value_10=%s"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    conn.commit()

                elif row[1] == '1':  # 핵심문장
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
        conn = pymssql.connect(host, username, password, database, charset="utf8")
        for i, row in df.iterrows():
            cursor = conn.cursor()
            
            # if type(row[3]) is np.float:
            #     print(f'{i}번째 행 type(row[3]) np.float라서 continue null')
            #     continue
            if row[3]=='오류' or row[3]=='':
                continue
            else:
                if row[1] == '0':  # 키워드
                    sql = "exec dbo.P_MNG_ANA003 @section = 'SA', @anal_code=%s,@keyword_gubun=%s,@site_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s,@rlt_value_04=%s,@rlt_value_05=%s,@rlt_value_06=%s,@rlt_value_07=%s,@rlt_value_08=%s,@rlt_value_09=%s,@rlt_value_10=%s"
                    cursor.execute(sql, tuple(row))
                    print("Record inserted")
                    conn.commit()

                elif row[1] == '1':  # 핵심문장
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

### send mail infomation
sendEmail="asclhg@naver.com"
recvEmail="seojeong@asc.kr"
mail_pw="llhhgg0119@"

smtpName="smtp.naver.com"
smtpPort=587
session=None
today=datetime.now().strftime('%Y%m%d')

def success_sendEmail():
    try:
        path=today_path()
        # SMTP 세션 생성
        session=smtplib.SMTP(smtpName,smtpPort)
        session.set_debuglevel(False)
        
        # SMTP 계정 인증 설정
        session.ehlo()
        session.starttls()
        session.login(sendEmail,mail_pw)

        # 메일컨텐츠 설정
        message=MIMEMultipart("mixed")
        
        # 메일 송/수신 옵션 설정
        message.set_charset('utf-8')
        
        content_path=os.path.join(path,'분석시간체크.txt')
        if not os.path.exists(content_path):
            text=f'{today} 분석완료'
        else:
            with open(content_path,'r',encoding='utf8') as f:
                text=f.read()
        
        message['From']=sendEmail
        message['To']=recvEmail
        message['Subject']=f"[리뷰분석완료]{today} Lipaco 분석완료"
        message.attach(MIMEText(text))
        
        # 메일 첨부파일
        attachments=os.listdir(path) # file_list: 폴더 내 파일 리스트
        
        work_dir=os.getcwd()
        isrt_dttm_path=os.path.join(work_dir,'etc','last_isrt_dttm.txt')
        #isrt_attach=open(isrt_dttm_path,'rb')
        part=MIMEBase('application','octet-stream')
        part.set_payload(open(isrt_dttm_path,'rb').read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", 'attachment',filename=os.path.basename(isrt_dttm_path))
        message.attach(part)

        if len(attachments) > 0:
            for attachment in attachments:
                file_path=os.path.join(path,attachment)
                attach_binary=MIMEBase("application","octet-stream")
                try:
                    attach_file=open(file_path,"rb").read() #open the file
                    attach_binary.set_payload(attach_file)
                    encoders.encode_base64(attach_binary)
                    
                    # 파일이름 지정된 report header추가
                    attach_binary.add_header("Content-Disposition", 'attachment',filename=attachment)
                    message.attach(attach_binary)
                except Exception as e:
                    print(e)

        # 메일 발송
        session.sendmail(sendEmail,recvEmail,message.as_string())
        print("successfully sent the mail")

    except Exception as e:
        print(e)
    finally:
        if session is not None:
            session.quit()


def fail_sendEmail(err):
    msg=MIMEText(err)
    msg['Subject']=f"[리뷰분석오류]{today} 리파코 분석 오류"
    msg['From']=sendEmail
    msg['To']=recvEmail
    print(msg.as_string())

    s=smtplib.SMTP(smtpName,smtpPort)
    s.starttls()
    s.login(sendEmail,mail_pw)
    s.sendmail(sendEmail,recvEmail,msg.as_string())
    s.close()
    print("successfully sent the mail")


def today_path():
    '''backup folder create'''
    folder_path=os.getcwd()+'/etc/result_data'
    #folder_path=os.getcwd()+'\\etc\\result_data'
    today_path=os.path.join(folder_path,today)
    if not os.path.exists(today_path):
        os.mkdir(today_path)
    return today_path