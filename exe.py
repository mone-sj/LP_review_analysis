#-*- coding:utf-8 -*-


from DB_conn import db
import classify_empathy
import numpy as np
import keysentence
import pandas as pd
# 테스트
#from_date='20211109'
#to_date='20211109'

#try:
#    select_conn=db.select_conn()
    # 날짜별
#    df=db.TB_REIVEW_qa(select_conn,from_date,to_date)
#   select_conn.close()
#except Exception as e:
#    print(e)
#finally:
#   select_conn.close()

df = pd.read_csv('CP_ANAL-211104-0163_analy_cd.csv')
print(df)


#새로운 리뷰 감성 및 분류분석
data=classify_empathy.how(df)

anal00=data[['REVIEW_DOC_NO','ANAL_CODE','classify','empathy','empathy_score']]

# anal00 insert
try:
    insert_conn=db.insert_conn()
    db.TB_anal_00_insert(insert_conn,anal00)
    print('--------------------------------------DB insert--------------------------')
except Exception as e:
    print(e)
finally:
    insert_conn.close()

# anal01에 insert해야함 -- 프로시저 호출


##키워드 키센텐스 돌아가야함
new = data[['ANAL_CODE']]
new = new.drop_duplicates()

code_list = new['ANAL_CODE'].tolist()
print(code_list)

anal3 = keysentence.total(code_list)
anal2 = keysentence.emo(code_list)
print(anal3)
print(anal2)


try:
    insert_conn=db.insert_conn()
    db.TB_anal_03_insert(insert_conn,anal3)
    
    db.TB_anal_02_insert(insert_conn,anal2)
except Exception as e:
    print(e)
finally:
    insert_conn.close()