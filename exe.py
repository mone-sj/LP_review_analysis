#-*- coding:utf-8 -*-
from DB_conn import db
import classify_empathy
import numpy as np
import keysentence
import pandas as pd
from datetime import datetime

to_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S') # anal_date

from_date=db.last_isrt_dttm()
print(f"to_date: {to_date}\nfrom_date:{from_date}")

df, isrt_dttm=db.TB_REIVEW_qa(from_date,to_date)
with open("./etc/last_isrt_dttm.txt","a",encoding='utf8') as f:
    f.write(f'\n{to_date}\t{isrt_dttm}')

# 테스트
#from_date='20211109'
#to_date='20211109'
# 날짜별
#df=db.TB_REIVEW_qa(from_date,to_date)
#df = pd.read_csv('CP_ANAL-211104-0163_analy_cd.csv')
print(df)

#새로운 리뷰 감성 및 분류분석
# api
#anal00=classify_empathy.api(df)
# pt
anal00=classify_empathy.model_pt(df)

# anal00 insert
db.TB_anal_00_insert(anal00)
print('--------------------------------------DB insert--------------------------')


## 키워드 키센텐스 실행을 위한 code_list
anal00_anal_code_list=db.ANAL00_AanlCode()
print(anal00_anal_code_list)
code_list=[]
for v in anal00_anal_code_list:
    if v not in code_list:
        code_list.append(v)
print(code_list)

anal3 = keysentence.total(code_list)
anal2 = keysentence.emo(code_list)
print(anal3)
print(anal2)

# anal03 / anal02 insert
db.TB_anal_03_insert(anal3)
db.TB_anal_02_insert(anal2)

# anal01 procesure
db.TB_anal_01_insert()
