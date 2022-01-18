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

# select review for empathy/classify analysis
df, isrt_dttm=db.TB_REIVEW_qa(from_date,to_date)

# REVIEW ANALYSIS
# api
#anal00=classify_empathy.api(df)
# pt
anal00=classify_empathy.model_pt(df)
#anal00.to_csv(f'{today_path}\\anal00_result.csv',index=None)

# anal00 insert
db.TB_anal_00_insert(anal00)
print('--------------------------------------DB insert--------------------------')
with open("./etc/last_isrt_dttm.txt","a",encoding='utf8') as f:
    f.write(f'\n{to_date}\t{isrt_dttm}\t분석완료')


## 키워드 키센텐스 실행을 위한 code_list
anal00_anal_code_list=db.ANAL00_AanlCode()

anal3 = keysentence.total(anal00_anal_code_list)
#anal3.to_csv(f'{today_path}\\anal03_result.csv',index=None)
anal2 = keysentence.emo(anal00_anal_code_list)
#anal2.to_csv(f'{today_path}\\anal02_result.csv',index=None)

# anal03 / anal02 insert
db.TB_anal_03_insert(anal3)
db.TB_anal_02_insert(anal2)

# anal01 procesure
db.TB_anal_01_insert()

finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"{finish_time} 분석 완료")