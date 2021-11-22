#-*- coding:utf-8 -*-


from DB_conn import db
import classify_empathy
import numpy as np
import keysentence
# 테스트
from_date='20211109'
to_date='20211109'

try:
    select_conn=db.select_conn()
    # 날짜별
    df=db.TB_REIVEW_qa(select_conn,from_date,to_date)
except Exception as e:
    select_conn.close()

print(df)
select_conn.close()


data=classify_empathy.how(df)
#print(data)
anal00=data[['review_doc_no','anal_code','classify','empathy','empathy_score']]

print(anal00)
print(len(anal00))

try:
    insert_conn=db.insert_conn()
    db.TB_anal_00_insert(insert_conn,anal00)
except Exception as e:
    print(e)
finally:
    insert_conn.close()