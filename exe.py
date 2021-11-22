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
    print(e)
finally:
    select_conn.close()

print(df)
select_conn.close()


data=classify_empathy.how(df)

anal00=data[['review_doc_no','anal_code','classify','empathy','empathy_score']]

# anal00 insert
try:
    insert_conn=db.insert_conn()
    db.TB_anal_00_insert(insert_conn,anal00)
except Exception as e:
    print(e)
finally:
    insert_conn.close()

# anal01에 insert해야함 -- 프로시저 호출


##키워드 키센텐스 돌아가야함
new = data[['anal_code']]
new = new.drop_duplicates()

code_list = new['anal_code'].tolist()
print(code_list)

anal3 = keysentence.total(code_list)
anal2 = keysentence.emo(code_list)

try:
    insert_conn=db.insert_conn()
    db.TB_anal_03_insert(insert_conn,anal3)
    db.TB_anal_02_insert(insert_conn,anal2)
except Exception as e:
    print(e)
finally:
    insert_conn.close()

print(anal3)
print(anal2)
