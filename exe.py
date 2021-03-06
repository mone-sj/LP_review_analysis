#-*- coding:utf-8 -*-
from DB_conn import db
import classify_empathy
import keysentence
from datetime import datetime
import time, traceback

def analysis():
    to_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S') # anal_date
    from_date=db.last_isrt_dttm()
    print(f"to_date: {to_date}\nfrom_date:{from_date}")

    # select review for empathy/classify analysis
    df,isrt_dttm=db.TB_REIVEW_qa(from_date,to_date)
    
    #REVIEW ANALYSIS
    if len(df)==0: # insert된 리뷰가 없을때는 분류 프로세스를 실행하지 않음
       print('insert no review: classification process pass')
    else:
        # anal00=classify_empathy.api(df)                # api
        anal00=classify_empathy.model_pt(df)                # pt
        
        # anal00 insert
        db.TB_anal_00_insert(anal00)
        print('--------------------------------------DB insert--------------------------')
        # review테이블에서 삭제된 리뷰를 동일하게 anal00에서도 삭제
        db.TB_anal_00_delete()
        with open("./etc/last_isrt_dttm.txt","a",encoding='utf8') as f:
            f.write(f'\n{to_date}\t{isrt_dttm}\t분석완료')
    
    anal00_anal_code_list=db.ANAL00_AanlCode() # 키워드 키센텐스 실행을 위한 code_list

    num_cores=3 # multiprocessing의 process 개수

    anal3 = keysentence.total_multi(anal00_anal_code_list, num_cores)
    anal2 = keysentence.emo_multi(anal00_anal_code_list, num_cores)
    
    # anal03 / anal02 insert
    db.TB_anal_03_insert(anal3)
    db.TB_anal_02_insert(anal2)

    # anal01 procesure
    db.TB_anal_01_insert()
    
    finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{finish_time} 분석 완료")

    
if __name__=='__main__':
    error_list=[]
    time_list=[]
    today_path=db.today_path()      # 백업을 위한 폴더 생성 

    try:
        start_time=time.time()
        analysis()
        end_time=time.time()
        all_time=end_time-start_time
        
        # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d-%H%M%S'),"all_analy","-","-",all_time]
        db.time_txt(time_list,f'{today_path}/time_check')
        #db.success_sendEmail()

    except Exception:
        err=traceback.format_exc()
        print(f'1번째 error\n{err}')
        now=datetime.now().strftime('%Y%m%d %H:%M:%S')
        e=f'{now}\n{err}'
        error_list.append(e)
        db.save_txt(error_list,f'{today_path}/errorList')
        #db.fail_sendEmail(e)
        #analysis()