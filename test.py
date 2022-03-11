#-*- coding:utf-8 -*-
from DB_conn import db
from datetime import datetime
import pandas as pd
import keysentence
import time, traceback
#import multiprocessing
from multiprocessing import Pool
from datetime import datetime
import numpy as np


#anal3=pd.read_csv('./etc/result_data/20220120/220120_1009_anal03_result_test.csv')

#list1=['ANAL-211104-0001']
#anal3 = keysentence.total(list1)
#anal2=keysentence.emo(list1)

def analysis():
    anal00_anal_code_list=db.ANAL00_AanlCode() # 키워드 키센텐스 실행을 위한 code_list
    print(f'analy_code 개수: {len(anal00_anal_code_list)}')
    
    # tb_anal03=keysentence.total(anal00_anal_code_list)
    tb_anal02=keysentence.emo(anal00_anal_code_list)
    
    # print(f'anal03길이: {len(tb_anal03)}')
    # print(tb_anal03)
    print(f'anal02길이: {len(tb_anal02)}')
    print(tb_anal02)
    # anal03 / anal02 insert
    #db.TB_anal_03_insert(tb_anal03)
    #db.TB_anal_02_insert(anal2)

    # anal01 procesure
    #db.TB_anal_01_insert()
    
    finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{finish_time} 분석 완료")

def parallelize_dataframe(code_list, func, num_cores):
    list_split = np.array_split(code_list, num_cores)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, list_split), ignore_index=True)
    pool.close()
    pool.join()
    return df


def analysis_multi():
    anal00_anal_code_list=db.ANAL00_AanlCode() # 키워드 키센텐스 실행을 위한 code_list
    print(f'analy_code 개수: {len(anal00_anal_code_list)}')

    #tb_anal03=parallelize_dataframe(anal00_anal_code_list, keysentence.total, 3)
    tb_anal02=parallelize_dataframe(anal00_anal_code_list, keysentence.emo, 3)
    
    #tb_anal03=keysentence.total(anal00_anal_code_list)
    '''
    tb_anal03=pd.DataFrame()
    pool=multiprocessing.Pool(processes=3)
    result=pool.map(keysentence.total, anal00_anal_code_list)
    pool.close()
    pool.join()
    for anal3_result in result:
        tb_anal03=pd.concat([tb_anal03, anal3_result], ignore_index=True)
    
    # for anal3 in pool.map(keysentence.total, anal00_anal_code_list):
    #     for anal03 in anal3:
    #         tb_anal03=pd.concat([tb_anal03, anal03], ignore_index=True)
    # pool.close()
    # pool.join()
    '''
    print(f'anal02길이: {len(tb_anal02)}')
    print(tb_anal02)
    
    finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{finish_time} 분석 완료")


if __name__=='__main__':
    error_list=[]
    time_list=[]
    today_path=db.today_path()      # 백업을 위한 폴더 생성 

    try:
        start_time=time.time()
        #analysis()
        analysis_multi()
        end_time=time.time()
        all_time=end_time-start_time
        
        # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"all_analy","-","-",all_time]
        db.time_txt(time_list,f'{today_path}/time_check')
        

    except Exception:
        err=traceback.format_exc()
        print(f'1번째 error\n{err}')
        now=datetime.now().strftime('%Y%m%d %H:%M')
        e=f'{now}\n{err}'
        error_list.append(e)
        db.save_txt(error_list,f'{today_path}/errorList')
        #analysis()