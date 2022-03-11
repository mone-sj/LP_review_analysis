#-*- coding: utf-8 -*-
import requests, time
from datetime import datetime
from classification import predict
from DB_conn import db

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

today_path=db.today_path()

time_list=[]
error_list=[]

# 충북서버 LP용 감정분석 및 분류모델 URL
#classify_api = 'https://192.168.200.83:30001/deployment/hd9f94c1d7eec27394bce84741124f49b/'
# Lipaco 서버용 분류모델 URL
classify_api='http://localhost/deployment/hab4e8860edc765dda4e30d629d0e196a/'

# acryl서버 내 LP용 감정분석
#empathy_url = 'https://flightbase.acryl.ai/deployment/ha43a1099d17205a660a517a62bfd5203/'
# Lipaco 서버용 감정분석 모델 URL
empathy_api='http://localhost/deployment/hd3656bd08ad06a0cad88f7319a2c49d9/'


# 감정별 점수 리스트
score_5 = ['황홀함', '행복', '기쁨', '즐거움', '홀가분함', '자신감']
score_4 = ['사랑', '그리움', '감동', '고마움', '만족', '설렘', '바람', '놀람']
score_3 = ['중립', '무관심']
score_2 = ['슬픔', '미안함', '부러움', '안타까움', '괴로움', '불안', '창피함', '당황', '지루함']
score_1 = ['외로움', '후회', '실망', '두려움', '싫음', '미워함', '짜증남', '분노', '억울함']

# 감정분류, 속성분류
def api(df): # api
    ''' 감정분류/속성분류 API를 이용한 분류 예측
    '''
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    df.to_csv(f'{today_path}/{now}_TB_review_data.csv', index=None)
    print(f'property+empathy_analysis 시작: {now}')
    print(f"anal00 분석리뷰수: {len(df)}")
    
    
    start_time=time.time()
    result_df=df.copy()

    # 감정,감정점수,분류 결과 컬럼 추가
    result_df['EMPATHY']=''
    result_df['EMPATHY_SCORE']=''
    result_df['CLASSIFY']=''

    for i in range(len(result_df)):
        print(f'{i+1}번째 property+empathy 분석')
        review=result_df.iloc[i,3]
        try:
            response_empathy=requests.post(empathy_api,json={'text':review},verify=False,timeout=180)
        except:
            time.sleep(2)
            response_empathy=requests.post(empathy_api,json={'text':review},verify=False,timeout=180)
        
        result_empathy=response_empathy.json()
        output_empathy=result_empathy.get('columnchart')[0].get('output')[0]
        output_first_empathy=list(output_empathy.keys())[0]

        # empathy_score
        if output_first_empathy in score_5:
            score = 5
        elif output_first_empathy in score_4:
            score = 4
        elif output_first_empathy in score_3:
            score = 3
        elif output_first_empathy in score_2:
            score = 2
        elif output_first_empathy in score_1:
            score = 1

        # classify
        try:
            response_classify = requests.post(classify_api,json={'text':review},verify=False,timeout=180)
        except:
            time.sleep(2)
            response_classify = requests.post(classify_api,json={'text':review},verify=False,timeout=180)

        result_classify=response_classify.json()

        output_classify=result_classify.get('text')[0]
        output_first_classify=list(output_classify.values())[0]
        
        # result_df 추가하기
        result_df.iloc[i,4]=output_first_empathy
        result_df.iloc[i,5]=score
        result_df.iloc[i,6]=output_first_classify
    
    data=result_df[['REVIEW_DOC_NO','ANAL_CODE','CLASSIFY','EMPATHY','EMPATHY_SCORE']]

    df_anal=data.drop_duplicates(['ANAL_CODE'])
    anal_list=df_anal['ANAL_CODE'].tolist()

    end_time=time.time()
    exe_time=end_time-start_time
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    print(f'property+empathy_analysis 완료: {now}')
    # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
    time_list=[now,"empathy+classify_api",len(anal_list),len(data),exe_time]

    #save
    db.time_txt(time_list,f'{today_path}/time_check')
    db.save_txt(error_list,f'{today_path}/errorList')
    data.to_csv(f'{today_path}/{now}_anal00_result.csv', index=None)
    return data


def model_pt(df): # API를 통한 결과값 출력의 실행시간을 단축하기 위해, 모델 파일을 사용
    ''' 감정분류API, 속성분류 모델파일(classification 폴더 내)을 이용한 분류 예측
    '''
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    df.to_csv(f'{today_path}/{now}_TB_review_data.csv', index=None)
    print(f'property+empathy_analysis 시작: {now}')
    print(f"anal00 분석리뷰수: {len(df)}")

    start_time=time.time()
    result_df=df.copy()

    # 감정,감정점수,분류 결과 컬럼 추가
    result_df['EMPATHY']='' #row[4]
    result_df['EMPATHY_SCORE']='' #row[5]
    result_df['CLASSIFY']='' #row[6]

    for cnt in range(len(result_df)):
        print(f'{cnt+1}번째 property+empathy 분석')
        review=result_df.iloc[cnt,3]
        
        # empathy_classification
        try:
            response_empathy=requests.post(empathy_api,json={'text':review},verify=False,timeout=180)
        except:
            time.sleep(2)
            response_empathy=requests.post(empathy_api,json={'text':review},verify=False,timeout=180)
        
        result_empathy=response_empathy.json()

        output_empathy=result_empathy.get('columnchart')[0].get('output')[0]
        output_first_empathy=list(output_empathy.keys())[0]

        # empathy_score
        if output_first_empathy in score_5:
            score = 5
        elif output_first_empathy in score_4:
            score = 4
        elif output_first_empathy in score_3:
            score = 3
        elif output_first_empathy in score_2:
            score = 2
        elif output_first_empathy in score_1:
            score = 1
        
        result_df.iloc[cnt,4]=output_first_empathy
        result_df.iloc[cnt,5]=score

        classify=predict.predict(review)
        result_df.iloc[cnt,6]=classify
    
    data=result_df[['REVIEW_DOC_NO','ANAL_CODE','CLASSIFY','EMPATHY','EMPATHY_SCORE']]

    df_anal=data.drop_duplicates(['ANAL_CODE'])
    anal_list=df_anal['ANAL_CODE'].tolist()

    end_time=time.time()
    exe_time=end_time-start_time
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    print(f'property+empathy_analysis 완료: {now}')
    # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
    time_list=[now,"empathy+classify_pt",len(anal_list),len(data),exe_time]

    #save
    db.time_txt(time_list,f'{today_path}/time_check')
    db.save_txt(error_list,f'{today_path}/errorList')
    data.to_csv(f'{today_path}/{now}_anal00_result.csv', index=None)
    return data
