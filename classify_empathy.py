#-*- coding: utf-8 -*-
import requests, sys
import pandas as pd
import datetime
import time
from .classification import predict
import urllib3

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# 충북서버 LP용 감정분석 및 분류모델 URL
#url = 'https://192.168.200.83:30001/deployment/h84b5e7b9deb74c30e55d78f38c3a10d1/'
empathy_url = 'https://flightbase.acryl.ai/deployment/ha43a1099d17205a660a517a62bfd5203/'
classify_url = 'https://192.168.200.83:30001/deployment/hd9f94c1d7eec27394bce84741124f49b/'


# 감정별 점수 리스트
score_5 = ['황홀함', '행복', '기쁨', '즐거움', '홀가분함', '자신감']
score_4 = ['사랑', '그리움', '감동', '고마움', '만족', '설렘', '바람', '놀람']
score_3 = ['중립', '무관심']
score_2 = ['슬픔', '미안함', '부러움', '안타까움', '괴로움', '불안', '창피함', '당황', '지루함']
score_1 = ['외로움', '후회', '실망', '두려움', '싫음', '미워함', '짜증남', '분노', '억울함']


# 감정분류, 속성분류

def api(df): # api
    result_df=df.copy()
    #result_df= result_df[0:5]
    # 감정,감정점수,분류 결과 컬럼 추가
    result_df['EMPATHY']=''
    result_df['EMPATHY_SCORE']=''
    result_df['CLASSIFY']=''
    
   
    for i in range(len(result_df)):
        review=result_df.iloc[i,3]
        # empathy
        response_empathy=requests.post(empathy_url,json={'text':review},verify=True,timeout=180)
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
        response_classify = requests.post(classify_url,json={'text':review},verify=False,timeout=180)
        result_classify=response_classify.json()

        output_classify=result_classify.get('text')[0]
        output_first_classify=list(output_classify.values())[0]
        
        # result_df 추가하기
        result_df.iloc[i,4]=output_first_empathy
        result_df.iloc[i,5]=score
        result_df.iloc[i,6]=output_first_classify
        data=result_df[['REVIEW_DOC_NO','PART_ID','CLASSIFY','EMPATHY','EMPATHY_SCORE']]
        
    return data


def model_pt(df):
    print('property+empathy_analysis')

    result_df=df.copy()
    # 감정,감정점수,분류 결과 컬럼 추가
    result_df['EMPATHY']='' #row[4]
    result_df['EMPATHY_SCORE']='' #row[5]
    result_df['CLASSIFY']='' #row[6]
    print(result_df.head(2))


    for cnt in range(len(result_df)):
        print('{}번째 property+empathy 분석'.format(cnt+1))
        # model_id
        review=result_df.iloc[cnt,3]
        

        # empathy_classification
        try:
            response_empathy=requests.post(empathy_url,json={'text':review},verify=True,timeout=180)
        except:
            time.sleep(2)
            response_empathy=requests.post(empathy_url,json={'text':review},verify=True,timeout=180)
        
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

        classify=predict(review)
        result_df.iloc[cnt,6]=classify
    data=result_df[['REVIEW_DOC_NO','PART_ID','CLASSIFY','EMPATHY','EMPATHY_SCORE']]
    return data



    