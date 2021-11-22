#-*- coding: utf-8 -*-
import requests, sys
import pandas as pd
import datetime

# 충북서버 LP용 감정분석 및 분류모델 URL
#url = 'https://192.168.200.83:30001/deployment/h84b5e7b9deb74c30e55d78f38c3a10d1/'
empathy_url = 'https://flightbase.acryl.ai/deployment/ha43a1099d17205a660a517a62bfd5203/'
classify_url = 'https://192.168.200.83:30001/deployment/he1572cbfbabe584b1ea6729b6dc89183/'




# 감정별 점수 리스트
score_5 = ['황홀함', '행복', '기쁨', '즐거움', '홀가분함', '자신감']
score_4 = ['사랑', '그리움', '감동', '고마움', '만족', '설렘', '바람', '놀람']
score_3 = ['중립', '무관심']
score_2 = ['슬픔', '미안함', '부러움', '안타까움', '괴로움', '불안', '창피함', '당황', '지루함']
score_1 = ['외로움', '후회', '실망', '두려움', '싫음', '미워함', '짜증남', '분노', '억울함']


# 감정분류, 속성분류

def how(df):
    result_df=df.copy()
    #result_df= result_df[0:5]
    # 감정,감정점수,분류 결과 컬럼 추가
    result_df['empathy']=''
    result_df['empathy_score']=''
    result_df['classify']=''
    
   
    for i in range(len(result_df)):
        review=result_df.iloc[i,3]
        # empathy
        response_empathy=requests.post(empathy_url,json={'text':review},verify=False,timeout=180)
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

        review=result_df.iloc[i,3]

        response_classify = requests.post(classify_url,json={'text':review},verify=False,timeout=180)
        result_classify = response_classify.json()

        output_classify=result_classify.get('text')[0]
        output_first_classify=list(output_classify.values())[0]
        print('done')



        # result_df 추가하기
        result_df.iloc[i,4]=output_first_empathy
        result_df.iloc[i,5]=score
        result_df.iloc[i,6]=output_first_classify
        
    return result_df



    