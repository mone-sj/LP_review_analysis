# -*- coding:utf-8 -*-
import os, io, time, kss
from datetime import datetime
from kss import split_sentences
from krwordrank.sentence import keysentence, summarize_with_sentences, make_vocab_score, MaxScoreTokenizer
from krwordrank.hangle import normalize
from numpy.core.arrayprint import SubArrayFormat
from numpy.core.numeric import NaN
from numpy.lib.npyio import save
import pandas as pd
import numpy as np
from keyword_lib import *
from krwordrank.word import *


stopword=stopwords('stopwords_living_ver1.0.txt')
path='./key_data/'
file_list=os.listdir(path)

#def keyword():
for i in file_list:
    
    # 전체리뷰만 리스트로 만들기
    
    f['review_content'] = df['review_content'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
    review_content=f['review_content'].tolist()

    analy_cd=f.iloc[0,0]
    product_name=f.iloc[0,1]
    jata='자사' #자사타사 구별 
    coupang = '1' 
    naver = '0' #사이트 구분 0:네이버/1:쿠팡
    keywords = '0' #0:키워드/1:문장
    sentences ='1'
    neg='N'
    pos='P'


    
    analy_cd_list=[analy_cd]
    product_name_list=[product_name]
   
    site_list=[naver]
    model_list=[keywords]
    
    # 전체 키워드
    try:
        all_keyword=keyword(review_content, stopword) #딕셔너리 {단어-점수}
        All_keywordList=list(all_keyword.keys())
        All_keywordGradeList=list(all_keyword.values())
        print(len(All_keywordList))
        All_keywordGradeList = np.round(All_keywordGradeList,2)

    
        df_x = pd.DataFrame({'0': All_keywordList})
        df_y = pd.DataFrame(columns={'0'})
        print(df_x)
        x = len(All_keywordList)
        if len(All_keywordList)< 6 :
            for x in range(5-x):
                df_y.loc[x] = NaN
            df_key = df_x.append(df_y,ignore_index=True)
            print(df_key)
        else :
            df_key = df_x.head(5)
            print(df_key)
        
        df_a = pd.DataFrame({'0': All_keywordGradeList})
        df_b = pd.DataFrame(columns={'0'})
        a = len(All_keywordGradeList)
        if len(All_keywordGradeList)< 6 :
            for a in range(5-a):
                df_b.loc[a] = NaN
            df_sentence = df_a.append(df_b,ignore_index=True)
            print(df_sentence)
        else:
            df_sentence = df_a.head(5)
            print(df_sentence)


        result = df_key.append(df_sentence,ignore_index=True)
        result = result.transpose()
        

        result_list = result.values.tolist()
        list1 = sum(result_list, [])

        all_keyword_result_df=pd.DataFrame({
            'ANALY_CD':analy_cd_list,
            'KEYWORD_GUBUN':model_list,
            'SITE_GUBUN':site_list,
            'KEY1' : list1[0],
            'KEY2' : list1[1],
            'KEY3' : list1[2],
            'KEY4' : list1[3],
            'KEY5' : list1[4] ,
            'KEY_GRADE1' : list1[5],
            'KEY_GRADE2' : list1[6],
            'KEY_GRADE3' : list1[7],
            'KEY_GRADE4' : list1[8],
            'KEY_GRADE5' : list1[9]
        })
        all_keyword_result_df.to_csv('./result/전체키워드/전체키워드_{}.csv'.format(i,product_name), index=False)
        print('=====\n{} 전체리뷰키워드 저장'.format(product_name))


    except Exception as e:
        print(e)
        print("{} 전체리뷰 키워드 오류".format(product_name))
        pass
    analy_cd_list=[analy_cd]
    product_name_list=[product_name]
   
    site_list=[naver]
    model_list=[sentences]
 
 #전체리뷰 키센텐스
    try:
        vocab_score = make_vocab_score(all_keyword, stopword, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
        print('vocab_score 완료')
        tokenizer = MaxScoreTokenizer(vocab_score)
        print('tokenizer 완료')
        keysentence_list_all=keysentence_list(review_content,vocab_score,tokenizer)
        list2 = sum(keysentence_list_all, [])
        print(len(keysentence_list_all))
        print('keysentence_list_all 완료')

        df_x = pd.DataFrame({'0': list2})
        print(df_x)
        df_y = pd.DataFrame(columns={'0'})
        print(df_y)
        x = len(list2)
        if len(list2)< 6 :
            for x in range(5-x):
                df_y.loc[x] = NaN
            df_keysentence = df_x.append(df_y,ignore_index=True)
        else:
            df_keysentence = df_x.head(5)
            print(df_keysentence)

        result = df_keysentence.transpose()
        result_list = result.values.tolist()
        list1 = sum(result_list, [])
        print(len(list1))
        all_keysentece_result_df=pd.DataFrame({
            'ANALY_CD':analy_cd_list,
            'KEYWORD_GUBUN':model_list,
            'SITE_GUBUN':site_list,
            'KEY1' : list1[0],
            'KEY2' : list1[1],
            'KEY3' : list1[2],
            'KEY4' : list1[3],
            'KEY5' : list1[4] ,            
        })
    
        all_keysentece_result_df.to_csv('./result/전체핵심문장/전체핵심문장_{}.csv'.format(product_name),index=False)
        print('=====\n{} 전체리뷰키센텐스 저장'.format(product_name))
    except Exception as e:
        print(e)
        print("{} 전체리뷰 키센텐스 오류".format(product_name))
        pass 


    analy_cd_list=[analy_cd]
    product_name_list=[product_name]
   
    site_list=[naver]
    model_list=[keywords]
    emo_list = [pos]
    
    # 긍정 키워드
    pos_df=f[f['empathy_score']>3]
    # 긍정 리뷰 리스트
    pos_review_content_list=pos_df['review_content'].tolist()
    try:
        pos_keyword=keyword(pos_review_content_list, stopword) #딕셔너리 {단어-점수}
        pos_keywordList=list(pos_keyword.keys())
        pos_keywordGradeList=list(pos_keyword.values())
        print(len(pos_keywordList))
        pos_keywordGradeList = np.round(pos_keywordGradeList,2)

    
        df_x = pd.DataFrame({'0': pos_keywordList})
        df_y = pd.DataFrame(columns={'0'})
        print(df_x)
        x = len(pos_keywordList)
        if len(pos_keywordList)< 6 :
            for x in range(5-x):
                df_y.loc[x] = NaN
            df_key = df_x.append(df_y,ignore_index=True)
            print(df_key)
        else :
            df_key = df_x.head(5)
            print(df_key)

        
        df_a = pd.DataFrame({'0': pos_keywordGradeList})
        df_b = pd.DataFrame(columns={'0'})
        a = len(pos_keywordGradeList)
        if len(pos_keywordGradeList)< 6 :
            for a in range(5-a):
                 df_b.loc[a] = NaN
            df_sentence = df_a.append(df_b,ignore_index=True)
            print(df_sentence)
        else:
            df_sentence = df_a.head(5)
            print(df_sentence)


        result = df_key.append(df_sentence,ignore_index=True)
        result = result.transpose()
        

        result_list = result.values.tolist()
        list1 = sum(result_list, [])
        print(len(list1))
        pos_keyword_result_df=pd.DataFrame({
            'ANALY_CD':analy_cd_list,
            'KEYWORD_GUBUN':model_list,
            'KEYWORD_POSITIVE' : emo_list,
            'SITE_GUBUN':site_list,
            'KEY1' : list1[0],
            'KEY2' : list1[1],
            'KEY3' : list1[2],
            'KEY4' : list1[3],
            'KEY5' : list1[4] ,
            'KEY_GRADE1' : list1[5],
            'KEY_GRADE2' : list1[6],
            'KEY_GRADE3' : list1[7],
            'KEY_GRADE4' : list1[8],
            'KEY_GRADE5' : list1[9]
        })
        pos_keyword_result_df.to_csv('./result/긍부정키워드/긍정키워드_{}.csv'.format(product_name), index=False)
        print('=====\n{} 긍정리뷰키워드 저장'.format(product_name))


    except Exception as e:
        print(e)
        print("{} 긍정키워드 오류".format(product_name))
        pass

    model_list=[sentences]
 
 #긍정리뷰 키센텐스
    try:
        pos_keyword=keyword(pos_review_content_list,stopword)
        vocab_score = make_vocab_score(pos_keyword, stopword, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
        print('vocab_score 완료')
        tokenizer = MaxScoreTokenizer(vocab_score)
        print('tokenizer 완료')
        keysentence_list_pos=keysentence_list(pos_review_content_list,vocab_score,tokenizer)
        list2 = sum(keysentence_list_pos, [])
        print(len(keysentence_list_pos))
        print('keysentence_list_pos 완료')

        df_x = pd.DataFrame({'0': list2})
        df_y = pd.DataFrame(columns={'0'})
        print(df_x)
        x = len(list2)
        if len(list2)< 6 :
            for x in range(5-x):
                df_y.loc[x] = NaN
            df_keysentence = df_x.append(df_y,ignore_index=True)
        else:
            df_keysentence  = df_x.head(5)
            print(df_keysentence)
        
        
        result = df_keysentence.transpose()
        result_list = result.values.tolist()
        list1 = sum(result_list, [])
        print(len(list1))
        pos_keys_result_df=pd.DataFrame({
            'ANALY_CD':analy_cd_list,
            'KEYWORD_GUBUN':model_list,
            'KEYWORD_POSITIVE' : emo_list,
            'SITE_GUBUN':site_list,
            'KEY1' : list1[0],
            'KEY2' : list1[1],
            'KEY3' : list1[2],
            'KEY4' : list1[3],
            'KEY5' : list1[4] ,            
        })
    
        pos_keys_result_df.to_csv('./result/긍부정핵심문장/긍정핵심문장_{}.csv'.format(product_name),index=False)
        print('=====\n{} 긍정리뷰키센텐스 저장'.format(product_name))
    except Exception as e:
        print(e)
        print("{} 긍정리뷰 키센텐스 오류".format(product_name))
        pass 


    model_list=[keywords]
    emo_list = [neg]
    
    # 부정 키워드
    neg_df=f[f['empathy_score']<3]
    # 부정리뷰 리스트
    neg_review_content_list=neg_df['review_content'].tolist()
    try:
        neg_keyword=keyword(neg_review_content_list, stopword) #딕셔너리 {단어-점수}
        neg_keywordList=list(neg_keyword.keys())
        neg_keywordGradeList=list(neg_keyword.values())
        print(len(neg_keywordList))
        neg_keywordGradeList = np.round(neg_keywordGradeList,2)

    
        df_x = pd.DataFrame({'0': neg_keywordList})
        df_y = pd.DataFrame(columns={'0'})
        print(df_x)
        x = len(neg_keywordList)
        if len(neg_keywordList)< 6 :
            for x in range(5-x):
                df_y.loc[x] = NaN
            df_key = df_x.append(df_y,ignore_index=True)
            print(df_key)
        else :
            df_key = df_x.head(5)
            print(df_key)
        
        df_a = pd.DataFrame({'0': neg_keywordGradeList})
        df_b = pd.DataFrame(columns={'0'})
        a = len(neg_keywordGradeList)
        if len(neg_keywordGradeList)< 6 :
            for a in range(5-a):
                 df_b.loc[a] = NaN
            df_sentence = df_a.append(df_b,ignore_index=True)
            print(df_sentence)
        else:
            df_sentence = df_a.head(5)
            print(df_sentence)


        result = df_key.append(df_sentence,ignore_index=True)
        result = result.transpose()
        

        result_list = result.values.tolist()
        list1 = sum(result_list, [])
        print(len(list1))
        neg_keyword_result_df=pd.DataFrame({
            'ANALY_CD':analy_cd_list,
            'KEYWORD_GUBUN':model_list,
            'KEYWORD_POSITIVE' : emo_list,
            'SITE_GUBUN':site_list,
            'KEY1' : list1[0],
            'KEY2' : list1[1],
            'KEY3' : list1[2],
            'KEY4' : list1[3],
            'KEY5' : list1[4] ,
            'KEY_GRADE1' : list1[5],
            'KEY_GRADE2' : list1[6],
            'KEY_GRADE3' : list1[7],
            'KEY_GRADE4' : list1[8],
            'KEY_GRADE5' : list1[9]
        })
        neg_keyword_result_df.to_csv('./result/긍부정키워드/부정키워드_{}.csv'.format(product_name), index=False)
        print('=====\n{} 부정리뷰키워드 저장'.format(product_name))


    except Exception as e:
        print(e)
        print("{} 부정키워드 오류".format(product_name))
        pass

    model_list=[sentences]
 
 #부정리뷰 키센텐스
    try:
        neg_keyword=keyword(neg_review_content_list,stopword)
        vocab_score = make_vocab_score(neg_keyword, stopword, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
        print('vocab_score 완료')
        tokenizer = MaxScoreTokenizer(vocab_score)
        print('tokenizer 완료')
        keysentence_list_neg=keysentence_list(neg_review_content_list,vocab_score,tokenizer)
        list2 = sum(keysentence_list_neg, [])
        print(len(keysentence_list_neg))
        print('keysentence_list_neg 완료')

        df_x = pd.DataFrame({'0': list2})
        df_y = pd.DataFrame(columns={'0'})
        print(df_x)
        x = len(list2)
        if len(list2)< 6 :
            for x in range(5-x):
                df_y.loc[x] = NaN
            df_keysentence = df_x.append(df_y,ignore_index=True)
        else:
            df_keysentence = df_x.head(5)
            print(df_keysentence)

        result = df_keysentence.transpose()
        

        result_list = result.values.tolist()
        list1 = sum(result_list, [])
        print(len(list1))
        neg_keys_result_df=pd.DataFrame({
            'ANALY_CD':analy_cd_list,
            'KEYWORD_GUBUN':model_list,
            'KEYWORD_POSITIVE' : emo_list,
            'SITE_GUBUN':site_list,
            'KEY1' : list1[0],
            'KEY2' : list1[1],
            'KEY3' : list1[2],
            'KEY4' : list1[3],
            'KEY5' : list1[4] ,            
        })
    
        neg_keys_result_df.to_csv('./result/긍부정핵심문장/부정핵심문장_{}.csv'.format(product_name),index=False)
        print('=====\n{} 부정리뷰키센텐스 저장'.format(product_name))
    except Exception as e:
        print(e)
        print("{} 부정리뷰 키센텐스 오류".format(product_name))
        pass  