# -*- coding:utf-8 -*-
import os, io, time, kss
from krwordrank import sentence
from datetime import datetime
from krwordrank.sentence import keysentence, summarize_with_sentences, make_vocab_score, MaxScoreTokenizer
from krwordrank.hangle import normalize
from numpy.core.arrayprint import SubArrayFormat
from numpy.core.numeric import NaN
from numpy.lib.npyio import save
import pandas as pd
import numpy as np
from keys.keyword_lib import *
from krwordrank.word import *
from keys.keysentence_lib import *

from DB_conn import db


def total(code_list):

    col_name3=["ANAL_CODE","KEYWORD_GUBUN","SITE_GUBUN","RLT_VALUE_01","RLT_VALUE_02","RLT_VALUE_03","RLT_VALUE_04","RLT_VALUE_05",
    "RLT_VALUE_06","RLT_VALUE_07","RLT_VALUE_08","RLT_VALUE_09","RLT_VALUE_10"]
    data_anal03=pd.DataFrame(columns=col_name3)
    print(data_anal03)

    for i in code_list:
    #range(len(code_list)):

        code = i #code_list[i]
        try:
            select_conn=db.select_conn()

            df_A=db.TB_REVIEW_A(select_conn,code)
            df_B=db.TB_REVIEW_B(select_conn,code)

            df = pd.merge(df_A,df_B,on='REVIEW_DOC_NO')
            select_conn.close()
        except Exception as e:
            select_conn.close()

        print(df)

        conn = db.select_conn()
        stopword=db.TB_UNUSE_KEYWORD(conn)
        conn.close()  

        df['REVIEW'] = df['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
        review_content=df['REVIEW'].tolist()

        analy_cd=df.iloc[0,2]
        site = df.iloc[0,0]  #사이트 구분 0:네이버/1:쿠팡
        keywords = '0' #0:키워드/1:문장
        sentences ='1'
        neg='N'
        pos='P'


        # 전체 키워드
        try:
            all_keyword=keyword(review_content, stopword) #딕셔너리 {단어-점수}
            All_keywordList=list(all_keyword.keys())
            All_keywordGradeList=list(all_keyword.values())
            print(len(All_keywordList))
            All_keywordGradeList = np.round(All_keywordGradeList,2)
            print(All_keywordGradeList)

            df_x = pd.DataFrame({'0': All_keywordList})
            df_y = pd.DataFrame(columns={'0'})
            print(df_x)
            x = len(All_keywordList)
            if len(All_keywordList)< 6 :
                for x in range(5-x):
                    df_y.loc[x] = ''
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
                    df_b.loc[a] = ''
                df_sentence = df_a.append(df_b,ignore_index=True)
                print(df_sentence)
            else:
                df_sentence = df_a.head(5)
                print(df_sentence)


            result = df_key.append(df_sentence,ignore_index=True)
            result = result.transpose()
                

            result_list = result.values.tolist()
            list1 = sum(result_list, [])
            model_list=[keywords]

            all_keyword_result_df=pd.DataFrame({
                'ANAL_CODE':analy_cd,
                'KEYWORD_GUBUN':model_list,
                'SITE_GUBUN':site,
                'RLT_VALUE_01' : list1[0],
                'RLT_VALUE_02' : list1[1],
                'RLT_VALUE_03' : list1[2],
                'RLT_VALUE_04' : list1[3],
                'RLT_VALUE_05' : list1[4] ,
                'RLT_VALUE_06' : list1[5],
                'RLT_VALUE_07' : list1[6],
                'RLT_VALUE_08' : list1[7],
                'RLT_VALUE_09' : list1[8],
                'RLT_VALUE_10' : list1[9]
            })
            print(all_keyword_result_df)


        except Exception as e:
            print(e)
            print("전체리뷰 키워드 오류")
            pass
        
        data_anal03 = pd.concat([data_anal03,all_keyword_result_df],ignore_index=True)
        print(data_anal03)
        
        #전체핵심문장
        model_list=[sentences]
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
                    df_y.loc[x] = ''
                df_keysentence = df_x.append(df_y,ignore_index=True)
            else:
                df_keysentence = df_x.head(5)
                print(df_keysentence)

            result = df_keysentence.transpose()
            result_list = result.values.tolist()
            list1 = sum(result_list, [])
            print(len(list1))
            all_keysentece_result_df=pd.DataFrame({
                'ANAL_CODE':analy_cd,
                'KEYWORD_GUBUN':model_list,
                'SITE_GUBUN':site,
                'RLT_VALUE_01' : list1[0],
                'RLT_VALUE_02' : list1[1],
                'RLT_VALUE_03' : list1[2],
                'RLT_VALUE_04' : list1[3],
                'RLT_VALUE_05' : list1[4] ,       
            })
           
        except Exception as e:
            print(e)
            print("{} 전체리뷰 키센텐스 오류".format(analy_cd))
            pass

        data_anal03 = pd.concat([data_anal03,all_keysentece_result_df],ignore_index=True)
        print(data_anal03)

    return data_anal03







def emo(code_list):
    #code_list = ['214112171','5470240040']


    col_name2=["ANAL_CODE","KEYWORD_GUBUN","KEYWORD_POSITIVE","SITE_GUBUN","RLT_VALUE_01","RLT_VALUE_02","RLT_VALUE_03","RLT_VALUE_04","RLT_VALUE_05",
    "RLT_VALUE_06","RLT_VALUE_07","RLT_VALUE_08","RLT_VALUE_09","RLT_VALUE_10"]
    data_anal02=pd.DataFrame(columns=col_name2)

    for i in range(len(code_list)):

        code = code_list[i]
        try:
            select_conn=db.select_conn()

            df=db.TB_REIVEW_join(select_conn,code)
        except Exception as e:
            select_conn.close()

        print(df)
        select_conn.close()


        stopword=stopwords('./keys/stopwords_living_ver1.0.txt')

        df['REVIEW'] = df['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
        review_content=df['REVIEW'].tolist()

        analy_cd=df.iloc[0,2]
        site = df.iloc[0,0]  #사이트 구분 0:네이버/1:쿠팡
        keywords = '0' #0:키워드/1:문장
        sentences ='1'
        neg='N'
        pos='P'

        # 긍정 키워드
        pos_df=df[df['RLT_VALUE_03']>3]
        df['REVIEW'] = df['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
        # 긍정 리뷰 리스트
        pos_review_list=pos_df['REVIEW'].tolist()

        try:
            pos_keyword=keyword(pos_review_list, stopword) #딕셔너리 {단어-점수}
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
                    df_y.loc[x] = ''
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
                    df_b.loc[a] = ''
                df_sentence = df_a.append(df_b,ignore_index=True)
                print(df_sentence)
            else:
                df_sentence = df_a.head(5)
                print(df_sentence)


            result = df_key.append(df_sentence,ignore_index=True)
            result = result.transpose()
            

            result_list = result.values.tolist()
            list1 = sum(result_list, [])

            pos_keyword_result_df=pd.DataFrame({
                'ANAL_CODE':analy_cd,
                'KEYWORD_GUBUN':keywords,
                'KEYWORD_POSITIVE':pos,
                'SITE_GUBUN':site,
                'RLT_VALUE_01' : list1[0],
                'RLT_VALUE_02' : list1[1],
                'RLT_VALUE_03' : list1[2],
                'RLT_VALUE_04' : list1[3],
                'RLT_VALUE_05' : list1[4] ,
                'RLT_VALUE_06' : list1[5],
                'RLT_VALUE_07' : list1[6],
                'RLT_VALUE_08' : list1[7],
                'RLT_VALUE_09' : list1[8],
                'RLT_VALUE_10' : list1[9]
            })
            print(pos_keyword_result_df)


        except Exception as e:
            print(e)
            print("긍정키워드 오류")
            pass
        
        data_anal02 = pd.concat([data_anal02,pos_keyword_result_df],ignore_index=True)
        print(data_anal02)


        # 부정 키워드
        neg_df=df[df['RLT_VALUE_03']<3]
        df['REVIEW'] = df['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
        # 부정 리뷰 리스트
        neg_review_list=neg_df['REVIEW'].tolist()

        try:
            neg_keyword=keyword(neg_review_list, stopword) #딕셔너리 {단어-점수}
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
                    df_y.loc[x] = ''
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
                    df_b.loc[a] = ''
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
                'ANAL_CODE':analy_cd,
                'KEYWORD_GUBUN':keywords,
                'KEYWORD_POSITIVE':neg,
                'SITE_GUBUN':site,
                'RLT_VALUE_01' : list1[0],
                'RLT_VALUE_02' : list1[1],
                'RLT_VALUE_03' : list1[2],
                'RLT_VALUE_04' : list1[3],
                'RLT_VALUE_05' : list1[4] ,
                'RLT_VALUE_06' : list1[5],
                'RLT_VALUE_07' : list1[6],
                'RLT_VALUE_08' : list1[7],
                'RLT_VALUE_09' : list1[8],
                'RLT_VALUE_10' : list1[9]
            })
            print(neg_keyword_result_df)

            data_anal02 = pd.concat([data_anal02,neg_keyword_result_df],ignore_index=True)

        except Exception as e:
            print(e)
            print("부정키워드 오류")
            pass


    #긍정리뷰 키센텐스
        try:
            pos_keyword=keyword(pos_review_list,stopword)
            vocab_score = make_vocab_score(pos_keyword, stopword, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
            print('vocab_score 완료')
            tokenizer = MaxScoreTokenizer(vocab_score)
            print('tokenizer 완료')
            keysentence_list_pos=keysentence_list(pos_review_list,vocab_score,tokenizer)
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
                'ANALY_CD':analy_cd,
                'KEYWORD_GUBUN':sentences,
                'KEYWORD_POSITIVE' : pos,
                'SITE_GUBUN':site,
                'RLT_VALUE_01' : list1[0],
                'RLT_VALUE_02' : list1[1],
                'RLT_VALUE_03' : list1[2],
                'RLT_VALUE_04' : list1[3],
                'RLT_VALUE_05' : list1[4]   
            })
        
        except Exception as e:
            print(e)
            print("{} 긍정리뷰 키센텐스 오류".format(analy_cd))
            pass 

        data_anal02=pd.concat([data_anal02,pos_keys_result_df],ignore_index=True)

    #부정리뷰 키센텐스
        try:
            neg_keyword=keyword(neg_review_list,stopword)
            vocab_score = make_vocab_score(neg_keyword, stopword, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
            print('vocab_score 완료')
            tokenizer = MaxScoreTokenizer(vocab_score)
            print('tokenizer 완료')
            keysentence_list_neg=keysentence_list(neg_review_list,vocab_score,tokenizer)
            list2 = sum(keysentence_list_neg, [])
            print(len(keysentence_list_neg))
            print('keysentence_list_neg 완료')

            df_x = pd.DataFrame({'0': list2})
            df_y = pd.DataFrame(columns={'0'})
            print(df_x)
            x = len(list2)
            if len(list2)< 6 :
                for x in range(5-x):
                    df_y.loc[x] = ''
                df_keysentence = df_x.append(df_y,ignore_index=True)
            else:
                df_keysentence  = df_x.head(5)
                print(df_keysentence)
            
            
            result = df_keysentence.transpose()
            result_list = result.values.tolist()
            list1 = sum(result_list, [])
            print(len(list1))
            pos_keys_result_df=pd.DataFrame({
                'ANALY_CD':analy_cd,
                'KEYWORD_GUBUN':sentences,
                'KEYWORD_POSITIVE' : neg,
                'SITE_GUBUN':site,
                'RLT_VALUE_01' : list1[0],
                'RLT_VALUE_02' : list1[1],
                'RLT_VALUE_03' : list1[2],
                'RLT_VALUE_04' : list1[3],
                'RLT_VALUE_05' : list1[4]   
            })
        
        except Exception as e:
            print(e)
            print("{} 긍정리뷰 키센텐스 오류".format(analy_cd))
            pass 

        data_anal02=pd.concat([data_anal02,pos_keys_result_df],ignore_index=True)




    return data_anal02
