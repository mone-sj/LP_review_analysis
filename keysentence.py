# -*- coding:utf-8 -*-
from datetime import datetime
import time
import pandas as pd
import numpy as np
from keys.keyword_lib import *
from krwordrank.word import *
from keys.keysentence_lib import *
from DB_conn import db

today_path=db.today_path() # 당일 폴더 생성

time_list=[]                                                    # exe time check
none_review=[]
error_list=[]                                                   # error check list

def total(code_list):
    site_gubun=db.site_gubun_list()                             # load crawling site code

    col_name3=["ANAL_CODE","KEYWORD_GUBUN","SITE_GUBUN","RLT_VALUE_01","RLT_VALUE_02","RLT_VALUE_03","RLT_VALUE_04","RLT_VALUE_05",
    "RLT_VALUE_06","RLT_VALUE_07","RLT_VALUE_08","RLT_VALUE_09","RLT_VALUE_10"]
    data_anal03=pd.DataFrame(columns=col_name3)                  # analysis result Dataframe

    review_join=db.TB_REVIEW_join()
    total_time_start=time.time()
    review_count=0
    product_count=0

    for code in code_list:
        df=review_join[review_join['ANAL_CODE']==code]
        review_count+=len(df)
        product_count+=1

        stopword=db.TB_UNUSE_KEYWORD()                             # stopword load (불용어)
        for site_no in site_gubun:
            now=datetime.now().strftime('%y%m%d_%H%M')
            print(f"total - {len(code_list)}개 중 {product_count}번째 / anal_code: {code} / 사이트구분: {site_no} 분석시작 / 실행 시간: {now}")
            df_site_ori=df[df['SITE_GUBUN']==site_no]              # 사이트별로 리뷰 분류

            if len(df_site_ori)==0:
                print(f'anal_code: {code}\tsite_gubun:{site_no}\ttotal_review_없음')
                error_list.append(f'anal_code: {code}\tsite_gubun:{site_no}\ttotal_review_없음')
                none_review.append(f'anal_code: {code}\tsite_gubun:{site_no}\ttotal_review_없음')
            else:
                df_site=df_site_ori.copy()
                df_site['REVIEW'] = df_site['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
                review_content=df_site['REVIEW'].tolist()

                analy_cd=df_site.iloc[0,2]
                site = df_site.iloc[0,0]                                 # SITE_GUBUN - 0:naver / 1:coupang

                # 리뷰 5개 이하면 키워드 분석은 하지 않고, 리뷰를 최신순으로 핵심문장으로 출력
                if 0<len(df_site)<6:
                    # 리뷰 최신순으로 dataframe생성
                    list5 = df_site.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                    list5 = list5['REVIEW'].values.tolist()
                    
                    list_review=[]
                    for index in range(5):
                        try:
                            list_review.append(list5[index])
                        except:
                            list_review.append('')
                    
                    total_sentence=total_sent(analy_cd,site,list_review)
                   
                    # analysis result add (anal03에 추가)
                    data_anal03=pd.concat([data_anal03,total_sentence],ignore_index=True)

                # 리뷰가 6개 이상이면, 키워드/핵심문장 분석
                elif len(df_site)>5:
                    try:                                                     # 전체 키워드
                        all_keyword,error=keyword_minCount(review_content, stopword)
                        if '오류없음' not in error:                           # 오류 발생
                            all_keyword_result_df=key_df_error(analy_cd,site)
                            error_list.append(f'{analy_cd} / site: {site} 전체키워드1\t{error}')
                        else:
                            All_keywordList=list(all_keyword.keys())
                            All_keywordGradeList=list(all_keyword.values())
                            All_keywordGradeList = (np.round(All_keywordGradeList,2)).tolist() # 소수점 둘째자리까지 반올림

                            # List(빈칸처리)
                            key_list=noValueToBlank(All_keywordList)
                            grade_list=noValueToBlank(All_keywordGradeList)
                            result_list=key_list+grade_list
                            all_keyword_result_df=total_key_df_result(analy_cd,site,result_list)

                            print(f"code: {code} / site_no: {site_no} 전체리뷰 키워드 완료\n총 리뷰수:{len(df_site)}")

                    except Exception as e:
                        error_list.append(f"{analy_cd} / site: {site} 전체 키워드 오류2\t{e}")
                        print(f"{e}\n{analy_cd} / site: {site} 전체리뷰 키워드 오류")
                        all_keyword_result_df=key_df_error(analy_cd,site)
                        all_keyword={}
                        pass

                    # 전체핵심문장
                    try:
                        if not all_keyword:                                         # keyword 가 없을때, 리뷰의 최신순으로 핵심문장 출력
                            list5 = df_site.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                            list5 = list5['REVIEW'].values.tolist()
                            
                            list_review=[]
                            for index in range(5):
                                try:
                                    list_review.append(list5[index])
                                except:
                                    list_review.append('')

                            all_keysentece_result_df=total_sent(analy_cd, site, list_review)
                            
                        else:
                            print("전체 핵심문장 분석 시작")
                            keysentence_list_all, error=keys_list(all_keyword,stopword,review_content)
                            error_list.append(f"{analy_cd} / site: {site} 전체 핵심문장 오류3\t{error}")
                            
                            if len(keysentence_list_all)==0:                        # 핵심문장이 출력되지 않으면 리뷰 최신순으로 출력
                                list5 = df_site.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                                list5 = list5['REVIEW'].values.tolist()
                                
                                list_review=[]
                                for index in range(5):
                                    try:
                                        list_review.append(list5[index])
                                    except:
                                        list_review.append('')
                                
                                all_keysentece_result_df=total_sent(analy_cd,site,list_review)
                                
                            else:
                                keys_list_fin=noValueToBlank(keysentence_list_all)
                                all_keysentece_result_df=total_sent(analy_cd, site, keys_list_fin)

                            print(f"code: {code} / site_no: {site_no} 전체리뷰 핵심문장 완료\n총 리뷰수: {len(df_site)}")
                    except Exception as e:                       
                        print(e)
                        error_list.append(f"{analy_cd} / site: {site} 전체 핵심문장 오류4\t{e}")
                        print("{} 전체리뷰 키센텐스 오류".format(analy_cd))
                        all_keysentece_result_df=keys_df_error(analy_cd,site)
                        pass
                    
                    data_anal03 = pd.concat([data_anal03,all_keyword_result_df,all_keysentece_result_df],ignore_index=True)
                    del all_keyword_result_df                                                            # dataframe 초기화
                    del all_keysentece_result_df                                                         # dataframe 초기화

    # Time check
    total_time_end=time.time()
    total_time=total_time_end-total_time_start
    now=datetime.now().strftime('%y%m%d_%H%M')
    print(f'total_분석완료: {now}')
    # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
    time_list=[now,"total_key",len(code_list),review_count,total_time]
    
    # save
    db.time_txt(time_list,f'{today_path}/time_check')
    db.save_txt(none_review,f'{today_path}/noReviewProduct')
    db.save_txt(error_list,f'{today_path}/errorList')
    data_anal03.to_csv(f'{today_path}/{now}_anal03_result.csv', index=None)
    return data_anal03

def emo(code_list):
    site_gubun=db.site_gubun_list()     # load crawling site code

    col_name2=["ANAL_CODE","KEYWORD_GUBUN","KEYWORD_POSITIVE","SITE_GUBUN","RLT_VALUE_01","RLT_VALUE_02","RLT_VALUE_03","RLT_VALUE_04","RLT_VALUE_05","RLT_VALUE_06","RLT_VALUE_07","RLT_VALUE_08","RLT_VALUE_09","RLT_VALUE_10"]
    data_anal02=pd.DataFrame(columns=col_name2)

    review_join=db.TB_REVIEW_join()
    emo_time_start=time.time()
    review_count=0
    product_count=0

    for code in code_list:
        df=review_join[review_join['ANAL_CODE']==code]
        stopword=db.TB_UNUSE_KEYWORD()
        product_count+=1

        for site_no in site_gubun:
            now=datetime.now().strftime('%y%m%d_%H%M')
            print(f"emo - {len(code_list)}개 중 {product_count}번째 / anal_code: {code} / 사이트구분: {site_no} / 실행 시간: {now}")
            df_site_ori=df[df['SITE_GUBUN']==site_no]
            df_site=df_site_ori.copy()
            df_site['REVIEW'] = df_site_ori['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)

            if len(df_site)==0:
                none_review.append(f'{code}\tsite_gubun:{site_no}\temo_전체_review_없음')
            else:
                analy_cd=df_site.iloc[0,2]
                site = df_site.iloc[0,0]  #사이트 구분 0:네이버/1:쿠팡
                # keywords = '0' / sentences ='1' // neg='N' / pos='P'

                # 긍정 키워드
                pos_df=df_site[df_site['RLT_VALUE_03']>3]
                review_count+=len(pos_df)
                
                # 리뷰 5개 이하면 키워드 분석은 하지 않고, 리뷰를 최신순으로 핵심문장으로 출력
                if 0<len(pos_df)<6:
                    list5 = pos_df.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                    list5 = list5['REVIEW'].values.tolist()
                    
                    list_pos_review=[]
                    for index in range(5):
                        try:
                            list_pos_review.append(list5[index])
                        except:
                            list_pos_review.append('')

                    pos_sentence=emo_pos_sent(analy_cd,site,list_pos_review)
                    data_anal02=pd.concat([data_anal02,pos_sentence],ignore_index=True)

                elif len(pos_df)>5:
                    # 긍정 리뷰 리스트
                    print('긍정리뷰 키워드/핵심문장 분석 시작')
                    pos_review_list=pos_df['REVIEW'].tolist()

                    try:
                        pos_keyword,error=keyword_minCount(pos_review_list, stopword)

                        if '오류없음' not in error:               # 오류 발생
                            pos_keyword_result_df=pos_key_error(analy_cd,site)
                            error_list.append(f"{analy_cd} / site: {site} 긍정 키워드 에러1\t{error}")
                            print(f"{analy_cd} / site: {site} 긍정 키워드 에러1\t{error}")
                        else:
                            pos_keywordList=list(pos_keyword.keys())
                            pos_keywordGradeList=list(pos_keyword.values())
                            pos_keywordGradeList = (np.round(pos_keywordGradeList,2)).tolist() # 반올림

                            # list_빈칸처리
                            pos_key_list=noValueToBlank(pos_keywordList)
                            pos_grade_list=noValueToBlank(pos_keywordGradeList)
                            pos_result_list=pos_key_list+pos_grade_list
                            pos_keyword_result_df=pos_key_result(analy_cd,site,pos_result_list)
                            
                            print(f"code: {code} / site_no: {site_no} 긍정리뷰 키워드 완료\t긍정 리뷰수:{len(pos_df)}")

                    except Exception as e:
                        error_list.append(f"{analy_cd} / site: {site} 긍정 키워드 오류2\t{e}")
                        print(f"{e}\n{analy_cd} site:{site} 긍정키워드 오류")
                        pos_keyword_result_df=pos_key_error(analy_cd,site)
                        pos_keyword={}
                        pass
                    
                    #긍정리뷰 핵심문장
                    try:
                        print("긍정리뷰 핵심문장 분석 시작")
                        # 키워드 리스트가 없으면, 센텐스 최신순으로 출력
                        if not pos_keyword:
                            list5 = pos_df.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                            list5 = list5['REVIEW'].values.tolist()
                            
                            list_pos_review=[]
                            for index in range(5):
                                try:
                                    list_pos_review.append(list5[index])
                                except:
                                    list_pos_review.append('')

                            pos_keys_result_df=emo_pos_sent(analy_cd,site,list_pos_review)

                        else:
                            keysentence_list_pos, error=keys_list(pos_keyword,stopword,pos_review_list)
                            
                            if '오류없음' not in error:
                                error_list.append(f"{analy_cd} / site: {site} 긍정 핵심문장 에러3\t{error}")

                            if len(keysentence_list_pos)==0:    # 최신순으로 출력
                                list5 = pos_df.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                                list5 = list5['REVIEW'].values.tolist()
                                
                                list_pos_review=[]
                                for index in range(5):
                                    try:
                                        list_pos_review.append(list5[index])
                                    except:
                                        list_pos_review.append('')

                                pos_keys_result_df=emo_pos_sent(analy_cd,site,list_pos_review)
                            
                            else:
                                pos_keys_fin=noValueToBlank(keysentence_list_pos)
                                pos_keys_result_df=emo_pos_sent(analy_cd,site,pos_keys_fin)

                    except Exception as e:
                        error_list.append(f"{analy_cd} / site: {site} 긍정 핵심문장 오류4\t{e}")
                        print(f"{analy_cd} 긍정 키센텐스 오류4\t{e}")
                        pos_keys_result_df=pos_sent_error(analy_cd,site)
                        pass 

                    data_anal02=pd.concat([data_anal02,pos_keyword_result_df,pos_keys_result_df],ignore_index=True)
                    del pos_keyword_result_df
                    del pos_keys_result_df
                
                else:
                    print(f'{analy_cd}_긍정리뷰 없음')
                    error_list.append(f'{analy_cd} site:{site} 긍정리뷰 없음')
                    none_review.append(f'{code}\tsite_gubun:{site_no}\temo_긍정_review_없음')


                # 부정 키워드
                print("부정키워드/핵심문장 분석 시작")
                neg_df=df_site[df_site['RLT_VALUE_03']<3]
                
                if 0< len(neg_df)<6:
                    list5 = neg_df.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                    list5 = list5['REVIEW'].values.tolist()
                    list_nega_review=[]
                    for index in range(5):
                        try:
                            list_nega_review.append(list5[index])
                        except:
                            list_nega_review.append('')
                        
                    neg_sentence=emo_neg_sent(analy_cd,site,list_nega_review)
                    
                    data_anal02=pd.concat([data_anal02,neg_sentence],ignore_index=True)

                elif len(neg_df)>5:
                    # 부정 리뷰 리스트
                    neg_review_list=neg_df['REVIEW'].tolist()

                    try:
                        neg_keyword,error=keyword_minCount(neg_review_list, stopword) #딕셔너리 {단어-점수}
                        if '오류없음' not in error:               # 오류 발생
                            neg_keyword_result_df=neg_key_error(analy_cd,site)
                            error_list.append(f"{analy_cd} / site: {site} 부정 키워드 오류5\t{error}")
                        else:
                            neg_keywordList=list(neg_keyword.keys())
                            neg_keywordGradeList=list(neg_keyword.values())
                            neg_keywordGradeList = (np.round(neg_keywordGradeList,2)).tolist() # 반올림

                            # List 빈칸처리
                            neg_key_list=noValueToBlank(neg_keywordList)
                            neg_grade_list=noValueToBlank(neg_keywordGradeList)
                            neg_result_list=neg_key_list+neg_grade_list
                            neg_keyword_result_df=neg_key_result(analy_cd,site,neg_result_list)
                            
                            print(f"code: {code} / site_no: {site_no} 부정 리뷰 키워드 완료\t부정 리뷰수:{len(neg_df)}")

                    except Exception as e:
                        error_list.append(f"{analy_cd} / site: {site} 부정키워드 오류6\t{e}")
                        print(f"{analy_cd} 부정키워드 오류6\n{e}")
                        neg_keyword_result_df=neg_key_error(analy_cd,site)
                        neg_keyword={}
                        pass
                
                    #부정리뷰 키센텐스
                    try:
                        print("부정리뷰 핵심문장 분석 시작")
                        if not neg_keyword:    # 부정키워드 없으면 리뷰 최신순으로 센텐스 출력
                            list5 = neg_df.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                            list5 = list5['REVIEW'].values.tolist()
                            list_nega_review=[]
                            for index in range(5):
                                try:
                                    list_nega_review.append(list5[index])
                                except:
                                    list_nega_review.append('')
                                
                            neg_keys_result_df=emo_neg_sent(analy_cd,site,list_nega_review)

                        else:
                            keysentence_list_neg,error=keys_list(neg_keyword,stopword,neg_review_list)
                            error_list.append(f"{analy_cd} / site: {site} 부정 핵심문장 오류7\t{error}")

                            if len(keysentence_list_neg)==0: # 핵심문장 분석결과가 없으면, 리뷰 최신순으로 출력
                                list5 = neg_df.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                                list5 = list5['REVIEW'].values.tolist()
                                list_nega_review=[]
                                for index in range(5):
                                    try:
                                        list_nega_review.append(list5[index])
                                    except:
                                        list_nega_review.append('')
                                    
                                neg_keys_result_df=emo_neg_sent(analy_cd,site,list_nega_review)
                            else:
                                neg_keys_fin=noValueToBlank(keysentence_list_neg)
                                neg_keys_result_df=emo_neg_sent(analy_cd,site,neg_keys_fin)

                    except Exception as e:
                        error_list.append(f"{analy_cd} / site: {site} 부정 핵심문장 오류8\t{e}")
                        print(f"{analy_cd} site: {site} 부정 핵심문장 오류8\t{e}")
                        neg_keys_result_df=neg_sent_error(analy_cd,site)
                        pass 

                    data_anal02=pd.concat([data_anal02,neg_keyword_result_df,neg_keys_result_df],ignore_index=True)
                    del neg_keyword_result_df
                    del neg_keys_result_df

                    print(f'{analy_cd} / site:{site} 부정리뷰 완료')
                
                else :
                    print('{}_부정리뷰부족'.format(analy_cd))
                    error_list.append(f'{analy_cd}_부정리뷰 없음')
                    none_review.append(f'{code}\tsite_gubun:{site_no}\temo_부정_review_없음')
    
    emo_total_end=time.time()
    emo_total_time=emo_total_end-emo_time_start
    now=datetime.now().strftime('%y%m%d_%H%M')
    print(f'emo_분석완료: {now}')
    # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
    time_list=[now, "emo",len(code_list),review_count,emo_total_time]

    # save
    db.time_txt(time_list,f'{today_path}/time_check')
    db.save_txt(none_review,f'{today_path}/noReviewProduct')
    db.save_txt(error_list,f'{today_path}/errorList')
    data_anal02.to_csv(f'{today_path}/{now}_anal02_result.csv', index=None)

    return data_anal02