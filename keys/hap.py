import csv
import pandas as pd
import numpy as np
import os

path = './result/긍부정키워드/'
file_list = os.listdir(path)
file_list_py = [file for file in file_list if file.endswith('.csv')] ## 파일명 끝이 .csv인 경우

d1 = pd.DataFrame()
for i in file_list_py:
    data = pd.read_csv(path+i)
    d1 = pd.concat([d1,data])
d1 = d1.reset_index(drop=True)


path = './result/긍부정핵심문장/'
file_list = os.listdir(path)
file_list_py = [file for file in file_list if file.endswith('.csv')] ## 파일명 끝이 .csv인 경우

d2 = pd.DataFrame()
for i in file_list_py:
    data = pd.read_csv(path+i)
    d2 = pd.concat([d2,data])   
d2 = d2.reset_index(drop=True)

df = pd.concat([d1,d2])
df = df.reset_index(drop=True)

df.to_csv('./final_result/coupang_tasa_emo_result.csv',index=False)

path = './result/전체키워드/'
file_list = os.listdir(path)
file_list_py = [file for file in file_list if file.endswith('.csv')] ## 파일명 끝이 .csv인 경우

d3 = pd.DataFrame()
for i in file_list_py:
    data = pd.read_csv(path+i)
    d3 = pd.concat([d3,data])
d3 = d3.reset_index(drop=True)


path = './result/전체핵심문장/'
file_list = os.listdir(path)
file_list_py = [file for file in file_list if file.endswith('.csv')] ## 파일명 끝이 .csv인 경우

d4 = pd.DataFrame()
for i in file_list_py:
    data = pd.read_csv(path+i)
    d4 = pd.concat([d4,data])
d4 = d4.reset_index(drop=True)

df_total = pd.concat([d3,d4])
df_total = df_total.reset_index(drop=True)

df_total.to_csv('./final_result/coupang_tasa_total_result.csv',index=False)



path='./key_data/'
file_list=os.listdir(path)



column_name = ['ANALY_CODE','SITE_GUBUN','제품상태','사이즈','가격','사용성','재질','디자인',
               '1','2','3','4','5',
               '제품상태_긍','제품상태_부','제품상태_중','사이즈_긍','사이즈_부','사이즈_중',
              '가격_긍','가격_부','가격_중','사용성_긍','사용성_부','사용성_중',
              '재질_긍','재질_부','재질_중','디자인_긍','디자인_부','디자인_중','Total_REVIEW']

total_list=[]

coupang = 1
naver = 0

for i in file_list:
    fname=path+i
    f=pd.read_csv(fname, encoding='utf-8')
    
    analy_code =f.iloc[0,0]
    site = naver
    # 변수 생성
    for i in column_name:
        globals()['count_{}'.format(i)]=0
    
    # counting
    for i in range(len(f)):
        
        if f['subject'][i]== '가격':
            count_가격+=1
        elif f['subject'][i]== '디자인':
            count_디자인+=1
        elif f['subject'][i]== '사용성':
            count_사용성+=1
        elif f['subject'][i]== '재질':
            count_재질+=1
        elif f['subject'][i]== '제품상태':
            count_제품상태+=1
        elif f['subject'][i]== '사이즈':
            count_사이즈+=1
            
    
    for i in range(len(f)):
        
        if f['subject'][i]==1:
            count_1 +=1
        elif f['subject'][i]==2:
            count_2 +=1
        elif f['subject'][i]==3:
            count_3 +=1
        elif f['subject'][i]==4:
            count_4 +=1
        elif f['subject'][i]==5:
            count_5 +=1
            
    for i in range(len(f)):
        
        if f['subject'][i]== '가격':               
            if f['empathy_score'][i]> 3 : 
                count_가격_긍+=1
            elif f['empathy_score'][i]< 3 :
                count_가격_부+=1
            elif f['empathy_score'][i]== 3 :
                 count_가격_중+=1
                
        elif f['subject'][i]== '디자인':
            if f['empathy_score'][i]> 3 :
                count_디자인_긍+=1
            elif f['empathy_score'][i]< 3 :  
                count_디자인_부+=1
            elif f['empathy_score'][i]== 3 :
                count_디자인_중+=1
                
        elif f['subject'][i]== '사용성':
            if f['empathy_score'][i]> 3 :
                count_사용성_긍+=1
            elif f['empathy_score'][i]< 3 :  
                count_사용성_부+=1
            elif f['empathy_score'][i]== 3 :
                count_사용성_중+=1
       
        elif f['subject'][i]== '재질':
            if f['empathy_score'][i]> 3 :
                count_재질_긍+=1
            elif f['empathy_score'][i]< 3 :  
                count_재질_부+=1
            elif f['empathy_score'][i]== 3 :
                count_재질_중+=1
                
        elif f['subject'][i]== '제품상태':
            if f['empathy_score'][i]> 3 :
                count_제품상태_긍+=1
            elif f['empathy_score'][i]< 3 :  
                count_제품상태_부+=1
            elif f['empathy_score'][i]== 3 :
                count_제품상태_중+=1         
                

        elif f['subject'][i]== '사이즈':
            if f['empathy_score'][i]> 3 :
                count_사이즈_긍+=1
            elif f['empathy_score'][i]< 3 :  
                count_사이즈_부+=1
            elif f['empathy_score'][i]== 3 :
                count_사이즈_중+=1                                
        
        

    total_list.append( [analy_code,site,count_제품상태,
                     count_사이즈, count_가격, count_사용성, count_재질, count_디자인,
                     count_1,count_2,count_3,count_4,count_5,
                     count_제품상태_긍,count_제품상태_부,count_제품상태_중,count_사이즈_긍,count_사이즈_부,count_사이즈_중,
                     count_가격_긍,count_가격_부,count_가격_중,count_사용성_긍,count_사용성_부,count_사용성_중,
                     count_재질_긍,count_재질_부,count_재질_중,count_디자인_긍,count_디자인_부,count_디자인_중,len(f)])
    
    new_dataframe= pd.DataFrame(total_list, columns=column_name)
new_dataframe.to_csv('./final_result/naver_tasa_totalcount.csv',encoding = 'utf-8-sig',index=False)    