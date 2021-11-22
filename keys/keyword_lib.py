import pandas as pd
from krwordrank.word import *
from krwordrank.hangle import normalize
from kss import split_sentences
from krwordrank.sentence import keysentence, summarize_with_sentences, make_vocab_score, MaxScoreTokenizer
import kss

# 전체리뷰, 리스트로 리턴
def all_get_texts(fname):
    # 불러오는 리뷰데이터 중 리뷰와 감정점수만 추출
    f = pd.read_csv(fname, encoding='utf-8')
    f = f[['review', 'emo_grade']].replace('\n', ' ', regex=True)

    f = f['review']
    texts = [doc.lower().replace('\n', ' ') for doc in f]
    
    return list(texts)

# 긍정리뷰 추출
def pos_get_texts(fname):
    
    # 불러오는 리뷰데이터 중 리뷰와 감정점수만 추출
    f = pd.read_csv(fname, encoding='utf-8', header=0)
    f = f[['review', 'emo_grade']].replace('\n', ' ', regex=True)
    # 긍정 -4,5
    pos_score = f.emo_grade > 3
    f = f[pos_score]
    f = f['review']
    texts = [doc.lower().replace('\n', ' ') for doc in f]

    return list(texts)

# 부정리뷰
def neg_get_texts(fname):
    
    # 불러오는 리뷰데이터 중 리뷰와 감정점수만 추출
    f = pd.read_csv(fname, encoding='utf-8', header=0)
    f = f[['review', 'emo_grade']].replace('\n', ' ', regex=True)

    #부정 -1,2
    neg_score = f.emo_grade < 3
    f=f[neg_score]
    f=f['review']
    texts=[doc.lower().replace('\n',' ') for doc in f]

    return list(texts)


#2-3 키워드 top 300 단어 확인
def keywords_common_top300(keywords):
    key_num = 300
    keyword_list = []
    for word, r in sorted(keywords.items(), key=lambda x: -x[1])[:key_num]:    
    #    print('%s:\t%.4f' % (word, r))
        keyword_list.append((word, r))
    return keyword_list

# stopwords 만들기
def stopwords(fname):
    f=open(fname,'r',encoding='UTF8')
    stopwords=[line.rstrip('\n') for line in f]
    f.close()
    return stopwords

# 키워드 top 300에서 stopwords 제거한 단어 리스트
def remove_stopwords_keywords(keyword_list, stopwords):
    #selected_top_keywords = []
    selected_keywords = []
    # 단어와 값을 딕셔너리로 담아야함
    #only_keyword={}
    for word, r in keyword_list:
        if word in stopwords:
            continue
        selected_keywords.append([word, r])
        #only_keyword.setdefault(word,r) 
    #딕셔너리로 변환
    selected_keywords_dic=dict(selected_keywords)
    #selected_top_keywords.append(selected_keywords)
    #selected_top_keywords = sum(selected_top_keywords, [])  # 이중리스트 제거
    return selected_keywords_dic #리턴값은 딕셔너리

# 앞에서 def get_texts로 파일불러와서 매개변수로 함수 넣기!
#리뷰 키워드 추출
def keyword(texts, stopwords):
    
    #all_texts=all_get_texts(fname)
    df=pd.DataFrame(texts)

    # 단어랭크 확인
    wordrank_extractor = KRWordRank(  # 단어랭크 추출
    min_count=5,   # 단어의 최소 출현 빈도수 (그래프 생성 시)
    max_length=10,  # 단어의 최대 길이
    verbose=True
    )
    beta = 0.85    
    max_iter = 10
    keywords, rank, graph = wordrank_extractor.extract(texts, beta, max_iter)  # 단어랭크-빈도수를 기반으로 키워드를 추출

    # 키워드 300단어 확인
    keyword_list=keywords_common_top300(keywords) # 리스토로 리턴

    #키워드 300개에서 stopwords 제거
    keywords_sw_apply= remove_stopwords_keywords(keyword_list, stopwords) # 딕셔너리
    # keywords_sw_apply=pd.DataFrame(keywords_sw_apply)
    return keywords_sw_apply # 딕셔너리로 리턴됨


# 주제별

skincare=['발림성', '보습력', '수분감', '끈적임', '향', '효과']
cleanser=['거품력', '보습력','세정력','향','효과']
hairBody=['발림성','보습력','수분감','탈모','세정력','향','효과']
suncare=['발림성','보섭력','수분감','끈적임','향','효과']
baseMakeUp=['발림성','모습력','수분감','커버력','지속력','향','효과']
pointMakeUp=['발림성','보습력','수분감','지속력','발색력','효과']
living=['제품상태','사이즈','가격','사용성','재질','디자인']



# 1. 화장품

def properties(CMK_catID):
    #CMK cat_id
    skincare_list=[51356,51357,51358,51359,51360,51361,51383,51384,51400]
    cleanser_list=[51380,51381,51382,51403]
    hairBody_list=[51390,51391,51392,51393,51394,51395,51396,51397,51398,51399,51404,51405]
    suncare_list=[51385,51386,51387,51388,51389,51402]
    baseMakeUp_list=[51362,51363,51364,51366,51370,51401]
    pointMakeUp_list=[51365,51367,51368,51369,51371,51372,51373,51374,51375,51376,51377,51378,51379]

    if CMK_catID in skincare_list: #  1
        review_classify=skincare
    elif CMK_catID in cleanser_list: #2
        review_classify=cleanser
    elif CMK_catID in hairBody_list: #3
        review_classify=hairBody
    elif CMK_catID in suncare_list: #4
        review_classify=suncare
    elif CMK_catID in baseMakeUp_list: #5
        review_classify=baseMakeUp
    elif CMK_catID in pointMakeUp_list: #6
        review_classify=pointMakeUp
    else:
        review_classify="None"
    print(review_classify)
    
    return review_classify #주제별 속성 리스트로 출력

def living_properties():
    return living



## dictionary를 txt 파일로 저장하기
def save_txt(dic, save_point):
    with open(save_point,'w',encoding='utf-8') as f:
        for word,r in dic.items():
            f.write(f'{word}\t{r}\n')

