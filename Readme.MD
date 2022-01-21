실행파일 <br>
exe.py

<br>
실행환경: python 3.6

<br><br>
dockerfile / cron 수정 필요
<br>

생활용품 리뷰 분석 종류<br>
1. 리뷰의 속성분류(Kobert)
2. 리뷰의 감정분류(bert(acryl)) - 1점~5점
3. 키워드/핵심문장(KRWordRank)
    - 상품의 전체 리뷰에 대한 키워드/핵심문장
    - 상품의 긍정리뷰(감정분류 결과 4~5점 리뷰)에 대한 키워드/핵심문장
    - 상품의 부정리뷰(감정분류 결과 1~2점 리뷰)에 대한 키워드/핵심문장

분석 종류별 폴더 설명<br>
- classification
    - models.py          : 속성분류 모델 설정
    - predict.py         : 추론 코드
- keys
    - keysentence_lib.py : 핵심문장 분석에 필요한 코드
    - keyword_lib.py     : 키워드 분석에 필요한 코드
- classify_empathy.py    : 리뷰 1건에 대한 속성분류/감정분류결과 추론 결과
- keysentenc.py          : 전체 리뷰에 대한 키워드/핵심문장, 긍-부정 리뷰에 대한 키워드/핵심문장 출력
<br>
기타 파일 설명
- DB_conn
    - db.py              : db 연결 및 자료 저장
- cron                   : 매일 00:00 실행 설정 (수정 필요)
- etc                    : 백업자료 저장 및 기타 자료
- exe.py                 : 실행 파일

<br>
분석 실행 순서 (exe.py 참고) <br>
1. 분석결과 백업을 위한 폴더 생성
2. DB에서 데이터 select(db.py) > 속성분류/감정분석모델 결과 출력((classify_empathy.py) > DB 저장
3. 분류/감정분석을 실시한 제품리스트를 체크 후 해당 제품에 관한 전체 리뷰 select
4. krwordrank 알고리즘을 이용한 키워드/핵심문장 추출(keysentence.py)
5. 결과값 DB 저장
6. 분석성공시 백업자료(분석결과 및 분석시 걸리는 시간 관련 자료) 메일 전송
7. 에러 발생시 에러 관련 내용 메일 전송