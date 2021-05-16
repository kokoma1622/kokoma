v1의 문제를 도저히 DB를 유지하면서 해결할 수는 없을 것 같아 DB 초기화 후 새로운 입력 시작

주요 수정 포인트:
1. 테이블의 column과 row 수를 제한하기 위해 테이블을 나눔.

1-1) match_id, personal summary/rune/detail, team summary, event kill/ward/item/skillup/building/monster

2. main의 코드가 지나치게 길어지니 파일을 분리.

2-1) get .py 파일에 DB에 저장할 pandas dataframe을 만드는 함수를 작성하고 main에서 import


문제 :
1. v1에도 있던 문제인데, 게임의 평균 티어를 계산하는 과정에서 API 요청을 지나치게 많이 사용

1-1) 게다가 받아온 티어가 게임이 있던 시점의 티어가 아니라 현재 티어이기 때문에 부정확.

2. 데이터를 많이 저장하다보니 30GB 프리티어 사용량을 초과하려하여 DB 용량이 부족해짐
