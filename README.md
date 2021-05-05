# stocklab_win
트레이딩 시스템

- Xing API 설치
```
고객센터 -> API  - 최신 버젼 설치 
```


- 개발 환경 구성 

```
# 32bit 환경 구성 
set CONDA_FORCE_32BIT=1
conda create -n toone32 python=3.7 anaconda

# 스케쥴러는 추후 변경
pip install apscheduler
pip install pywin32
```



- 32 bit 환경을 사용함으로 인해 설치 오류 현상 들이 발생
- pip 오류 발생으로 conda 설치 진행
https://pypi.org/project/fastparquet/
```
# pip 설치 시 오류 
conda install -c conda-forge fastparquet

# 필요할 경우만 설치 (기본 설치 되어 있음)
conda install pandas
```

- fastparquet 도 기본 설치 되어 있고 아래 처럼 pandas와 함께 사용
```
import pandas as pd
df.to_parquet('sample.parquet', compression='gzip')

```


## 진행 시 이슈 사항 
- fastparquet 기본은 snappy는 지원 하지 않음 
- permssion 오류 발생 

```
# C 드라이브와 관련된 부분인지 확인 필요 
PermissionError: [Errno 13] Permission denied: 'c:\\data'
```


## Xing ACE 사용 방법





# pyspark 구성 
### lakehouse 구축을 위해 필요 

- 전체 spark의 기능은 필요 없고 delta lake와 parquet 파티션 저장등의 용도로 사용

```
pip instll pyspark
```

### 환경 변수 설정 
- PATH 지정 
  - C:\ProgramData\Anaconda3\envs\test32 
- SPARK_HOME 지정
  - c:\programdata\anaconda3\envs\test32\lib\site-packages\pyspark
