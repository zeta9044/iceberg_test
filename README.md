# Apache Iceberg 테스트 프로젝트

이 프로젝트는 Apache Iceberg를 Python에서 사용하는 방법을 시연하고 테스트하기 위한 포괄적인 도구를 제공합니다. 대규모 프로젝트 구조를 채택하여 확장성과 유지보수성을 높였습니다.

## 프로젝트 개요

[Apache Iceberg](https://iceberg.apache.org/)는 대규모 분석 데이터셋을 위한 오픈소스 테이블 형식으로, 다음과 같은 특징이 있습니다:

- 스키마 진화
- 파티션 진화
- 타임 트래블 (Time Travel)
- 롤백 및 버전 관리
- ACID 트랜잭션

이 프로젝트는 Python에서 Iceberg를 사용하는 데 필요한 도구를 제공하며, 다음 기능을 포함합니다:

- Iceberg 테이블 생성 및 관리
- 데이터 읽기 및 쓰기
- 스키마 진화 관리
- 트랜잭션 처리
- CLI 도구

## 설치

### 요구 사항

- Python 3.9 이상
- [Poetry](https://python-poetry.org/) 패키지 관리자

### 설치 단계

```bash
# 저장소 클론
git clone https://github.com/yourusername/iceberg-test.git
cd iceberg-test

# Poetry로 의존성 설치
poetry install

# 가상 환경 활성화
poetry shell
```

## 사용 방법

### CLI 도구 사용

이 프로젝트는 편리한 명령줄 인터페이스를 제공합니다:

```bash
# 테이블 생성
iceberg-test create-table my_table --namespace my_namespace

# 테이블 목록 조회
iceberg-test list-tables my_namespace

# 테이블에 데이터 쓰기
iceberg-test write-table my_table data.csv --namespace my_namespace

# 테이블에서 데이터 읽기
iceberg-test read-table my_table --namespace my_namespace
```

### 프로그래밍 방식으로 사용

```python
from iceberg_test.core import create_table, write_to_table, read_from_table
from iceberg_test.models import IcebergConfig
import pandas as pd

# 설정 생성
config = IcebergConfig(
    catalog_name="my_catalog",
    warehouse_path="file:///path/to/warehouse"
)

# 테이블 생성
create_table("my_table", namespace="my_namespace", config=config)

# 데이터 쓰기
df = pd.read_csv("data.csv")
write_to_table("my_table", df, namespace="my_namespace", config=config)

# 데이터 읽기
result_df = read_from_table("my_table", namespace="my_namespace", config=config)
print(result_df.head())
```

## 예제

다양한 예제가 `examples/` 디렉토리에 포함되어 있습니다:

- `simple_table.py`: 기본적인 테이블 생성 및 사용 방법
- `schema_evolution.py`: 스키마 진화 예제 (열 추가, 이름 변경 등)

예제를 실행하는 방법:

```bash
# 가상 환경 활성화
poetry shell

# 간단한 테이블 예제 실행
python -m examples.simple_table

# 스키마 진화 예제 실행
python -m examples.schema_evolution
```

## 프로젝트 구조

```
iceberg-test/
├── pyproject.toml            # Poetry 구성 파일
├── README.md                 # 프로젝트 설명
├── src/                      # 소스 코드
│   └── iceberg_test/         # 메인 패키지
│       ├── __init__.py
│       ├── __main__.py       # CLI 엔트리포인트
│       ├── config/           # 설정 관련 모듈
│       ├── core/             # 핵심 기능
│       ├── models/           # 데이터 모델
│       ├── cli/              # CLI 명령어
│       ├── utils/            # 유틸리티
│       └── exceptions.py     # 커스텀 예외
├── tests/                    # 테스트
│   ├── unit/                 # 단위 테스트
│   └── integration/          # 통합 테스트
├── examples/                 # 사용 예제
├── docs/                     # 문서
└── data/                     # 샘플 데이터
```

## 기여 방법

기여를 환영합니다! 다음 단계를 따라 기여할 수 있습니다:

1. 이 저장소를 포크합니다.
2. 새 기능 브랜치를 생성합니다: `git checkout -b feature/amazing-feature`
3. 변경 사항을 커밋합니다: `git commit -m 'Add amazing feature'`
4. 브랜치를 푸시합니다: `git push origin feature/amazing-feature`
5. Pull Request를 제출합니다.

## 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다. 자세한 내용은 `LICENSE` 파일을 참조하세요.

## 주의 사항

- 이 프로젝트는 실험적이며 프로덕션 환경에서 사용하기 전에 충분한 테스트가 필요합니다.
- 현재 로컬 파일 시스템 위주로 구현되어 있으며, 클라우드 스토리지를 사용하려면 추가 설정이 필요합니다.
- Apache Iceberg는 활발히 개발 중인 프로젝트이므로, 최신 버전의 pyiceberg와 호환성을 확인하세요.
