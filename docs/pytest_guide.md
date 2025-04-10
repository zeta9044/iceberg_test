# pytest 사용 가이드

이 문서는 Apache Iceberg 테스트 프로젝트에서 pytest를 사용하는 방법을 설명합니다.

## 목차
1. [설치 및 기본 설정](#설치-및-기본-설정)
2. [테스트 실행 방법](#테스트-실행-방법)
3. [테스트 작성 방법](#테스트-작성-방법)
4. [Fixture 사용 방법](#fixture-사용-방법)
5. [Mocking과 Patching](#mocking과-patching)
6. [테스트 표시 및 분류](#테스트-표시-및-분류)
7. [코드 커버리지 측정](#코드-커버리지-측정)
8. [디버깅 팁](#디버깅-팁)

## 설치 및 기본 설정

이 프로젝트는 Poetry를 통해 pytest와 관련 의존성을 관리합니다. 설치 방법은 다음과 같습니다:

```bash
# Poetry로 설치
poetry install

# 또는 pip으로 직접 설치
pip install pytest pytest-cov
```

### pyproject.toml 설정

`pyproject.toml` 파일에 pytest 설정이 포함되어 있습니다:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = "test_*.py"
python_functions = "test_*"
python_classes = "Test*"
addopts = "--cov=iceberg_test --cov-report=xml --cov-report=term"
```

## 테스트 실행 방법

### 모든 테스트 실행

```bash
# Poetry 환경에서 실행
poetry run pytest

# 또는 가상환경이 활성화된 상태에서
pytest
```

### 특정 테스트 파일 실행

```bash
pytest tests/unit/utils/test_data.py
```

### 특정 테스트 함수 실행

```bash
pytest tests/unit/utils/test_data.py::test_sample_data_generator
```

### 키워드로 테스트 필터링

```bash
pytest -k "schema"  # "schema"가 포함된 모든
```

### 상세 출력 옵션

```bash
# 자세한 출력
pytest -v

# 매우 자세한 출력
pytest -vv

# 진행 상황 표시
pytest --verbose
```

### 실패한 테스트만 실행

```bash
# 마지막으로 실패한 테스트만 실행
pytest --lf

# 마지막으로 실패한 테스트 먼저 실행 후 나머지 테스트 실행
pytest --ff
```

## 테스트 작성 방법

테스트 파일은 `tests/` 디렉토리에 위치하며, 파일 이름은 `test_`로 시작해야 합니다. 테스트 함수도 `test_`로 시작해야 합니다.

### 기본 테스트 작성 예시

```python
# tests/unit/utils/test_example.py
def test_addition():
    """덧셈 함수 테스트 예시."""
    result = 1 + 1
    assert result == 2
```

### 예외 테스트 작성 예시

```python
import pytest
from iceberg_test.exceptions import TableNotFoundError

def test_exception_raised():
    """예외 발생 테스트 예시."""
    with pytest.raises(TableNotFoundError):
        # 예외가 발생할 것으로 예상되는 코드
        raise TableNotFoundError("테이블을 찾을 수 없음")
```

### 테스트 클래스 작성 예시

```python
class TestExample:
    """테스트 클래스 예시."""
    
    def test_method_one(self):
        """첫 번째 테스트 메서드."""
        assert 1 == 1
    
    def test_method_two(self):
        """두 번째 테스트 메서드."""
        assert "a" + "b" == "ab"
```

## Fixture 사용 방법

Fixture는 테스트에 필요한 데이터나 객체를 설정하는 데 사용됩니다. `conftest.py` 파일에 정의하면 여러 테스트 파일에서 사용할 수 있습니다.

### Fixture 정의 예시

```python
# tests/conftest.py
import pytest
import pandas as pd

@pytest.fixture
def sample_dataframe():
    """테스트용 샘플 DataFrame 생성."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })
```

### Fixture 사용 예시

```python
def test_dataframe_operations(sample_dataframe):
    """fixture를 사용하는 테스트 예시."""
    # sample_dataframe이 자동으로 주입됨
    assert len(sample_dataframe) == 3
    assert "id" in sample_dataframe.columns
```

### Fixture 범위 설정

```python
@pytest.fixture(scope="function")  # 기본값: 각 테스트 함수마다 실행
@pytest.fixture(scope="class")     # 클래스 내 모든 테스트에 공유
@pytest.fixture(scope="module")    # 모듈 내 모든 테스트에 공유
@pytest.fixture(scope="session")   # 테스트 세션 전체에 공유
```

### Fixture 정리 작업

```python
@pytest.fixture
def temp_file():
    """임시 파일을 생성하고 테스트 후 삭제하는 fixture."""
    # 설정
    file_path = "temp_test_file.txt"
    with open(file_path, "w") as f:
        f.write("test data")
    
    # fixture 값 반환
    yield file_path
    
    # 정리 작업
    import os
    os.remove(file_path)
```

## Mocking과 Patching

복잡한 의존성이 있는 함수를 테스트할 때 mock 객체를 사용하여 의존성을 대체할 수 있습니다.

### 기본 Mocking 예시

```python
from unittest.mock import patch, MagicMock

def test_catalog_connection():
    """Mocking 예시."""
    with patch('iceberg_test.core.catalog.load_catalog') as mock_load:
        # Mock 객체 설정
        mock_catalog = MagicMock()
        mock_catalog.name = "mock_catalog"
        mock_load.return_value = mock_catalog
        
        # 테스트 대상 함수 호출
        from iceberg_test.core.catalog import get_catalog
        result = get_catalog()
        
        # 검증
        assert result == mock_catalog
        mock_load.assert_called_once()
```

### Mock 객체 속성 및 메서드 설정

```python
# 반환 값 설정
mock_obj.return_value = "test_value"

# 예외 발생 설정
mock_obj.side_effect = Exception("Test error")

# 여러 호출에 대한 다른 반환 값 설정
mock_obj.side_effect = [1, 2, 3]

# 속성 설정
mock_obj.attribute = "test_attribute"

# 메서드 호출 검증
mock_obj.method.assert_called_once_with(arg1, arg2)
mock_obj.method.assert_called_with(arg1, arg2)
mock_obj.method.assert_not_called()
```

## 테스트 표시 및 분류

테스트에 표시를 추가하여 특정 카테고리의 테스트를 구성하고 실행할 수 있습니다.

### 테스트 표시 추가

```python
@pytest.mark.slow
def test_slow_operation():
    """오래 걸리는 테스트."""
    # 실행에 시간이 오래 걸리는 테스트 코드
    pass

@pytest.mark.integration
def test_integration_feature():
    """통합 테스트."""
    # 여러 구성 요소를 함께 테스트하는 코드
    pass
```

### 표시된 테스트 실행

```bash
# slow 표시가 있는 테스트만 실행
pytest -m slow

# slow 표시가 없는 테스트만 실행
pytest -m "not slow"

# slow 또는 integration 표시가 있는 테스트 실행
pytest -m "slow or integration"
```

## 코드 커버리지 측정

pytest-cov 플러그인을 사용하여 테스트 코드 커버리지를 측정할 수 있습니다.

### 기본 커버리지 측정

```bash
pytest --cov=iceberg_test
```

### 상세 커버리지 보고서 생성

```bash
# 터미널 출력
pytest --cov=iceberg_test --cov-report=term-missing

# HTML 보고서 생성
pytest --cov=iceberg_test --cov-report=html

# XML 보고서 생성 (CI/CD 시스템용)
pytest --cov=iceberg_test --cov-report=xml
```

## 디버깅 팁

### 디버깅 모드로 테스트 실행

```bash
# PDB 디버거 사용
pytest --pdb

# 첫 번째 실패 시 디버거 진입
pytest --pdb --exitfirst
```

### 테스트 출력 확인

```bash
# 테스트 중 출력 표시 (기본적으로 캡처됨)
pytest -s

# 또는
pytest --capture=no
```

### 디버깅용 코드 추가

```python
def test_complex_calculation():
    result = complex_function()
    # 디버깅 지점 추가
    import pdb; pdb.set_trace()
    assert result == expected_value
```

### 테스트 시간 측정

```bash
# 가장 느린 10개 테스트 표시
pytest --durations=10

# 모든 테스트 시간 표시
pytest --durations=0
```

이 가이드가 Apache Iceberg 테스트 프로젝트에서 pytest를 효과적으로 사용하는 데 도움이 되기를 바랍니다. 추가 질문이나 도움이 필요하면 팀에 문의하세요.
