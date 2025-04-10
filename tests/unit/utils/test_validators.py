"""
검증 유틸리티 함수 테스트.
"""
import pytest
import pandas as pd
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    NestedField,
    StringType,
)

from iceberg_test.utils.validators import (
    validate_table_name,
    validate_namespace,
    validate_data_against_schema,
    validate_column_names,
)


def test_validate_table_name():
    """validate_table_name 함수 테스트."""
    # 유효한 테이블 이름
    assert validate_table_name("my_table") == True
    assert validate_table_name("table123") == True
    assert validate_table_name("a") == True
    assert validate_table_name("a_very_long_table_name_with_123") == True
    
    # 유효하지 않은 테이블 이름
    assert validate_table_name("123table") == False  # 숫자로 시작
    assert validate_table_name("my-table") == False  # 하이픈 포함
    assert validate_table_name("my.table") == False  # 점 포함
    assert validate_table_name("my table") == False  # 공백 포함
    assert validate_table_name("") == False  # 빈 문자열
    assert validate_table_name("_table") == False  # 언더스코어로 시작


def test_validate_namespace():
    """validate_namespace 함수 테스트."""
    # 유효한 네임스페이스
    assert validate_namespace("my_namespace") == True
    assert validate_namespace("namespace123") == True
    assert validate_namespace("a") == True
    assert validate_namespace("my.nested.namespace") == True
    
    # 유효하지 않은 네임스페이스
    assert validate_namespace("123namespace") == False  # 숫자로 시작
    assert validate_namespace("my-namespace") == False  # 하이픈 포함
    assert validate_namespace("my namespace") == False  # 공백 포함
    assert validate_namespace("") == False  # 빈 문자열
    assert validate_namespace("_namespace") == False  # 언더스코어로 시작


def test_validate_data_against_schema():
    """validate_data_against_schema 함수 테스트."""
    # 테스트 스키마 생성
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "active", BooleanType(), required=False),
    )
    
    # 유효한 데이터
    valid_data = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "active": [True, False, True],
    })
    
    errors = validate_data_against_schema(valid_data, schema)
    assert len(errors) == 0
    
    # 필수 열 누락
    missing_required_col = pd.DataFrame({
        "id": [1, 2, 3],
        "active": [True, False, True],
    })
    
    errors = validate_data_against_schema(missing_required_col, schema)
    assert len(errors) == 1
    assert "name" in errors[0]
    
    # 필수 필드에 누락된 값
    null_in_required = pd.DataFrame({
        "id": [1, None, 3],  # id는 필수지만 NULL 값이 있음
        "name": ["Alice", "Bob", "Charlie"],
        "active": [True, False, True],
    })
    
    errors = validate_data_against_schema(null_in_required, schema)
    assert len(errors) == 1
    assert "id" in errors[0]
    
    # 타입 불일치
    wrong_type = pd.DataFrame({
        "id": ["1", "2", "3"],  # 문자열 (정수형이어야 함)
        "name": ["Alice", "Bob", "Charlie"],
        "active": [True, False, True],
    })
    
    errors = validate_data_against_schema(wrong_type, schema)
    assert len(errors) == 1
    assert "id" in errors[0]
    assert "타입" in errors[0]


def test_validate_column_names():
    """validate_column_names 함수 테스트."""
    # 유효한 열 이름
    valid_columns = ["id", "user_name", "active123", "created_at"]
    invalid_columns = validate_column_names(valid_columns)
    assert len(invalid_columns) == 0
    
    # 유효하지 않은 열 이름이 포함된 경우
    mixed_columns = ["id", "user-name", "123active", "created at"]
    invalid_columns = validate_column_names(mixed_columns)
    assert len(invalid_columns) == 3
    assert "user-name" in invalid_columns
    assert "123active" in invalid_columns
    assert "created at" in invalid_columns
