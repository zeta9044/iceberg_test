"""
스키마 유틸리티 함수 테스트.
"""
import pytest

from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from iceberg_test.utils.schema import (
    pandas_type_to_iceberg_type,
    iceberg_type_to_pandas_type,
    get_schema_diff,
)


def test_pandas_type_to_iceberg_type():
    """pandas_type_to_iceberg_type 함수 테스트."""
    # 정수형 테스트
    assert isinstance(pandas_type_to_iceberg_type("int64"), IntegerType)
    assert isinstance(pandas_type_to_iceberg_type("int32"), IntegerType)
    assert isinstance(pandas_type_to_iceberg_type("int"), IntegerType)
    
    # 실수형 테스트
    assert isinstance(pandas_type_to_iceberg_type("float64"), DoubleType)
    assert isinstance(pandas_type_to_iceberg_type("float32"), FloatType)
    assert isinstance(pandas_type_to_iceberg_type("float"), DoubleType)
    
    # 불리언 테스트
    assert isinstance(pandas_type_to_iceberg_type("bool"), BooleanType)
    
    # 날짜/시간 테스트
    assert isinstance(pandas_type_to_iceberg_type("datetime64[ns]"), TimestampType)
    assert isinstance(pandas_type_to_iceberg_type("datetime64"), TimestampType)
    assert isinstance(pandas_type_to_iceberg_type("date"), DateType)
    
    # 문자열 테스트
    assert isinstance(pandas_type_to_iceberg_type("object"), StringType)
    assert isinstance(pandas_type_to_iceberg_type("string"), StringType)
    
    # 알 수 없는 타입 테스트 (기본값은 문자열)
    assert isinstance(pandas_type_to_iceberg_type("unknown_type"), StringType)


def test_iceberg_type_to_pandas_type():
    """iceberg_type_to_pandas_type 함수 테스트."""
    # 정수형 테스트
    assert iceberg_type_to_pandas_type(IntegerType()) == "int64"
    assert iceberg_type_to_pandas_type(LongType()) == "int64"
    assert iceberg_type_to_pandas_type("integer") == "int64"
    assert iceberg_type_to_pandas_type("long") == "int64"
    
    # 실수형 테스트
    assert iceberg_type_to_pandas_type(FloatType()) == "float32"
    assert iceberg_type_to_pandas_type(DoubleType()) == "float64"
    assert iceberg_type_to_pandas_type("float") == "float32"
    assert iceberg_type_to_pandas_type("double") == "float64"
    
    # 불리언 테스트
    assert iceberg_type_to_pandas_type(BooleanType()) == "bool"
    assert iceberg_type_to_pandas_type("boolean") == "bool"
    
    # 날짜/시간 테스트
    assert iceberg_type_to_pandas_type(TimestampType()) == "datetime64[ns]"
    assert iceberg_type_to_pandas_type(DateType()) == "datetime64[ns]"
    assert iceberg_type_to_pandas_type("timestamp") == "datetime64[ns]"
    assert iceberg_type_to_pandas_type("date") == "datetime64[ns]"
    
    # 문자열 테스트
    assert iceberg_type_to_pandas_type(StringType()) == "object"
    assert iceberg_type_to_pandas_type("string") == "object"
    
    # 알 수 없는 타입 테스트 (기본값은 object)
    assert iceberg_type_to_pandas_type("unknown_type") == "object"


def test_get_schema_diff():
    """get_schema_diff 함수 테스트."""
    # 테스트용 스키마 정의
    schema1 = {
        "fields": [
            {"name": "id", "type": "integer", "required": True},
            {"name": "name", "type": "string", "required": True},
            {"name": "email", "type": "string", "required": False},
        ]
    }
    
    schema2 = {
        "fields": [
            {"name": "id", "type": "integer", "required": True},
            {"name": "name", "type": "string", "required": False},  # 변경: required가 False로 변경
            {"name": "contact_email", "type": "string", "required": False},  # 추가: email -> contact_email
            {"name": "age", "type": "integer", "required": False},  # 추가: 새 필드
        ]
    }
    
    # 스키마 차이 계산
    diff = get_schema_diff(schema1, schema2)
    
    # 추가된 필드 확인
    assert len(diff["added_fields"]) == 1
    assert diff["added_fields"][0]["name"] == "age"
    
    # 제거된 필드 확인
    assert len(diff["removed_fields"]) == 1
    assert diff["removed_fields"][0]["name"] == "email"
    
    # 변경된 필드 확인
    assert len(diff["modified_fields"]) == 1
    assert diff["modified_fields"][0]["field"] == "name"
    assert diff["modified_fields"][0]["from"]["required"] == True
    assert diff["modified_fields"][0]["to"]["required"] == False
