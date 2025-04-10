"""
스키마 관련 유틸리티 함수를 제공하는 모듈.
"""
from typing import Dict, Type, Union

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


def pandas_type_to_iceberg_type(pandas_type: str) -> Type:
    """
    Pandas 데이터 타입을 Iceberg 데이터 타입으로 변환합니다.
    
    Args:
        pandas_type: Pandas 데이터 타입 문자열
        
    Returns:
        Type: 해당하는 Iceberg 타입 객체
    """
    type_mapping = {
        "int64": IntegerType,
        "int32": IntegerType,
        "int": IntegerType,
        "int16": IntegerType,
        "int8": IntegerType,
        "float64": DoubleType,
        "float32": FloatType,
        "float": DoubleType,
        "bool": BooleanType,
        "datetime64[ns]": TimestampType,
        "datetime64": TimestampType,
        "date": DateType,
        "object": StringType,
        "string": StringType,
    }
    
    # pandas_type이 문자열 형태로 제공될 경우 (예: 'int64')
    if isinstance(pandas_type, str):
        for key, value in type_mapping.items():
            if key in pandas_type.lower():
                return value()
    
    # 기본 타입은 문자열로 가정
    return StringType()


def iceberg_type_to_pandas_type(iceberg_type: Union[Type, str]) -> str:
    """
    Iceberg 데이터 타입을 Pandas 데이터 타입으로 변환합니다.
    
    Args:
        iceberg_type: Iceberg 데이터 타입 객체 또는 문자열
        
    Returns:
        str: 해당하는 Pandas 타입 문자열
    """
    # 문자열로 변환
    if not isinstance(iceberg_type, str):
        iceberg_type_str = str(iceberg_type).lower()
    else:
        iceberg_type_str = iceberg_type.lower()
    
    type_mapping = {
        "integer": "int64",
        "long": "int64",
        "float": "float32",
        "double": "float64",
        "boolean": "bool",
        "string": "object",
        "timestamp": "datetime64[ns]",
        "date": "datetime64[ns]",
    }
    
    for key, value in type_mapping.items():
        if key in iceberg_type_str:
            return value
    
    # 기본 타입은 object (문자열)
    return "object"


def get_schema_diff(schema1: Dict, schema2: Dict) -> Dict:
    """
    두 스키마 간의 차이점을 찾습니다.
    
    Args:
        schema1: 첫 번째 스키마 (딕셔너리 형태)
        schema2: 두 번째 스키마 (딕셔너리 형태)
        
    Returns:
        Dict: 스키마 차이점
    """
    diff = {
        "added_fields": [],
        "removed_fields": [],
        "modified_fields": [],
    }
    
    # 첫 번째 스키마의 필드 이름 집합
    schema1_fields = {field["name"]: field for field in schema1["fields"]}
    
    # 두 번째 스키마의 필드 이름 집합
    schema2_fields = {field["name"]: field for field in schema2["fields"]}
    
    # 추가된 필드
    for name in schema2_fields:
        if name not in schema1_fields:
            diff["added_fields"].append(schema2_fields[name])
    
    # 제거된 필드
    for name in schema1_fields:
        if name not in schema2_fields:
            diff["removed_fields"].append(schema1_fields[name])
    
    # 변경된 필드
    for name in schema1_fields:
        if name in schema2_fields:
            field1 = schema1_fields[name]
            field2 = schema2_fields[name]
            
            # 타입이나 필수 여부가 변경된 경우
            if field1["type"] != field2["type"] or field1["required"] != field2["required"]:
                diff["modified_fields"].append({
                    "field": name,
                    "from": field1,
                    "to": field2,
                })
    
    return diff
