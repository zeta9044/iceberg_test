"""
데이터 검증 유틸리티 함수를 제공하는 모듈.
"""
import re
from typing import List, Optional, Union

import pandas as pd
from pyiceberg.schema import Schema


def validate_table_name(table_name: str) -> bool:
    """
    테이블 이름이 유효한지 검사합니다.
    
    Args:
        table_name: 검사할 테이블 이름
        
    Returns:
        bool: 테이블 이름이 유효하면 True, 그렇지 않으면 False
    """
    # 테이블 이름 규칙: 문자, 숫자, 언더스코어만 허용하고, 문자로 시작해야 함
    pattern = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*)
    return bool(pattern.match(table_name))


def validate_namespace(namespace: str) -> bool:
    """
    네임스페이스가 유효한지 검사합니다.
    
    Args:
        namespace: 검사할 네임스페이스
        
    Returns:
        bool: 네임스페이스가 유효하면 True, 그렇지 않으면 False
    """
    # 네임스페이스 규칙: 문자, 숫자, 언더스코어, 점만 허용
    pattern = re.compile(r'^[a-zA-Z][a-zA-Z0-9_.]*)
    return bool(pattern.match(namespace))


def validate_data_against_schema(df: pd.DataFrame, schema: Schema) -> List[str]:
    """
    데이터가 스키마에 맞는지 검사합니다.
    
    Args:
        df: 검사할 Pandas DataFrame
        schema: 검사할 Iceberg 스키마
        
    Returns:
        List[str]: 오류 메시지 목록 (오류가 없으면 빈 목록)
    """
    errors = []
    
    # 스키마의 모든 필드 확인
    for field in schema.fields:
        field_name = field.name
        
        # 필드가 데이터에 존재하는지 확인
        if field_name not in df.columns:
            if field.required:
                errors.append(f"필수 필드 '{field_name}'이(가) 데이터에 없습니다.")
            continue
        
        # 필드 타입 확인
        # (이상적으로는 타입 호환성을 더 자세히 확인해야 하지만, 여기서는 기본적인 확인만 수행)
        pandas_type = df[field_name].dtype
        is_compatible = True
        
        # 타입 호환성 간단히 확인
        field_type_str = str(field.field_type).lower()
        
        # 정수형 확인
        if "integer" in field_type_str or "long" in field_type_str:
            if not pd.api.types.is_integer_dtype(pandas_type):
                is_compatible = False
        
        # 실수형 확인
        elif "float" in field_type_str or "double" in field_type_str:
            if not pd.api.types.is_float_dtype(pandas_type) and not pd.api.types.is_integer_dtype(pandas_type):
                is_compatible = False
        
        # 불리언 확인
        elif "boolean" in field_type_str:
            if not pd.api.types.is_bool_dtype(pandas_type):
                is_compatible = False
        
        # 타임스탬프 확인
        elif "timestamp" in field_type_str:
            if not pd.api.types.is_datetime64_dtype(pandas_type):
                is_compatible = False
        
        # 타입이 호환되지 않으면 오류 추가
        if not is_compatible:
            errors.append(
                f"필드 '{field_name}'의 타입 '{pandas_type}'이(가) 스키마의 타입 '{field.field_type}'과(와) 호환되지 않습니다."
            )
        
        # 필수 필드에 누락된 값이 있는지 확인
        if field.required and df[field_name].isnull().any():
            errors.append(f"필수 필드 '{field_name}'에 누락된 값이 있습니다.")
    
    return errors


def validate_column_names(column_names: List[str]) -> List[str]:
    """
    열 이름이 Iceberg에서 유효한지 검사합니다.
    
    Args:
        column_names: 검사할 열 이름 목록
        
    Returns:
        List[str]: 유효하지 않은 열 이름 목록
    """
    invalid_columns = []
    
    # 열 이름 규칙: 문자, 숫자, 언더스코어만 허용하고, 문자로 시작해야 함
    pattern = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*)
    
    for col in column_names:
        if not pattern.match(col):
            invalid_columns.append(col)
    
    return invalid_columns
