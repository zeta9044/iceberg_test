"""
데이터 처리 유틸리티 함수를 제공하는 모듈.
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from iceberg_test.utils.schema import pandas_type_to_iceberg_type


def infer_schema_from_pandas(df: pd.DataFrame, table_name: str = "inferred_table") -> Schema:
    """
    Pandas DataFrame에서 Iceberg 스키마를 추론합니다.
    
    Args:
        df: 스키마를 추론할 Pandas DataFrame
        table_name: 테이블 이름 (기본값: "inferred_table")
        
    Returns:
        Schema: 추론된 Iceberg 스키마
    """
    fields = []
    field_id = 1
    
    for column_name, dtype in df.dtypes.items():
        iceberg_type = pandas_type_to_iceberg_type(str(dtype))
        
        # 필드 ID가 고유한지 확인 (Iceberg 요구사항)
        field = NestedField(field_id, column_name, iceberg_type, required=not df[column_name].isnull().any())
        fields.append(field)
        field_id += 1
    
    return Schema(*fields)


def sample_data_generator(num_rows: int = 100, seed: Optional[int] = None) -> pd.DataFrame:
    """
    테스트를 위한 샘플 데이터를 생성합니다.
    
    Args:
        num_rows: 생성할 행 수 (기본값: 100)
        seed: 난수 생성 시드 (기본값: None)
        
    Returns:
        pd.DataFrame: 샘플 데이터가 포함된 Pandas DataFrame
    """
    if seed is not None:
        np.random.seed(seed)
    
    # 샘플 데이터 생성
    data = {
        "id": range(1, num_rows + 1),
        "name": [f"Name-{i}" for i in range(1, num_rows + 1)],
        "value": np.random.randint(1, 1000, size=num_rows),
        "active": np.random.choice([True, False], size=num_rows),
        "timestamp": pd.date_range(start="2023-01-01", periods=num_rows),
    }
    
    return pd.DataFrame(data)


def create_sample_csv(file_path: Union[str, Path], num_rows: int = 100, seed: Optional[int] = None) -> Path:
    """
    테스트를 위한 샘플 CSV 파일을 생성합니다.
    
    Args:
        file_path: 저장할 CSV 파일 경로
        num_rows: 생성할 행 수 (기본값: 100)
        seed: 난수 생성 시드 (기본값: None)
        
    Returns:
        Path: 생성된 CSV 파일 경로
    """
    # 경로가 문자열이면 Path 객체로 변환
    if isinstance(file_path, str):
        file_path = Path(file_path)
    
    # 디렉토리가 없으면 생성
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 샘플 데이터 생성
    df = sample_data_generator(num_rows, seed)
    
    # CSV 파일로 저장
    df.to_csv(file_path, index=False)
    
    return file_path


def get_dataset_stats(df: pd.DataFrame) -> Dict:
    """
    데이터셋의 기본 통계를 계산합니다.
    
    Args:
        df: 통계를 계산할 Pandas DataFrame
        
    Returns:
        Dict: 데이터셋 통계 정보
    """
    # 기본 통계 계산
    stats = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "dtypes": {col: str(df[col].dtype) for col in df.columns},
        "missing_values": {col: int(df[col].isnull().sum()) for col in df.columns},
        "numeric_columns": list(df.select_dtypes(include=['number']).columns),
    }
    
    # 숫자형 열에 대한 통계
    numeric_stats = {}
    for col in stats["numeric_columns"]:
        numeric_stats[col] = {
            "min": float(df[col].min()),
            "max": float(df[col].max()),
            "mean": float(df[col].mean()),
            "median": float(df[col].median()),
            "std": float(df[col].std()),
        }
    
    stats["numeric_stats"] = numeric_stats
    
    return stats
