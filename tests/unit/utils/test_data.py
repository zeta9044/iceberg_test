"""
데이터 유틸리티 함수 테스트.
"""
import os
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

from iceberg_test.utils.data import (
    sample_data_generator,
    create_sample_csv,
    infer_schema_from_pandas,
    get_dataset_stats,
)


def test_sample_data_generator():
    """sample_data_generator 함수 테스트."""
    # 기본 행 수 테스트
    df = sample_data_generator()
    assert len(df) == 100
    
    # 사용자 지정 행 수 테스트
    df = sample_data_generator(num_rows=10)
    assert len(df) == 10
    
    # 시드 설정 테스트 (동일한 시드로 생성된 데이터는 같아야 함)
    df1 = sample_data_generator(num_rows=5, seed=42)
    df2 = sample_data_generator(num_rows=5, seed=42)
    
    # 데이터프레임 비교
    pd.testing.assert_frame_equal(df1, df2)
    
    # 열 확인
    expected_columns = ["id", "name", "value", "active", "timestamp"]
    assert all(col in df.columns for col in expected_columns)
    
    # 데이터 타입 확인
    assert pd.api.types.is_integer_dtype(df["id"])
    assert pd.api.types.is_string_dtype(df["name"])
    assert pd.api.types.is_integer_dtype(df["value"])
    assert pd.api.types.is_bool_dtype(df["active"])
    assert pd.api.types.is_datetime64_dtype(df["timestamp"])


def test_create_sample_csv(temp_dir):
    """create_sample_csv 함수 테스트."""
    # 샘플 CSV 파일 생성
    csv_path = temp_dir / "test_sample.csv"
    result_path = create_sample_csv(csv_path, num_rows=15, seed=42)
    
    # 파일 존재 확인
    assert os.path.exists(csv_path)
    assert result_path == csv_path
    
    # 파일 내용 확인
    df = pd.read_csv(csv_path)
    assert len(df) == 15
    assert all(col in df.columns for col in ["id", "name", "value", "active", "timestamp"])


def test_infer_schema_from_pandas(sample_data):
    """infer_schema_from_pandas 함수 테스트."""
    schema = infer_schema_from_pandas(sample_data)
    
    # 스키마 필드 확인
    assert len(schema.fields) == 5
    
    # 필드 이름 확인
    field_names = [field.name for field in schema.fields]
    assert all(name in field_names for name in ["id", "name", "value", "active", "timestamp"])
    
    # 필드 타입 확인
    field_dict = {field.name: field for field in schema.fields}
    
    assert "integer" in str(field_dict["id"].field_type).lower()
    assert "string" in str(field_dict["name"].field_type).lower()
    assert "integer" in str(field_dict["value"].field_type).lower()
    assert "boolean" in str(field_dict["active"].field_type).lower()
    assert "timestamp" in str(field_dict["timestamp"].field_type).lower()


def test_get_dataset_stats(sample_data):
    """get_dataset_stats 함수 테스트."""
    stats = get_dataset_stats(sample_data)
    
    # 기본 통계 확인
    assert stats["row_count"] == len(sample_data)
    assert stats["column_count"] == len(sample_data.columns)
    assert set(stats["columns"]) == set(sample_data.columns)
    
    # 데이터 타입 확인
    assert all(col in stats["dtypes"] for col in sample_data.columns)
    
    # 숫자형 열에 대한 통계 확인
    assert "numeric_columns" in stats
    assert "id" in stats["numeric_columns"]
    assert "value" in stats["numeric_columns"]
    
    # 숫자형 통계 확인
    assert "numeric_stats" in stats
    assert "id" in stats["numeric_stats"]
    assert "min" in stats["numeric_stats"]["id"]
    assert "max" in stats["numeric_stats"]["id"]
    assert "mean" in stats["numeric_stats"]["id"]
    assert "median" in stats["numeric_stats"]["id"]
    assert "std" in stats["numeric_stats"]["id"]
