"""
pytest 공통 설정 및 fixture 파일.

이 파일은 pytest가 자동으로 로드하여 모든 테스트에서 공유하는 fixture를 정의합니다.
"""
import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from pyiceberg.catalog import Catalog

from iceberg_test.models.config import IcebergConfig
from iceberg_test.core.catalog import get_catalog
from iceberg_test.utils.data import sample_data_generator


@pytest.fixture(scope="session")
def temp_dir():
    """
    테스트용 임시 디렉토리를 생성하고 테스트 세션 후 정리합니다.
    
    Returns:
        Path: 임시 디렉토리 경로
    """
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session")
def test_warehouse_path(temp_dir):
    """
    테스트용 웨어하우스 경로를 생성합니다.
    
    Args:
        temp_dir: 임시 디렉토리 경로
        
    Returns:
        str: 웨어하우스 경로
    """
    warehouse_dir = temp_dir / "warehouse"
    warehouse_dir.mkdir(exist_ok=True)
    # file:// URI 형식으로 반환
    return f"file://{warehouse_dir.absolute()}"


@pytest.fixture(scope="session")
def test_config(test_warehouse_path):
    """
    테스트용 Iceberg 설정을 생성합니다.
    
    Args:
        test_warehouse_path: 웨어하우스 경로
        
    Returns:
        IcebergConfig: 테스트 설정 객체
    """
    return IcebergConfig(
        catalog_name="test_catalog",
        warehouse_path=test_warehouse_path
    )


@pytest.fixture(scope="session")
def test_catalog(test_config):
    """
    테스트용 Iceberg 카탈로그를 생성합니다.
    
    Args:
        test_config: 테스트 설정 객체
        
    Returns:
        Catalog: 테스트 카탈로그 객체
    """
    return get_catalog(test_config)


@pytest.fixture
def sample_data():
    """
    테스트용 샘플 데이터를 생성합니다.
    
    Returns:
        pd.DataFrame: 샘플 데이터
    """
    return sample_data_generator(num_rows=10, seed=42)


@pytest.fixture
def sample_csv_path(temp_dir, sample_data):
    """
    테스트용 샘플 CSV 파일을 생성합니다.
    
    Args:
        temp_dir: 임시 디렉토리 경로
        sample_data: 샘플 데이터
        
    Returns:
        Path: 샘플 CSV 파일 경로
    """
    csv_path = temp_dir / "sample_data.csv"
    sample_data.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def test_namespace():
    """
    테스트용 네임스페이스를 반환합니다.
    
    Returns:
        str: 테스트 네임스페이스
    """
    return "test_namespace"


@pytest.fixture
def test_table_name():
    """
    테스트용 테이블 이름을 반환합니다.
    
    Returns:
        str: 테스트 테이블 이름
    """
    return "test_table"
