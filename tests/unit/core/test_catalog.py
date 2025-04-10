"""
카탈로그 관련 함수 테스트.
"""
import pytest
from unittest.mock import patch, MagicMock

from iceberg_test.core.catalog import get_catalog, list_namespaces, list_tables
from iceberg_test.exceptions import CatalogConnectionError
from iceberg_test.models.config import IcebergConfig


def test_get_catalog(test_config):
    """get_catalog 함수 테스트."""
    # 실제 카탈로그 연결 테스트
    catalog = get_catalog(test_config)
    
    # 기본 검사
    assert catalog is not None
    assert catalog.name == test_config.catalog_name


def test_get_catalog_with_error():
    """get_catalog 함수 오류 처리 테스트."""
    # 잘못된 설정으로 오류 테스트
    invalid_config = IcebergConfig(
        catalog_name="invalid_catalog",
        warehouse_path="invalid://path"
    )
    
    # 오류 발생 확인
    with pytest.raises(CatalogConnectionError):
        get_catalog(invalid_config)


def test_get_catalog_with_default_config():
    """get_catalog 함수 기본 설정 테스트."""
    # config=None으로 호출하면 기본 설정 사용
    with patch('iceberg_test.core.catalog.IcebergConfig') as mock_config:
        mock_config.return_value = IcebergConfig()
        
        with patch('iceberg_test.core.catalog.load_catalog') as mock_load:
            mock_catalog = MagicMock()
            mock_load.return_value = mock_catalog
            
            catalog = get_catalog()
            
            # IcebergConfig가 기본값으로 생성되었는지 확인
            mock_config.assert_called_once()
            
            # 카탈로그가 반환되었는지 확인
            assert catalog == mock_catalog


def test_list_namespaces(test_catalog):
    """list_namespaces 함수 테스트."""
    # 실제 호출 테스트
    with patch.object(test_catalog, 'list_namespaces') as mock_list:
        mock_list.return_value = ["default", "test"]
        
        # 함수 호출
        namespaces = list_namespaces(test_catalog)
        
        # 결과 확인
        assert namespaces == ["default", "test"]
        mock_list.assert_called_once()


def test_list_tables(test_catalog):
    """list_tables 함수 테스트."""
    namespace = "test_namespace"
    
    # 실제 호출 테스트
    with patch.object(test_catalog, 'list_tables') as mock_list:
        mock_list.return_value = ["table1", "table2"]
        
        # 함수 호출
        tables = list_tables(namespace, test_catalog)
        
        # 결과 확인
        assert tables == ["table1", "table2"]
        mock_list.assert_called_once_with(namespace)
