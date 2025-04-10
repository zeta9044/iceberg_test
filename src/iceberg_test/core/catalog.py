"""
카탈로그 관련 기능을 제공하는 모듈.
"""

from typing import Dict, Optional

from pyiceberg.catalog import Catalog, load_catalog

from iceberg_test.models.config import IcebergConfig
from iceberg_test.exceptions import CatalogConnectionError


def get_catalog(config: Optional[IcebergConfig] = None) -> Catalog:
    """
    Iceberg 카탈로그에 연결합니다.
    
    Args:
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        
    Returns:
        Catalog: 연결된 카탈로그 객체
        
    Raises:
        CatalogConnectionError: 카탈로그 연결 실패 시 발생
    """
    if config is None:
        config = IcebergConfig()
    
    try:
        # 카탈로그 로드
        catalog = load_catalog(
            name=config.catalog_name,
            **config.connection_props
        )
        
        return catalog
    except Exception as e:
        raise CatalogConnectionError(f"카탈로그 연결 실패: {str(e)}") from e


def list_namespaces(catalog: Optional[Catalog] = None, config: Optional[IcebergConfig] = None) -> list:
    """
    카탈로그의 모든 네임스페이스를 나열합니다.
    
    Args:
        catalog: 카탈로그 객체 (기본값: None, 새로 연결)
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        
    Returns:
        list: 네임스페이스 목록
    """
    if catalog is None:
        catalog = get_catalog(config)
    
    return catalog.list_namespaces()


def list_tables(namespace: str, catalog: Optional[Catalog] = None, config: Optional[IcebergConfig] = None) -> list:
    """
    지정된 네임스페이스의 모든 테이블을 나열합니다.
    
    Args:
        namespace: 네임스페이스 이름
        catalog: 카탈로그 객체 (기본값: None, 새로 연결)
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        
    Returns:
        list: 테이블 목록
    """
    if catalog is None:
        catalog = get_catalog(config)
    
    return catalog.list_tables(namespace)
