"""
설정 데이터 모델을 정의하는 모듈.
"""
from dataclasses import dataclass, field
from typing import Dict, Optional
from pyiceberg.catalog import CatalogType


@dataclass
class IcebergConfig:
    """
    Apache Iceberg 연결 및 구성을 위한 데이터 클래스
    """
    # 카탈로그 이름
    catalog_name: str = "memory_catalog"

    # 카탈로그 구현
    catalog_impl: str = "memory"  # pyiceberg의 memory.py 구현체를 사용

    # 카탈로그 타입
    catalog_type: CatalogType = CatalogType.IN_MEMORY

    # 웨어하우스 경로
    warehouse_path: str = "file:///tmp/iceberg/warehouse"

    # 추가 연결 속성
    connection_props: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        기본 연결 속성을 초기화합니다.
        """
        # 기본 연결 속성 (필요한 경우)
        if not self.connection_props:
            self.connection_props = {
                "type": self.catalog_type.value,
                "catalog-impl": self.catalog_impl,
                "warehouse": self.warehouse_path,
            }

    def to_dict(self) -> Dict:
        """
        구성을 딕셔너리로 변환합니다.

        Returns:
            Dict: 구성 딕셔너리
        """
        return {
            "catalog_name": self.catalog_name,
            "catalog_impl": self.catalog_impl,
            "catalog_type": self.catalog_type.value,
            "warehouse_path": self.warehouse_path,
            "connection_props": self.connection_props,
        }


@dataclass
class TransactionConfig:
    """
    Iceberg 트랜잭션 구성을 위한 데이터 클래스
    """
    # 트랜잭션 ID
    transaction_id: Optional[str] = None

    # 격리 수준
    isolation_level: str = "serializable"

    # 커밋 전략
    commit_strategy: str = "default"

    def to_dict(self) -> Dict:
        """
        구성을 딕셔너리로 변환합니다.

        Returns:
            Dict: 구성 딕셔너리
        """
        return {
            "transaction_id": self.transaction_id,
            "isolation_level": self.isolation_level,
            "commit_strategy": self.commit_strategy,
        }


@dataclass
class ScanConfig:
    """
    Iceberg 테이블 스캔 구성을 위한 데이터 클래스
    """
    # 스캔 계획 ID
    plan_id: Optional[str] = None

    # 최대 스캔 스레드
    max_threads: int = 4

    # 스냅샷 ID (특정 스냅샷을 스캔하는 경우)
    snapshot_id: Optional[int] = None

    # 필터 표현식
    filter_expression: Optional[str] = None

    # 최대 반환 행 수
    limit: Optional[int] = None

    # 선택할 열 목록
    selected_columns: Optional[list] = None

    def to_dict(self) -> Dict:
        """
        구성을 딕셔너리로 변환합니다.

        Returns:
            Dict: 구성 딕셔너리
        """
        return {
            "plan_id": self.plan_id,
            "max_threads": self.max_threads,
            "snapshot_id": self.snapshot_id,
            "filter_expression": self.filter_expression,
            "limit": self.limit,
            "selected_columns": self.selected_columns,
        }