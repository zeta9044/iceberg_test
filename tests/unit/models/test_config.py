"""
IcebergConfig 모델 테스트.
"""
import pytest

from iceberg_test.models.config import IcebergConfig, TransactionConfig, ScanConfig


def test_iceberg_config_defaults():
    """기본 설정값으로 IcebergConfig 생성 테스트."""
    config = IcebergConfig()
    
    assert config.catalog_name == "local"
    assert config.catalog_impl == "hive"
    assert config.catalog_type == "hadoop"
    assert "file:///tmp/iceberg/warehouse" in config.warehouse_path
    
    # 기본 connection_props 확인
    assert config.connection_props["type"] == config.catalog_type
    assert config.connection_props["catalog-impl"] == config.catalog_impl
    assert config.connection_props["warehouse"] == config.warehouse_path


def test_iceberg_config_custom_values():
    """사용자 지정 값으로 IcebergConfig 생성 테스트."""
    custom_config = IcebergConfig(
        catalog_name="custom_catalog",
        catalog_impl="rest",
        catalog_type="rest",
        warehouse_path="file:///custom/path",
        connection_props={"custom-key": "custom-value"}
    )
    
    assert custom_config.catalog_name == "custom_catalog"
    assert custom_config.catalog_impl == "rest"
    assert custom_config.catalog_type == "rest"
    assert custom_config.warehouse_path == "file:///custom/path"
    assert custom_config.connection_props["custom-key"] == "custom-value"


def test_iceberg_config_to_dict():
    """IcebergConfig의 to_dict 메서드 테스트."""
    config = IcebergConfig(catalog_name="test_catalog")
    config_dict = config.to_dict()
    
    assert isinstance(config_dict, dict)
    assert config_dict["catalog_name"] == "test_catalog"
    assert "warehouse_path" in config_dict
    assert "connection_props" in config_dict


def test_transaction_config():
    """TransactionConfig 테스트."""
    # 기본 설정값 테스트
    default_config = TransactionConfig()
    assert default_config.transaction_id is None
    assert default_config.isolation_level == "serializable"
    assert default_config.commit_strategy == "default"
    
    # 사용자 지정 값 테스트
    custom_config = TransactionConfig(
        transaction_id="test-tx-id",
        isolation_level="snapshot",
        commit_strategy="custom"
    )
    assert custom_config.transaction_id == "test-tx-id"
    assert custom_config.isolation_level == "snapshot"
    assert custom_config.commit_strategy == "custom"
    
    # to_dict 메서드 테스트
    config_dict = custom_config.to_dict()
    assert config_dict["transaction_id"] == "test-tx-id"
    assert config_dict["isolation_level"] == "snapshot"
    assert config_dict["commit_strategy"] == "custom"


def test_scan_config():
    """ScanConfig 테스트."""
    # 기본 설정값 테스트
    default_config = ScanConfig()
    assert default_config.plan_id is None
    assert default_config.max_threads == 4
    assert default_config.snapshot_id is None
    assert default_config.filter_expression is None
    assert default_config.limit is None
    assert default_config.selected_columns is None
    
    # 사용자 지정 값 테스트
    custom_config = ScanConfig(
        plan_id="test-plan",
        max_threads=8,
        snapshot_id=123,
        filter_expression="id > 100",
        limit=10,
        selected_columns=["id", "name"]
    )
    assert custom_config.plan_id == "test-plan"
    assert custom_config.max_threads == 8
    assert custom_config.snapshot_id == 123
    assert custom_config.filter_expression == "id > 100"
    assert custom_config.limit == 10
    assert custom_config.selected_columns == ["id", "name"]
    
    # to_dict 메서드 테스트
    config_dict = custom_config.to_dict()
    assert config_dict["plan_id"] == "test-plan"
    assert config_dict["max_threads"] == 8
    assert config_dict["selected_columns"] == ["id", "name"]
