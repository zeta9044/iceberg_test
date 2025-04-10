"""
테이블 관련 함수 테스트.
"""
import pytest
from unittest.mock import patch, MagicMock

import pandas as pd
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    IntegerType,
    NestedField,
    StringType,
)

from iceberg_test.core.table import (
    create_table,
    load_table,
    read_from_table,
    write_to_table,
    drop_table,
)
from iceberg_test.exceptions import TableNotFoundError, WriteError
from iceberg_test.models.config import IcebergConfig


@pytest.fixture
def mock_table():
    """테스트용 모의 테이블 객체."""
    table = MagicMock(spec=Table)
    table.name.return_value = "test_table"
    table.identifier.return_value = "test_namespace.test_table"
    return table


@pytest.fixture
def test_schema():
    """테스트용 스키마."""
    return Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType()),
    )


def test_create_table(test_catalog, test_schema):
    """create_table 함수 테스트."""
    table_name = "new_table"
    namespace = "test_namespace"
    
    # 모의 호출
    with patch.object(test_catalog, 'create_table') as mock_create:
        mock_table = MagicMock(spec=Table)
        mock_table.name.return_value = table_name
        mock_create.return_value = mock_table
        
        # 함수 호출
        result = create_table(
            table_name=table_name,
            schema=test_schema,
            namespace=namespace,
            catalog=test_catalog
        )
        
        # 결과 확인
        assert result == mock_table
        mock_create.assert_called_once()
        
        # 호출 인자 확인
        args, kwargs = mock_create.call_args
        assert kwargs["identifier"] == f"{namespace}.{table_name}"
        assert kwargs["schema"] == test_schema
        assert "properties" in kwargs


def test_load_table(test_catalog):
    """load_table 함수 테스트."""
    table_name = "existing_table"
    namespace = "test_namespace"
    
    # 성공 케이스
    with patch.object(test_catalog, 'load_table') as mock_load:
        mock_table = MagicMock(spec=Table)
        mock_load.return_value = mock_table
        
        # 함수 호출
        result = load_table(
            table_name=table_name,
            namespace=namespace,
            catalog=test_catalog
        )
        
        # 결과 확인
        assert result == mock_table
        mock_load.assert_called_once_with(f"{namespace}.{table_name}")
    
    # 테이블을 찾을 수 없는 경우
    with patch.object(test_catalog, 'load_table') as mock_load:
        mock_load.side_effect = Exception("Table not found")
        
        # 예외 발생 확인
        with pytest.raises(TableNotFoundError):
            load_table(
                table_name=table_name,
                namespace=namespace,
                catalog=test_catalog
            )


def test_write_to_table(mock_table, test_catalog, sample_csv_path):
    """write_to_table 함수 테스트."""
    table_name = "write_table"
    namespace = "test_namespace"
    
    # load_table 모의 설정
    with patch('iceberg_test.core.table.load_table') as mock_load:
        mock_load.return_value = mock_table
        
        # append 컨텍스트 매니저 모의 설정
        mock_append = MagicMock()
        mock_table.new_append.return_value.__enter__.return_value = mock_append
        
        # 데이터프레임으로 쓰기
        df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        write_to_table(
            table_name=table_name,
            data=df,
            namespace=namespace,
            catalog=test_catalog
        )
        
        # 함수 호출 확인
        mock_load.assert_called_once_with(table_name, namespace, test_catalog, None)
        mock_table.new_append.assert_called_once()
        mock_append.append_data.assert_called_once()
        
        # CSV 파일로 쓰기
        mock_append.reset_mock()
        mock_load.reset_mock()
        mock_table.new_append.reset_mock()
        
        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = df
            
            write_to_table(
                table_name=table_name,
                data=str(sample_csv_path),
                namespace=namespace,
                catalog=test_catalog
            )
            
            # 함수 호출 확인
            mock_read_csv.assert_called_once_with(str(sample_csv_path))
            mock_load.assert_called_once()
            mock_table.new_append.assert_called_once()
            mock_append.append_data.assert_called_once()
        
        # 덮어쓰기 모드 테스트
        mock_append.reset_mock()
        mock_load.reset_mock()
        mock_table.new_append.reset_mock()
        mock_overwrite = MagicMock()
        mock_table.new_overwrite.return_value.__enter__.return_value = mock_overwrite
        
        write_to_table(
            table_name=table_name,
            data=df,
            namespace=namespace,
            catalog=test_catalog,
            overwrite=True
        )
        
        # 함수 호출 확인
        mock_table.new_overwrite.assert_called_once()
        mock_overwrite.overwrite_data.assert_called_once()
        
        # 에러 케이스
        mock_load.side_effect = Exception("Error")
        
        with pytest.raises(WriteError):
            write_to_table(
                table_name=table_name,
                data=df,
                namespace=namespace,
                catalog=test_catalog
            )


def test_read_from_table(mock_table, test_catalog):
    """read_from_table 함수 테스트."""
    table_name = "read_table"
    namespace = "test_namespace"
    
    # 모의 데이터
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    
    # load_table 모의 설정
    with patch('iceberg_test.core.table.load_table') as mock_load:
        mock_load.return_value = mock_table
        
        # scan 모의 설정
        mock_scan = MagicMock()
        mock_table.scan.return_value = mock_scan
        mock_scan.to_pandas.return_value = df
        
        # 기본 읽기
        result = read_from_table(
            table_name=table_name,
            namespace=namespace,
            catalog=test_catalog
        )
        
        # 결과 확인
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        mock_load.assert_called_once_with(table_name, namespace, test_catalog, None)
        mock_table.scan.assert_called_once()
        mock_scan.to_pandas.assert_called_once()
        
        # 필터 및 제한 테스트
        mock_load.reset_mock()
        mock_table.scan.reset_mock()
        mock_scan.reset_mock()
        
        filter_expr = "id > 1"
        limit = 2
        columns = ["id", "name"]
        
        mock_filter = MagicMock()
        mock_select = MagicMock()
        mock_scan.filter.return_value = mock_filter
        mock_filter.select.return_value = mock_select
        mock_select.to_pandas.return_value = df.iloc[:2]
        
        result = read_from_table(
            table_name=table_name,
            namespace=namespace,
            limit=limit,
            filter_expr=filter_expr,
            columns=columns,
            catalog=test_catalog
        )
        
        # 결과 확인
        assert len(result) == 2
        mock_scan.filter.assert_called_once_with(filter_expr)
        mock_filter.select.assert_called_once_with(*columns)
        
        # 에러 케이스
        mock_load.side_effect = TableNotFoundError("Table not found")
        
        with pytest.raises(TableNotFoundError):
            read_from_table(
                table_name=table_name,
                namespace=namespace,
                catalog=test_catalog
            )


def test_drop_table(test_catalog):
    """drop_table 함수 테스트."""
    table_name = "drop_table"
    namespace = "test_namespace"
    
    # 성공 케이스
    with patch.object(test_catalog, 'drop_table') as mock_drop:
        mock_drop.return_value = True
        
        result = drop_table(
            table_name=table_name,
            namespace=namespace,
            catalog=test_catalog
        )
        
        # 결과 확인
        assert result is True
        mock_drop.assert_called_once_with(f"{namespace}.{table_name}", purge=False)
        
        # purge 옵션 테스트
        mock_drop.reset_mock()
        mock_drop.return_value = True
        
        result = drop_table(
            table_name=table_name,
            namespace=namespace,
            catalog=test_catalog,
            purge=True
        )
        
        assert result is True
        mock_drop.assert_called_once_with(f"{namespace}.{table_name}", purge=True)
    
    # 에러 케이스
    with patch.object(test_catalog, 'drop_table') as mock_drop:
        mock_drop.side_effect = Exception("Error")
        
        with pytest.raises(TableNotFoundError):
            drop_table(
                table_name=table_name,
                namespace=namespace,
                catalog=test_catalog
            )
