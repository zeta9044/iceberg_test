"""
트랜잭션 관련 함수 테스트.
"""
import pytest
from unittest.mock import patch, MagicMock

from pyiceberg.table import Table
from pyiceberg.transaction import Transaction

from iceberg_test.core.transaction import (
    create_transaction,
    create_transaction_for_table,
    commit_transaction,
    rollback_transaction,
)
from iceberg_test.exceptions import TransactionError


@pytest.fixture
def mock_table():
    """테스트용 모의 테이블 객체."""
    table = MagicMock(spec=Table)
    table.name.return_value = "test_table"
    table.identifier.return_value = "test_namespace.test_table"
    return table


@pytest.fixture
def mock_transaction():
    """테스트용 모의 트랜잭션 객체."""
    return MagicMock(spec=Transaction)


def test_create_transaction(mock_table, mock_transaction):
    """create_transaction 함수 테스트."""
    # 성공 케이스
    with patch.object(mock_table, 'transaction') as mock_tx:
        mock_tx.return_value = mock_transaction
        
        # 기본 격리 수준으로 호출
        result = create_transaction(mock_table)
        
        # 결과 확인
        assert result == mock_transaction
        mock_tx.assert_called_once_with(isolation_level="serializable")
        
        # 사용자 지정 격리 수준으로 호출
        mock_tx.reset_mock()
        
        result = create_transaction(mock_table, isolation_level="snapshot")
        
        assert result == mock_transaction
        mock_tx.assert_called_once_with(isolation_level="snapshot")
    
    # 오류 케이스
    with patch.object(mock_table, 'transaction') as mock_tx:
        mock_tx.side_effect = Exception("Transaction error")
        
        with pytest.raises(TransactionError):
            create_transaction(mock_table)


def test_create_transaction_for_table(mock_table, mock_transaction):
    """create_transaction_for_table 함수 테스트."""
    table_name = "tx_table"
    namespace = "test_namespace"
    
    # load_table 모의 설정
    with patch('iceberg_test.core.transaction.load_table') as mock_load:
        mock_load.return_value = mock_table
        
        # create_transaction 모의 설정
        with patch('iceberg_test.core.transaction.create_transaction') as mock_create:
            mock_create.return_value = mock_transaction
            
            # 함수 호출
            result = create_transaction_for_table(
                table_name=table_name,
                namespace=namespace
            )
            
            # 결과 확인
            assert result == mock_transaction
            mock_load.assert_called_once_with(table_name, namespace, config=None)
            mock_create.assert_called_once_with(mock_table, isolation_level="serializable")
            
            # 사용자 지정 격리 수준으로 호출
            mock_load.reset_mock()
            mock_create.reset_mock()
            
            result = create_transaction_for_table(
                table_name=table_name,
                namespace=namespace,
                isolation_level="snapshot"
            )
            
            assert result == mock_transaction
            mock_load.assert_called_once()
            mock_create.assert_called_once_with(mock_table, isolation_level="snapshot")


def test_commit_transaction(mock_transaction):
    """commit_transaction 함수 테스트."""
    # 성공 케이스
    result = commit_transaction(mock_transaction)
    
    # 결과 확인
    assert result is True
    mock_transaction.commit.assert_called_once()
    
    # 오류 케이스
    mock_transaction.commit.side_effect = Exception("Commit error")
    
    with pytest.raises(TransactionError):
        commit_transaction(mock_transaction)


def test_rollback_transaction(mock_transaction):
    """rollback_transaction 함수 테스트."""
    # 성공 케이스
    result = rollback_transaction(mock_transaction)
    
    # 결과 확인
    assert result is True
    mock_transaction.abort.assert_called_once()
    
    # 오류 케이스
    mock_transaction.abort.side_effect = Exception("Rollback error")
    
    with pytest.raises(TransactionError):
        rollback_transaction(mock_transaction)
