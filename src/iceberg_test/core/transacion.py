"""
트랜잭션 관련 기능을 제공하는 모듈.
"""

from typing import Dict, Optional

from pyiceberg.table import Table
from pyiceberg.transaction import Transaction

from iceberg_test.core.table import load_table
from iceberg_test.exceptions import TransactionError
from iceberg_test.models.config import IcebergConfig


def create_transaction(
    table: Table,
    isolation_level: str = "serializable",
) -> Transaction:
    """
    테이블에 대한 새 트랜잭션을 생성합니다.
    
    Args:
        table: 트랜잭션을 시작할 테이블
        isolation_level: 격리 수준 (기본값: "serializable")
        
    Returns:
        Transaction: 생성된 트랜잭션 객체
        
    Raises:
        TransactionError: 트랜잭션 생성 실패 시 발생
    """
    try:
        return table.transaction(isolation_level=isolation_level)
    except Exception as e:
        raise TransactionError(f"트랜잭션 생성 실패: {str(e)}") from e


def create_transaction_for_table(
    table_name: str,
    namespace: str = "default",
    isolation_level: str = "serializable",
    config: Optional[IcebergConfig] = None,
) -> Transaction:
    """
    테이블 이름으로 새 트랜잭션을 생성합니다.
    
    Args:
        table_name: 테이블 이름
        namespace: 테이블 네임스페이스 (기본값: "default")
        isolation_level: 격리 수준 (기본값: "serializable")
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        
    Returns:
        Transaction: 생성된 트랜잭션 객체
        
    Raises:
        TransactionError: 트랜잭션 생성 실패 시 발생
    """
    # 테이블 로드
    table = load_table(table_name, namespace, config=config)
    
    # 트랜잭션 생성
    return create_transaction(table, isolation_level=isolation_level)


def commit_transaction(transaction: Transaction) -> bool:
    """
    트랜잭션을 커밋합니다.
    
    Args:
        transaction: 커밋할 트랜잭션 객체
        
    Returns:
        bool: 커밋 성공 여부
        
    Raises:
        TransactionError: 트랜잭션 커밋 실패 시 발생
    """
    try:
        transaction.commit()
        return True
    except Exception as e:
        raise TransactionError(f"트랜잭션 커밋 실패: {str(e)}") from e


def rollback_transaction(transaction: Transaction) -> bool:
    """
    트랜잭션을 롤백합니다.
    
    Args:
        transaction: 롤백할 트랜잭션 객체
        
    Returns:
        bool: 롤백 성공 여부
        
    Raises:
        TransactionError: 트랜잭션 롤백 실패 시 발생
    """
    try:
        transaction.abort()
        return True
    except Exception as e:
        raise TransactionError(f"트랜잭션 롤백 실패: {str(e)}") from e
