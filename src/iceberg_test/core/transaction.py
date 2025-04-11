"""
트랜잭션 관련 기능을 제공하는 모듈.
"""

from typing import Optional

from iceberg_test.core.table import load_table
from iceberg_test.exceptions import TransactionError
from iceberg_test.models.config import IcebergConfig
from pyiceberg.table import Table, Transaction


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
        return table.transaction()
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
        transaction.commit_transaction()
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
        # PyIceberg 1.x에서는 명시적인 롤백 메서드가 없으므로
        # 트랜잭션을 닫을 때 아무 작업도 수행하지 않습니다.
        # 트랜잭션은 커밋되지 않으면 자동으로 롤백됩니다.
        return True
    except Exception as e:
        raise TransactionError(f"트랜잭션 롤백 실패: {str(e)}") from e