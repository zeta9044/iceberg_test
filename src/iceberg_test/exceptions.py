"""
커스텀 예외 클래스를 정의하는 모듈.
"""


class IcebergTestError(Exception):
    """
    모든 Iceberg 테스트 프로젝트 예외의 기본 클래스.
    """
    pass


class CatalogConnectionError(IcebergTestError):
    """
    카탈로그 연결 실패 시 발생하는 예외.
    """
    pass


class TableNotFoundError(IcebergTestError):
    """
    테이블을 찾을 수 없을 때 발생하는 예외.
    """
    pass


class SchemaError(IcebergTestError):
    """
    스키마 오류 시 발생하는 예외.
    """
    pass


class WriteError(IcebergTestError):
    """
    데이터 쓰기 실패 시 발생하는 예외.
    """
    pass


class ReadError(IcebergTestError):
    """
    데이터 읽기 실패 시 발생하는 예외.
    """
    pass


class TransactionError(IcebergTestError):
    """
    트랜잭션 오류 시 발생하는 예외.
    """
    pass


class ValidationError(IcebergTestError):
    """
    데이터 검증 실패 시 발생하는 예외.
    """
    pass


class ConfigError(IcebergTestError):
    """
    설정 오류 시 발생하는 예외.
    """
    pass
