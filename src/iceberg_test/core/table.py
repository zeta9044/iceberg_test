"""
테이블 관련 기능을 제공하는 모듈.
"""

from typing import Dict, List, Optional, Union

import pandas as pd
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortOrder

from iceberg_test.core.catalog import get_catalog
from iceberg_test.exceptions import TableNotFoundError, WriteError
from iceberg_test.models.config import IcebergConfig
from iceberg_test.utils.data import infer_schema_from_pandas


def create_table(
    table_name: str,
    schema: Optional[Schema] = None,
    namespace: str = "default",
    partition_spec: Optional[PartitionSpec] = None,
    sort_order: Optional[SortOrder] = None,
    properties: Optional[Dict[str, str]] = None,
    catalog: Optional[Catalog] = None,
    config: Optional[IcebergConfig] = None,
) -> Table:
    """
    새 Iceberg 테이블을 생성합니다.
    
    Args:
        table_name: 생성할 테이블 이름
        schema: 테이블 스키마 (기본값: None, 기본 스키마 사용)
        namespace: 테이블 네임스페이스 (기본값: "default")
        partition_spec: 파티션 스펙 (기본값: None)
        sort_order: 정렬 순서 (기본값: None)
        properties: 테이블 속성 (기본값: None)
        catalog: 카탈로그 객체 (기본값: None, 새로 연결)
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        
    Returns:
        Table: 생성된 Iceberg 테이블 객체
    """
    if catalog is None:
        catalog = get_catalog(config)
    
    # 기본 테이블 속성
    if properties is None:
        properties = {"format-version": "2"}
    
    # 전체 테이블 식별자
    full_table_name = f"{namespace}.{table_name}"
    
    # 테이블 생성
    creation_args = {
        "identifier": full_table_name,
        "schema": schema,
        "properties": properties,
    }
    
    # 선택적 인자 추가
    if partition_spec is not None:
        creation_args["partition_spec"] = partition_spec
    
    if sort_order is not None:
        creation_args["sort_order"] = sort_order
    
    # 테이블 생성
    return catalog.create_table(**creation_args)


def load_table(
    table_name: str,
    namespace: str = "default",
    catalog: Optional[Catalog] = None,
    config: Optional[IcebergConfig] = None,
) -> Table:
    """
    기존 Iceberg 테이블을 로드합니다.
    
    Args:
        table_name: 로드할 테이블 이름
        namespace: 테이블 네임스페이스 (기본값: "default")
        catalog: 카탈로그 객체 (기본값: None, 새로 연결)
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        
    Returns:
        Table: 로드된 Iceberg 테이블 객체
        
    Raises:
        TableNotFoundError: 테이블을 찾을 수 없을 때 발생
    """
    if catalog is None:
        catalog = get_catalog(config)
    
    # 전체 테이블 식별자
    full_table_name = f"{namespace}.{table_name}"
    
    try:
        return catalog.load_table(full_table_name)
    except Exception as e:
        raise TableNotFoundError(f"테이블을 찾을 수 없음: {full_table_name}") from e


def write_to_table(
    table_name: str,
    data: Union[str, pd.DataFrame],
    namespace: str = "default",
    catalog: Optional[Catalog] = None,
    config: Optional[IcebergConfig] = None,
    overwrite: bool = False,
) -> None:
    """
    Iceberg 테이블에 데이터를 씁니다.
    
    Args:
        table_name: 쓸 테이블 이름
        data: 파일 경로 또는 Pandas DataFrame
        namespace: 테이블 네임스페이스 (기본값: "default")
        catalog: 카탈로그 객체 (기본값: None, 새로 연결)
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        overwrite: 기존 데이터 덮어쓰기 여부 (기본값: False)
        
    Raises:
        WriteError: 데이터 쓰기 실패 시 발생
    """
    # 데이터가 문자열이면 파일 경로로 간주하고 로드
    if isinstance(data, str):
        try:
            df = pd.read_csv(data)
        except Exception as e:
            raise WriteError(f"CSV 파일 로드 실패: {str(e)}") from e
    else:
        df = data
    
    try:
        # 테이블 로드
        table = load_table(table_name, namespace, catalog, config)
        
        # 쓰기 작업 선택
        if overwrite:
            # 덮어쓰기 모드
            with table.new_overwrite() as overwrite_op:
                overwrite_op.overwrite_data(df)
        else:
            # 추가 모드
            with table.new_append() as append:
                append.append_data(df)
    except Exception as e:
        raise WriteError(f"테이블에 데이터 쓰기 실패: {str(e)}") from e


def read_from_table(
    table_name: str,
    namespace: str = "default",
    limit: Optional[int] = None,
    filter_expr: Optional[str] = None,
    snapshot_id: Optional[int] = None,
    columns: Optional[List[str]] = None,
    catalog: Optional[Catalog] = None,
    config: Optional[IcebergConfig] = None,
) -> pd.DataFrame:
    """
    Iceberg 테이블에서 데이터를 읽습니다.
    
    Args:
        table_name: 읽을 테이블 이름
        namespace: 테이블 네임스페이스 (기본값: "default")
        limit: 반환할 최대 행 수 (기본값: None, 모든 행 반환)
        filter_expr: 필터 표현식 (기본값: None)
        snapshot_id: 특정 스냅샷 ID (기본값: None, 최신 스냅샷 사용)
        columns: 읽을 열 목록 (기본값: None, 모든 열 읽기)
        catalog: 카탈로그 객체 (기본값: None, 새로 연결)
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        
    Returns:
        pd.DataFrame: 테이블 데이터가 포함된 Pandas DataFrame
        
    Raises:
        TableNotFoundError: 테이블을 찾을 수 없을 때 발생
    """
    try:
        # 테이블 로드
        table = load_table(table_name, namespace, catalog, config)
        
        # 스캔 빌더 시작
        scan_builder = table.scan()
        
        # 필터 추가 (있는 경우)
        if filter_expr is not None:
            scan_builder = scan_builder.filter(filter_expr)
        
        # 열 선택 (있는 경우)
        if columns is not None:
            scan_builder = scan_builder.select(*columns)
        
        # 스냅샷 ID 설정 (있는 경우)
        if snapshot_id is not None:
            scan_builder = scan_builder.use_snapshot(snapshot_id)
        
        # 데이터 읽기
        df = scan_builder.to_pandas()
        
        # 결과 제한 (있는 경우)
        if limit is not None and limit > 0:
            df = df.head(limit)
        
        return df
    except TableNotFoundError:
        raise
    except Exception as e:
        raise TableNotFoundError(f"테이블에서 데이터 읽기 실패: {str(e)}") from e


def drop_table(
    table_name: str,
    namespace: str = "default",
    catalog: Optional[Catalog] = None,
    config: Optional[IcebergConfig] = None,
    purge: bool = False,
) -> bool:
    """
    Iceberg 테이블을 삭제합니다.
    
    Args:
        table_name: 삭제할 테이블 이름
        namespace: 테이블 네임스페이스 (기본값: "default")
        catalog: 카탈로그 객체 (기본값: None, 새로 연결)
        config: Iceberg 설정 객체 (기본값: None, 기본 설정 사용)
        purge: 데이터 파일까지 삭제할지 여부 (기본값: False)
        
    Returns:
        bool: 삭제 성공 여부
        
    Raises:
        TableNotFoundError: 테이블을 찾을 수 없을 때 발생
    """
    if catalog is None:
        catalog = get_catalog(config)
    
    # 전체 테이블 식별자
    full_table_name = f"{namespace}.{table_name}"
    
    try:
        return catalog.drop_table(full_table_name, purge=purge)
    except Exception as e:
        raise TableNotFoundError(f"테이블 삭제 실패: {str(e)}") from e
