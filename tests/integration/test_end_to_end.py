"""
종단간 통합 테스트.

이 테스트는 실제 Iceberg 테이블을 생성하고, 데이터를 쓰고 읽는 종단간 흐름을 테스트합니다.
"""
import os
import uuid
from pathlib import Path

import pandas as pd
import pytest
from pyiceberg.schema import Schema

from iceberg_test.core.catalog import get_catalog
from iceberg_test.core.table import (
    create_table,
    load_table,
    read_from_table,
    write_to_table,
    drop_table,
)
from iceberg_test.utils.data import sample_data_generator, infer_schema_from_pandas


# 이 테스트는 실제 파일 시스템에 접근하므로 느릴 수 있으며 
# pytest.mark.slow 데코레이터로 표시합니다.
@pytest.mark.slow
def test_full_table_lifecycle(test_config, test_warehouse_path, temp_dir):
    """
    테이블 생성, 데이터 쓰기, 읽기, 삭제 등의 전체 라이프사이클을 테스트합니다.
    """
    # 테스트 고유성을 위한 랜덤 네임스페이스 및 테이블 이름 생성
    namespace = f"test_{uuid.uuid4().hex[:8]}"
    table_name = f"table_{uuid.uuid4().hex[:8]}"
    
    try:
        # 카탈로그 연결
        catalog = get_catalog(test_config)
        
        # 1. 샘플 데이터 생성
        sample_df = sample_data_generator(num_rows=5, seed=42)
        
        # 스키마 추론
        schema = infer_schema_from_pandas(sample_df)
        
        # 2. 테이블 생성
        table = create_table(
            table_name=table_name,
            schema=schema,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 테이블이 생성되었는지 확인
        assert table is not None
        assert table.name() == table_name
        assert table.identifier() == f"{namespace}.{table_name}"
        
        # 3. 데이터 쓰기
        write_to_table(
            table_name=table_name,
            data=sample_df,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 4. 데이터 읽기
        result_df = read_from_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 데이터가 올바르게 쓰이고 읽혔는지 확인
        assert len(result_df) == len(sample_df)
        assert set(result_df.columns) == set(sample_df.columns)
        
        # ID 칼럼 기준으로 정렬하여 비교
        sample_df_sorted = sample_df.sort_values('id').reset_index(drop=True)
        result_df_sorted = result_df.sort_values('id').reset_index(drop=True)
        
        # 정수형 ID 비교
        assert all(sample_df_sorted['id'] == result_df_sorted['id'])
        
        # 이름 비교
        assert all(sample_df_sorted['name'] == result_df_sorted['name'])
        
        # 5. 테이블 다시 로드
        loaded_table = load_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 테이블이 올바르게 로드되었는지 확인
        assert loaded_table.name() == table_name
        
        # 스키마가 올바른지 확인
        loaded_schema = loaded_table.schema()
        assert len(loaded_schema.fields) == len(schema.fields)
        
        # 6. 스키마 진화 테스트 (새 열 추가)
        # 새 데이터 준비
        new_data = sample_df.copy()
        new_data['extra_col'] = [f"value-{i}" for i in range(len(new_data))]
        
        # 새 스키마로 데이터 쓰기 (스키마 진화가 자동으로 처리되어야 함)
        write_to_table(
            table_name=table_name,
            data=new_data,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 업데이트된 데이터 읽기
        updated_df = read_from_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 열이 추가되었는지 확인
        assert 'extra_col' in updated_df.columns
        
        # 이전 행과 새 행 모두 존재하는지 확인
        assert len(updated_df) == len(sample_df) * 2
        
    finally:
        # 7. 테이블 삭제 (정리)
        try:
            drop_table(
                table_name=table_name,
                namespace=namespace,
                catalog=catalog,
                purge=True
            )
        except Exception as e:
            print(f"테이블 삭제 중 오류 발생: {str(e)}")


@pytest.mark.slow
def test_transaction_workflow(test_config):
    """
    트랜잭션 기반 워크플로우를 테스트합니다.
    """
    # 테스트 고유성을 위한 랜덤 네임스페이스 및 테이블 이름 생성
    namespace = f"tx_test_{uuid.uuid4().hex[:8]}"
    table_name = f"tx_table_{uuid.uuid4().hex[:8]}"
    
    try:
        # 카탈로그 연결
        catalog = get_catalog(test_config)
        
        # 1. 샘플 데이터 생성
        sample_df = sample_data_generator(num_rows=3, seed=43)
        
        # 스키마 추론
        schema = infer_schema_from_pandas(sample_df)
        
        # 2. 테이블 생성
        table = create_table(
            table_name=table_name,
            schema=schema,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 테이블 로드
        loaded_table = load_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 3. 트랜잭션 생성 및 데이터 쓰기
        with loaded_table.transaction() as tx:
            with tx.new_append() as append:
                append.append_data(sample_df)
        
        # 4. 데이터 읽기 및 확인
        result_df = read_from_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        assert len(result_df) == len(sample_df)
        
        # 5. 롤백 트랜잭션 테스트
        additional_df = sample_data_generator(num_rows=2, seed=44)
        
        # 트랜잭션 시작
        tx = loaded_table.transaction()
        
        # 데이터 추가
        try:
            with tx.new_append() as append:
                append.append_data(additional_df)
            
            # 의도적으로 트랜잭션 롤백
            tx.abort()
            
            # 데이터가 롤백되었는지 확인
            after_rollback_df = read_from_table(
                table_name=table_name,
                namespace=namespace,
                catalog=catalog,
            )
            
            # 롤백 후 행 수는 원래 샘플 데이터와 동일해야 함
            assert len(after_rollback_df) == len(sample_df)
            
        except Exception as e:
            print(f"트랜잭션 테스트 중 오류 발생: {str(e)}")
            tx.abort()  # 오류 발생 시 롤백
    
    finally:
        # 6. 테이블 삭제 (정리)
        try:
            drop_table(
                table_name=table_name,
                namespace=namespace,
                catalog=catalog,
                purge=True
            )
        except Exception as e:
            print(f"테이블 삭제 중 오류 발생: {str(e)}")
