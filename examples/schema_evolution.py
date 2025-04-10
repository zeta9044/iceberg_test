"""
스키마 진화 예제.

이 예제는 Apache Iceberg의 스키마 진화 기능을 보여줍니다.
새 열 추가, 열 이름 변경, 데이터 타입 변경 등의 예제를 포함합니다.
"""

import os
import time
from pathlib import Path

import pandas as pd
import numpy as np
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from iceberg_test.core.catalog import get_catalog
from iceberg_test.core.table import (
    create_table,
    load_table,
    read_from_table,
    write_to_table,
)
from iceberg_test.models.config import IcebergConfig


def main():
    """
    스키마 진화 예제의 메인 함수.
    """
    # 작업 디렉토리 설정
    current_dir = os.path.dirname(os.path.abspath(__file__))
    warehouse_path = os.path.join(current_dir, "..", "temp_warehouse")
    os.makedirs(warehouse_path, exist_ok=True)
    
    print("="*80)
    print("Apache Iceberg 스키마 진화 예제")
    print("="*80)
    
    # 설정 생성
    config = IcebergConfig(
        catalog_name="evolution_catalog",
        warehouse_path=f"file://{warehouse_path}"
    )
    
    print(f"카탈로그: {config.catalog_name}")
    print(f"웨어하우스 경로: {config.warehouse_path}")
    print("-"*80)
    
    # 테이블 이름 설정
    table_name = "users"
    namespace = "evolution"
    
    try:
        # 카탈로그 연결
        print("카탈로그에 연결 중...")
        catalog = get_catalog(config)
        
        # 초기 스키마 정의
        print("\n1. 초기 스키마 정의 및 테이블 생성")
        print("-"*60)
        
        initial_schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
            NestedField(3, "email", StringType()),
            NestedField(4, "active", BooleanType(), required=True),
            NestedField(5, "created_at", TimestampType()),
        )
        
        # 스키마 출력
        print("초기 스키마:")
        for field in initial_schema.fields:
            print(f"  - {field.name} ({field.field_type}): 필수={field.required}")
        
        # 테이블 생성
        print(f"\n테이블 생성 중: {namespace}.{table_name}")
        table = create_table(
            table_name=table_name,
            schema=initial_schema,
            namespace=namespace,
            catalog=catalog,
        )
        print("테이블이 생성되었습니다.")
        
        # 초기 데이터 생성
        print("\n초기 데이터 생성 중...")
        initial_data = pd.DataFrame({
            "id": range(1, 6),
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
            "email": ["alice@example.com", "bob@example.com", None, "dave@example.com", "eve@example.com"],
            "active": [True, True, False, True, False],
            "created_at": pd.date_range(start="2023-01-01", periods=5),
        })
        
        # 데이터 미리보기
        print("\n초기 데이터:")
        print(initial_data)
        
        # 데이터 쓰기
        print("\n테이블에 초기 데이터 쓰기 중...")
        write_to_table(
            table_name=table_name,
            data=initial_data,
            namespace=namespace,
            catalog=catalog,
        )
        print("초기 데이터가 기록되었습니다.")
        
        # 현재 데이터 읽기
        print("\n현재 테이블에서 데이터 읽기 중...")
        result_df = read_from_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        print("\n현재 데이터:")
        print(result_df)
        print("-"*80)
        
        # 새 열 추가하는 스키마 진화
        print("\n2. 스키마 진화: 새 열 추가 (age, last_login)")
        print("-"*60)
        
        # 테이블 로드
        table = load_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 현재 스키마 가져오기
        current_schema = table.schema()
        
        # 새로운 열 추가
        updated_schema = current_schema.update_schema() \
            .add_column(path="age", field_type=IntegerType()) \
            .add_column(path="last_login", field_type=TimestampType()) \
            .apply()
        
        # 스키마 업데이트
        table.update_schema().schema(updated_schema).commit()
        print("스키마가 업데이트되었습니다.")
        
        # 업데이트된 스키마 출력
        print("\n업데이트된 스키마:")
        for field in updated_schema.fields:
            print(f"  - {field.name} ({field.field_type}): 필수={field.required}")
        
        # 새 데이터 생성 (새 열 포함)
        print("\n새 열이 포함된 새 데이터 생성 중...")
        new_data = pd.DataFrame({
            "id": range(6, 11),
            "name": ["Frank", "Grace", "Heidi", "Ivan", "Judy"],
            "email": ["frank@example.com", "grace@example.com", "heidi@example.com", 
                     "ivan@example.com", "judy@example.com"],
            "active": [True, False, True, True, False],
            "created_at": pd.date_range(start="2023-01-06", periods=5),
            "age": [25, 30, 35, 40, 45],
            "last_login": pd.date_range(start="2023-02-01", periods=5),
        })
        
        # 데이터 미리보기
        print("\n새 데이터:")
        print(new_data)
        
        # 데이터 쓰기
        print("\n테이블에 새 데이터 쓰기 중...")
        write_to_table(
            table_name=table_name,
            data=new_data,
            namespace=namespace,
            catalog=catalog,
        )
        print("새 데이터가 기록되었습니다.")
        
        # 현재 데이터 읽기
        print("\n현재 테이블에서 모든 데이터 읽기 중...")
        result_df = read_from_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        print("\n결합된 데이터 (이전 데이터는 새 열이 NULL):")
        print(result_df)
        print("-"*80)
        
        # 열 이름 변경하는 스키마 진화
        print("\n3. 스키마 진화: 열 이름 변경 (email -> contact_email)")
        print("-"*60)
        
        # 테이블 로드
        table = load_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        # 열 이름 변경
        renamed_schema = table.schema().update_schema() \
            .rename_column("email", "contact_email") \
            .apply()
        
        # 스키마 업데이트
        table.update_schema().schema(renamed_schema).commit()
        print("스키마가 업데이트되었습니다.")
        
        # 업데이트된 스키마 출력
        print("\n업데이트된 스키마 (이름 변경 후):")
        for field in renamed_schema.fields:
            print(f"  - {field.name} ({field.field_type}): 필수={field.required}")
        
        # 현재 데이터 읽기
        print("\n이름이 변경된 열로 데이터 읽기 중...")
        result_df = read_from_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )
        
        print("\n이름이 변경된 열이 포함된 데이터:")
        print(result_df.head())
        print("-"*80)
        
        # 요약
        print("\n스키마 진화 요약:")
        print("1. 초기 스키마에서 테이블 생성")
        print("2. 새 열 추가 (age, last_login)")
        print("3. 열 이름 변경 (email -> contact_email)")
        print("모든 변경이 백필 없이 원활하게 이루어졌습니다.")
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")
    
    print("\n예제 완료!")
    print("="*80)


if __name__ == "__main__":
    main()
