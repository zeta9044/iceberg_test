"""
간단한 테이블 생성 및 사용 예제.
"""

import os

import pandas as pd
from iceberg_test.core.catalog import get_catalog
from iceberg_test.core.table import (
    create_table,
    read_from_table,
    write_to_table,
)
from iceberg_test.models.config import IcebergConfig
from iceberg_test.utils.data import create_sample_csv, infer_schema_from_pandas


def main():
    """
    간단한 테이블 생성 및 사용 예제의 메인 함수.
    """
    # 작업 디렉토리 설정
    current_dir = os.path.dirname(os.path.abspath(__file__))
    warehouse_path = os.path.join(current_dir, "..", "temp_warehouse")
    os.makedirs(warehouse_path, exist_ok=True)

    # 샘플 데이터 파일 경로
    data_dir = os.path.join(current_dir, "..", "data", "sample")
    os.makedirs(data_dir, exist_ok=True)
    sample_data_path = os.path.join(data_dir, "simple_data.csv")

    print("="*80)
    print("간단한 Apache Iceberg 테이블 예제")
    print("="*80)

    # 설정 생성
    config = IcebergConfig(
        catalog_name="example_catalog",
        warehouse_path=f"file://{warehouse_path}"
    )

    print(config.to_dict())

    print(f"카탈로그: {config.catalog_name}")
    print(f"웨어하우스 경로: {config.warehouse_path}")
    print("-"*80)

    # 샘플 데이터 생성
    print("샘플 데이터 생성 중...")
    create_sample_csv(sample_data_path, num_rows=100, seed=42)
    print(f"샘플 데이터가 생성되었습니다: {sample_data_path}")

    # 샘플 데이터 로드
    print("데이터 로드 중...")
    df = pd.read_csv(sample_data_path)
    print(f"로드된 데이터 크기: {len(df)} 행 x {len(df.columns)} 열")

    # 데이터 미리보기
    print("\n데이터 미리보기:")
    print(df.head())
    print("-"*80)

    # 스키마 추론
    print("Iceberg 스키마 추론 중...")
    schema = infer_schema_from_pandas(df)

    # 필드 정보 출력
    print("\n스키마 필드:")
    for field in schema.fields:
        print(f"  - {field.name} ({field.field_type}): 필수={field.required}")
    print("-"*80)

    # 테이블 이름 설정
    table_name = "simple_table"
    namespace = "example"

    try:
        # 카탈로그 연결
        print("카탈로그에 연결 중...")
        catalog = get_catalog(config)
        catalog.create_namespace_if_not_exists(namespace)

        # 테이블 생성
        print(f"테이블 생성 중: {namespace}.{table_name}")
        create_table(
            table_name=table_name,
            schema=schema,
            namespace=namespace,
            catalog=catalog,
        )
        print("테이블이 생성되었습니다.")

        # 데이터 쓰기
        print("테이블에 데이터 쓰기 중...")
        write_to_table(
            table_name=table_name,
            data=df,
            namespace=namespace,
            catalog=catalog,
        )
        print("데이터가 기록되었습니다.")

        # 데이터 읽기
        print("테이블에서 데이터 읽기 중...")
        result_df = read_from_table(
            table_name=table_name,
            namespace=namespace,
            catalog=catalog,
        )

        # 결과 확인
        print(f"\n읽은 데이터 크기: {len(result_df)} 행 x {len(result_df.columns)} 열")
        print("\n읽은 데이터 미리보기:")
        print(result_df.head())

        # 데이터 검증
        if len(df) == len(result_df):
            print("\n데이터 검증: 성공 (행 수가 일치함)")
        else:
            print(f"\n데이터 검증: 실패 (원본 {len(df)} 행, 읽은 데이터 {len(result_df)} 행)")

    except Exception as e:
        print(f"오류 발생: {str(e)}")

    print("\n예제 완료!")
    print("="*80)


if __name__ == "__main__":
    main()
