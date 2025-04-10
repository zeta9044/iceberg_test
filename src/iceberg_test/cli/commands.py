"""
CLI 명령어 정의 모듈.
"""

import os
from pathlib import Path
from typing import List, Optional

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from iceberg_test.core.catalog import get_catalog, list_namespaces, list_tables
from iceberg_test.core.table import (
    create_table,
    read_from_table,
    write_to_table,
)
from iceberg_test.exceptions import IcebergTestError
from iceberg_test.models.config import IcebergConfig
from iceberg_test.utils.data import create_sample_csv, infer_schema_from_pandas

# 콘솔 인스턴스 생성
console = Console()


def create_table_command(
    table_name: str = typer.Argument(..., help="생성할 테이블 이름"),
    namespace: str = typer.Option("default", "--namespace", "-n", help="테이블 네임스페이스"),
    schema_file: Optional[Path] = typer.Option(
        None, "--schema", "-s", help="스키마 정의 파일 경로 (CSV 형식)"
    ),
    sample_file: Optional[Path] = typer.Option(
        None, "--sample", "-f", help="샘플 데이터 파일 경로 (CSV 형식)"
    ),
    catalog_name: str = typer.Option("local", "--catalog", "-c", help="카탈로그 이름"),
    warehouse_path: str = typer.Option(
        "file:///tmp/iceberg/warehouse", "--warehouse", "-w", help="웨어하우스 경로"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="상세 출력", is_flag=True),
) -> None:
    """
    새 Iceberg 테이블을 생성합니다.
    """
    try:
        # 설정 생성
        config = IcebergConfig(
            catalog_name=catalog_name,
            warehouse_path=warehouse_path,
        )
        
        # 스키마 결정
        if schema_file or sample_file:
            # 스키마 파일이나 샘플 파일에서 스키마 추론
            if sample_file:
                # 샘플 파일에서 스키마 추론
                df = pd.read_csv(sample_file)
                schema = infer_schema_from_pandas(df)
                console.print(f"[green]샘플 파일에서 스키마를 추론했습니다: {sample_file}[/green]")
            else:
                # 스키마 파일에서 스키마 로드
                df = pd.read_csv(schema_file)
                schema = infer_schema_from_pandas(df)
                console.print(f"[green]스키마 파일에서 스키마를 로드했습니다: {schema_file}[/green]")
            
            if verbose:
                # 스키마 정보 출력
                console.print("[bold]스키마 정보:[/bold]")
                schema_table = Table(title="필드 정보")
                schema_table.add_column("ID", style="cyan")
                schema_table.add_column("이름", style="green")
                schema_table.add_column("타입", style="magenta")
                schema_table.add_column("필수", style="red")
                
                for field in schema.fields:
                    schema_table.add_row(
                        str(field.field_id),
                        field.name,
                        str(field.field_type),
                        "Yes" if field.required else "No",
                    )
                
                console.print(schema_table)
        else:
            # 기본 스키마 사용
            schema = None
            console.print("[yellow]기본 스키마를 사용합니다.[/yellow]")
        
        # 테이블 생성
        table = create_table(
            table_name=table_name,
            schema=schema,
            namespace=namespace,
            config=config,
        )
        
        console.print(f"[bold green]테이블이 성공적으로 생성되었습니다: {namespace}.{table_name}[/bold green]")
        
        # 샘플 데이터 쓰기
        if sample_file:
            write_to_table(
                table_name=table_name,
                data=sample_file,
                namespace=namespace,
                config=config,
            )
            console.print(f"[green]샘플 데이터가 테이블에 기록되었습니다: {sample_file}[/green]")
    
    except IcebergTestError as e:
        console.print(f"[bold red]오류: {str(e)}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]예상치 못한 오류: {str(e)}[/bold red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(code=1)


def list_tables_command(
    namespace: str = typer.Argument(..., help="네임스페이스"),
    catalog_name: str = typer.Option("local", "--catalog", "-c", help="카탈로그 이름"),
    warehouse_path: str = typer.Option(
        "file:///tmp/iceberg/warehouse", "--warehouse", "-w", help="웨어하우스 경로"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="상세 출력", is_flag=True),
) -> None:
    """
    지정된 네임스페이스의 테이블 목록을 표시합니다.
    """
    try:
        # 설정 생성
        config = IcebergConfig(
            catalog_name=catalog_name,
            warehouse_path=warehouse_path,
        )
        
        # 카탈로그 연결
        catalog = get_catalog(config)
        
        # 테이블 목록 가져오기
        tables = list_tables(namespace, catalog)
        
        if tables:
            # 테이블 목록 표시
            table = Table(title=f"{namespace} 네임스페이스의 테이블")
            table.add_column("이름", style="green")
            
            for table_name in tables:
                table.add_row(table_name)
            
            console.print(table)
            
            console.print(f"[green]총 {len(tables)}개의 테이블이 있습니다.[/green]")
        else:
            console.print(f"[yellow]{namespace} 네임스페이스에 테이블이 없습니다.[/yellow]")
    
    except IcebergTestError as e:
        console.print(f"[bold red]오류: {str(e)}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]예상치 못한 오류: {str(e)}[/bold red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(code=1)


def read_table_command(
    table_name: str = typer.Argument(..., help="읽을 테이블 이름"),
    namespace: str = typer.Option("default", "--namespace", "-n", help="테이블 네임스페이스"),
    output_file: Optional[Path] = typer.Option(
        None, "--output", "-o", help="출력 파일 경로 (CSV 형식)"
    ),
    limit: int = typer.Option(10, "--limit", "-l", help="표시할 최대 행 수"),
    catalog_name: str = typer.Option("local", "--catalog", "-c", help="카탈로그 이름"),
    warehouse_path: str = typer.Option(
        "file:///tmp/iceberg/warehouse", "--warehouse", "-w", help="웨어하우스 경로"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="상세 출력", is_flag=True),
) -> None:
    """
    Iceberg 테이블에서 데이터를 읽습니다.
    """
    try:
        # 설정 생성
        config = IcebergConfig(
            catalog_name=catalog_name,
            warehouse_path=warehouse_path,
        )
        
        # 테이블에서 데이터 읽기
        df = read_from_table(
            table_name=table_name,
            namespace=namespace,
            limit=None,  # 전체 데이터 읽기
            config=config,
        )
        
        # 출력 파일에 저장
        if output_file:
            df.to_csv(output_file, index=False)
            console.print(f"[green]데이터가 {output_file}에 저장되었습니다.[/green]")
        
        # 콘솔에 표시
        if not df.empty:
            # 테이블 생성
            table = Table(title=f"{namespace}.{table_name} 데이터")
            
            # 열 추가
            for col in df.columns:
                table.add_column(col)
            
            # 행 추가 (제한된 수만큼)
            for _, row in df.head(limit).iterrows():
                table.add_row(*[str(val) for val in row])
            
            console.print(table)
            
            # 총 행 수 표시
            if len(df) > limit:
                console.print(f"[green]총 {len(df)}개 행 중 {limit}개가 표시되었습니다.[/green]")
                
                if not output_file:
                    console.print("[yellow]모든 데이터를 보려면 --output 옵션을 사용하세요.[/yellow]")
        else:
            console.print(f"[yellow]테이블 {namespace}.{table_name}에 데이터가 없습니다.[/yellow]")
    
    except IcebergTestError as e:
        console.print(f"[bold red]오류: {str(e)}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]예상치 못한 오류: {str(e)}[/bold red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(code=1)


def write_table_command(
    table_name: str = typer.Argument(..., help="쓸 테이블 이름"),
    data_file: Path = typer.Argument(..., help="데이터 파일 경로 (CSV 형식)"),
    namespace: str = typer.Option("default", "--namespace", "-n", help="테이블 네임스페이스"),
    overwrite: bool = typer.Option(
        False, "--overwrite", help="기존 데이터 덮어쓰기", is_flag=True
    ),
    catalog_name: str = typer.Option("local", "--catalog", "-c", help="카탈로그 이름"),
    warehouse_path: str = typer.Option(
        "file:///tmp/iceberg/warehouse", "--warehouse", "-w", help="웨어하우스 경로"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="상세 출력", is_flag=True),
) -> None:
    """
    Iceberg 테이블에 데이터를 씁니다.
    """
    try:
        # 파일 존재 확인
        if not os.path.exists(data_file):
            console.print(f"[bold red]파일을 찾을 수 없습니다: {data_file}[/bold red]")
            raise typer.Exit(code=1)
        
        # 설정 생성
        config = IcebergConfig(
            catalog_name=catalog_name,
            warehouse_path=warehouse_path,
        )
        
        # 파일 로드
        console.print(f"[green]파일을 로드 중: {data_file}[/green]")
        
        # 데이터 쓰기
        write_to_table(
            table_name=table_name,
            data=data_file,
            namespace=namespace,
            config=config,
            overwrite=overwrite,
        )
        
        console.print(f"[bold green]데이터가 성공적으로 테이블에 기록되었습니다: {namespace}.{table_name}[/bold green]")
        
        if overwrite:
            console.print("[yellow]기존 데이터를 덮어썼습니다.[/yellow]")
        else:
            console.print("[green]기존 데이터에 추가했습니다.[/green]")
    
    except IcebergTestError as e:
        console.print(f"[bold red]오류: {str(e)}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]예상치 못한 오류: {str(e)}[/bold red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(code=1)
