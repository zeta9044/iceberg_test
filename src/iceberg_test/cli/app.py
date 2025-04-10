"""
CLI 애플리케이션 정의 모듈.
"""

import typer
from rich.console import Console

from iceberg_test.cli.commands import (
    create_table_command,
    list_tables_command,
    read_table_command,
    write_table_command,
)
from iceberg_test import __version__

# CLI 앱 생성
app = typer.Typer(
    name="iceberg-test",
    help="Apache Iceberg 테스트 도구",
    add_completion=False,
)

# 콘솔 인스턴스 생성
console = Console()


@app.callback()
def callback(
    version: bool = typer.Option(
        False, "--version", "-v", help="버전 정보 표시", is_flag=True
    ),
) -> None:
    """
    Apache Iceberg 테스트 CLI 도구.
    """
    if version:
        console.print(f"iceberg-test 버전: {__version__}")
        raise typer.Exit()


# 명령어 등록
app.command(name="create-table")(create_table_command)
app.command(name="list-tables")(list_tables_command)
app.command(name="read-table")(read_table_command)
app.command(name="write-table")(write_table_command)


if __name__ == "__main__":
    app()
