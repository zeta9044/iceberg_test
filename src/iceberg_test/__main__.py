"""
CLI 애플리케이션 엔트리포인트.
"""

import sys
from iceberg_test.cli.app import app as cli_app

app = cli_app

if __name__ == "__main__":
    sys.exit(app())
