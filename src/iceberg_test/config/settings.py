"""
설정 파일 관리 모듈.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from iceberg_test.models.config import IcebergConfig


def get_config_path() -> Path:
    """
    설정 파일 경로를 반환합니다.
    
    Returns:
        Path: 설정 파일 경로
    """
    config_dir = Path.home() / ".iceberg-test"
    config_dir.mkdir(exist_ok=True, parents=True)
    return config_dir / "config.json"


def get_default_config() -> IcebergConfig:
    """
    기본 설정을 반환합니다.
    
    Returns:
        IcebergConfig: 기본 설정 객체
    """
    return IcebergConfig()


def load_config(config_path: Optional[Path] = None) -> IcebergConfig:
    """
    설정 파일을 로드합니다.
    
    Args:
        config_path: 설정 파일 경로 (기본값: None, 기본 경로 사용)
        
    Returns:
        IcebergConfig: 로드된 설정 객체
    """
    if config_path is None:
        config_path = get_config_path()
    
    # 설정 파일이 없으면 기본 설정 사용
    if not config_path.exists():
        return get_default_config()
    
    # 설정 파일 로드
    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)
        
        return IcebergConfig(**config_data)
    except Exception:
        # 로드 실패 시 기본 설정 사용
        return get_default_config()


def save_config(config: IcebergConfig, config_path: Optional[Path] = None) -> None:
    """
    설정을 파일에 저장합니다.
    
    Args:
        config: 저장할 설정 객체
        config_path: 설정 파일 경로 (기본값: None, 기본 경로 사용)
    """
    if config_path is None:
        config_path = get_config_path()
    
    # 설정 디렉토리 생성
    config_path.parent.mkdir(exist_ok=True, parents=True)
    
    # 설정 파일 저장
    with open(config_path, "w") as f:
        # dataclass를 사전으로 변환
        config_dict = config.to_dict()
        json.dump(config_dict, f, indent=2)
