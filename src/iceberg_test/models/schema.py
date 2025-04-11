"""
스키마 데이터 모델을 정의하는 모듈.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    IcebergType,

)


@dataclass
class SchemaField:
    """
    Iceberg 스키마 필드를 위한 데이터 클래스
    """
    # 필드 ID
    id: int
    
    # 필드 이름
    name: str
    
    # 필드 타입
    type: Union[IcebergType, str]
    
    # 필수 여부
    required: bool = True
    
    # 설명 (선택적)
    doc: Optional[str] = None
    
    def __post_init__(self) -> None:
        """
        필드 타입을 문자열에서 적절한 타입 객체로 변환합니다.
        """
        if isinstance(self.type, str):
            self.type = self._string_to_type(self.type)
    
    def _string_to_type(self, type_str: str) -> IcebergType:
        """
        문자열 타입을 Iceberg 타입 객체로 변환합니다.
        
        Args:
            type_str: 타입 문자열
            
        Returns:
            Type: Iceberg 타입 객체
            
        Raises:
            ValueError: 알 수 없는 타입인 경우
        """
        type_map = {
            "boolean": BooleanType,
            "int": IntegerType,
            "integer": IntegerType,
            "long": LongType,
            "float": FloatType,
            "double": DoubleType,
            "string": StringType,
            "date": DateType,
            "timestamp": TimestampType,
        }
        
        for key, value in type_map.items():
            if key in type_str.lower():
                return value()
        
        # 기본값은 문자열
        return StringType()
    
    def to_dict(self) -> Dict:
        """
        필드를 딕셔너리로 변환합니다.
        
        Returns:
            Dict: 필드 딕셔너리
        """
        return {
            "id": self.id,
            "name": self.name,
            "type": str(self.type),
            "required": self.required,
            "doc": self.doc,
        }


def create_schema_fields_from_dict(fields_dict: List[Dict]) -> List[SchemaField]:
    """
    딕셔너리 목록에서 스키마 필드 목록을 생성합니다.
    
    Args:
        fields_dict: 필드 딕셔너리 목록
        
    Returns:
        List[SchemaField]: 스키마 필드 목록
    """
    return [SchemaField(**field) for field in fields_dict]
