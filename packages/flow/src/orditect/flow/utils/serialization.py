"""Serialization utilities"""
import json
from typing import Any


def serialize_result(result: Any) -> str:
    """Serialize a task result.

        Args:
            result: Task result

        Returns:
            JSON string

        Example:
            serialized = serialize_result({"data": [1, 2, 3]})
            # returns '{"data": [1, 2, 3]}'
        """
    try:
        return json.dumps(result, ensure_ascii=False)
    except (TypeError, ValueError) as e:

        return repr(result)


def deserialize_result(serialized: str) -> Any:
    """Deserialize a task result.

        Args:
            serialized: JSON string

        Returns:
            Task result

        Example:
            result = deserialize_result('{"data": [1, 2, 3]}')
            # returns {"data": [1, 2, 3]}
        """
    try:
        return json.loads(serialized)
    except (TypeError, ValueError) as e:
        return serialized