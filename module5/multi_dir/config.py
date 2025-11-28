
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import os

@dataclass
class MultiDirConfig:
    """
    Configuration for multi-directory support.

    Attributes:
        roots: List of root directories. Typically yearly folders (e.g., ".../2024", ".../2025").
        excel_patterns: Filenames to search for the index file within each root (in priority order).
        allowed_exts: PDF file extensions to recognise.
        schema_hints: Optional per-root schema hints (column name mapping overrides).
    """
    roots: List[str]
    excel_patterns: List[str] = field(default_factory=lambda: [
        "index.xlsx", "目录.xlsx", "contracts.xlsx", "目录.xls", "index.xls"
    ])
    allowed_exts: List[str] = field(default_factory=lambda: [".pdf"])
    schema_hints: Dict[str, Dict[str, str]] = field(default_factory=dict)

    def resolve_index_path(self, root: str) -> Optional[str]:
        for name in self.excel_patterns:
            candidate = os.path.join(root, name)
            if os.path.exists(candidate):
                return candidate
        return None

    def validate(self) -> None:
        if not self.roots:
            raise ValueError("MultiDirConfig.roots is empty. Provide at least one directory root.")
        missing = [r for r in self.roots if not os.path.isdir(r)]
        if missing:
            raise FileNotFoundError(f"These roots do not exist or are not directories: {missing}")
