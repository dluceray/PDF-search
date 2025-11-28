
"""
Module 5: Multi-directory index integration for the PDF Contract Search System.

This package is designed to plug into the existing system (Modules 1–4) without
requiring any example data. It provides:
- A normalised in-memory index assembled from multiple yearly directories.
- Support for multiple Excel index formats with partial field overlaps.
- Fuzzy filters for project/unit text and flexible date granularity (Y / YM / YMD).
- Contract number based PDF path resolution via naming rules.
- A FastAPI router (or Flask Blueprint compatible pattern) exposing endpoints.

Integration points:
- FastAPI: include_router(search_router, prefix="/api/mdirs").
- Flask: wrap the Router via ASGI or adapt endpoints (see notes in search_api.py).

No example files are included. The module expects the main app to pass folder
roots (e.g., ["/data/contracts/2024", "/data/contracts/2025"]). Each folder
contains an Excel index and a set of PDF files. The Excel schema may vary;
we unify the fields to a standard output.
"""

from .config import MultiDirConfig
from .aggregator import MultiDirAggregator
from .search_api import router as search_router

__all__ = ["MultiDirConfig", "MultiDirAggregator", "search_router"]
