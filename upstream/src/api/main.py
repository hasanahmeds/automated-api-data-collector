"""Uvicorn entrypoint: uvicorn src.api.main:app"""
from src.api import app

__all__ = ["app"]
