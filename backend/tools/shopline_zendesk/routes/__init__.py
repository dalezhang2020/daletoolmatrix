"""Shopline-Zendesk route package."""

from .mounts import (
    include_oauth_routes,
    include_shopline_frontend_routes,
    include_zaf_frontend_routes,
)

__all__ = [
    "include_oauth_routes",
    "include_shopline_frontend_routes",
    "include_zaf_frontend_routes",
]
