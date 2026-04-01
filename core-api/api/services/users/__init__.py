"""
User services - user lookup and search operations
"""
from api.services.users.search import (
    attach_public_profiles,
    get_auth_user_by_email,
    get_public_profiles_by_ids,
    get_user_by_email,
    get_users_by_ids,
    search_users_by_email,
)

__all__ = [
    "attach_public_profiles",
    "get_auth_user_by_email",
    "get_public_profiles_by_ids",
    "search_users_by_email",
    "get_user_by_email",
    "get_users_by_ids",
]
