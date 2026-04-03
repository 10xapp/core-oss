"""
Application configuration settings
"""
import os
from cryptography.fernet import Fernet
from pydantic import model_validator
from pydantic_settings import BaseSettings
from typing import List, Optional, Set


# Allowed MIME types for file uploads
ALLOWED_MIME_TYPES: Set[str] = {
    # Images
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/heic",
    "image/heif",

    # Videos
    "video/mp4",
    "video/quicktime",
    "video/webm",

    # Documents
    "application/pdf",
    "text/plain",
    "text/markdown",
    "text/csv",
    "text/comma-separated-values",
    "text/tab-separated-values",
    "application/csv",
    "application/x-csv",
    "application/json",
    "text/json",
    "application/xml",
    "text/xml",
    "application/msword",
    "application/vnd.ms-excel",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.oasis.opendocument.text",
    "application/vnd.oasis.opendocument.spreadsheet",
    "application/vnd.oasis.opendocument.presentation",

    # Archives
    "application/zip",
    "application/x-zip-compressed",
    "application/x-7z-compressed",
    "application/x-rar-compressed",
    "application/gzip",
    "application/x-tar",

    # Audio
    "audio/mpeg",
    "audio/wav",
    "audio/x-wav",
    "audio/mp4",
    "audio/aac",
    "audio/ogg",
    "audio/webm",

    # Fallback
    "application/octet-stream",
}


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # App metadata
    app_name: str = "Core Productivity API"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # CORS settings
    allowed_origins: List[str] = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:5173",  # Vite dev server
    ]
    
    # Vercel deployments - set via environment variable
    # e.g., ALLOWED_ORIGINS=https://yourapp.vercel.app,https://yourdomain.com
    allowed_origins_env: str = ""
    
    @property
    def get_allowed_origins(self) -> List[str]:
        """Get combined allowed origins from defaults and environment"""
        origins = self.allowed_origins.copy()
        if self.allowed_origins_env:
            origins.extend([o.strip() for o in self.allowed_origins_env.split(",")])
        return origins
    
    # Supabase settings
    supabase_url: str = ""
    supabase_anon_key: str = ""
    supabase_service_role_key: str = ""  # Service role key for cron jobs and admin operations
    supabase_jwt_secret: str = ""  # Legacy JWT secret for verifying token signatures
    
    # Google OAuth settings (required for Google Calendar sync)
    google_client_id: str = ""
    google_client_secret: str = ""
    
    # Google Cloud Project settings (for push notifications)
    google_cloud_project_id: str = ""
    google_pubsub_topic: str = ""  # Full topic path: projects/PROJECT_ID/topics/TOPIC_NAME

    # Microsoft OAuth settings (for Outlook/Microsoft 365 sync)
    microsoft_client_id: str = ""
    microsoft_client_secret: str = ""
    microsoft_tenant_id: str = "common"  # "common" for multi-tenant + personal accounts
    microsoft_webhook_url: str = ""  # Webhook URL for Graph subscriptions (e.g., https://xxx.ngrok.app/api/webhooks/microsoft)

    # Webhook URLs (set in production)
    webhook_base_url: str = ""  # Set to your deployed API URL (e.g., https://your-api.vercel.app)
    
    # Cron job authentication
    cron_secret: str = ""  # Secret for authenticating cron job requests
    
    # Groq API settings (for AI email analysis)
    groq_api_key: str = ""
    
    # OpenAI API settings (for Chat Agent)
    openai_api_key: str = ""

    # Exa API settings (for web search RAG)
    exa_api_key: str = ""

    # Resend API settings (workspace invitations)
    resend_api_key: str = ""
    resend_from_domain: str = ""
    resend_from_email: str = ""
    frontend_url: str = "http://localhost:3000"

    @property
    def resend_from_address(self) -> str:
        """Return configured sender address for transactional emails."""
        if self.resend_from_email:
            return self.resend_from_email
        if self.resend_from_domain:
            return f"Core <invites@{self.resend_from_domain}>"
        return ""

    # Cloudflare R2 settings (files bucket)
    r2_account_id: str = ""
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""
    r2_bucket_name: str = "core-os-files"
    r2_s3_api: str = ""  # e.g., https://<account_id>.r2.cloudflarestorage.com
    r2_presigned_url_expiry: int = 604800  # 7 days in seconds (for downloads)
    r2_max_file_size: int = 52428800  # 50MB in bytes

    # Presigned upload settings
    r2_upload_url_expiry: int = 300  # 5 minutes for upload URLs
    r2_public_url: str = ""  # Public URL base, e.g., https://files.yourdomain.com

    @property
    def r2_public_base_url(self) -> str:
        """Get R2 public base URL (supports both env var names)"""
        return self.r2_public_url

    # Cloudflare R2 public bucket (for avatars)
    r2_public_bucket: str = "core-public"  # Public bucket name
    r2_public_access_url: str = ""  # Public r2.dev URL, e.g., https://pub-xxx.r2.dev

    # Image proxy (Cloudflare Worker)
    image_proxy_secret: str = ""  # HMAC shared secret with the Worker
    image_proxy_url: str = ""  # Worker URL, e.g., "https://img.yourdomain.com"

    # Chat attachment settings
    chat_attachment_max_size: int = 20971520  # 20MB in bytes
    chat_attachment_max_per_message: int = 3
    chat_attachment_upload_expiry: int = 300  # 5 minutes for upload URLs
    chat_attachment_download_expiry: int = 3600  # 1 hour for viewing

    # Anthropic (AI agent runtime)
    anthropic_api_key: str = ""

    # E2B (AI agent sandboxes)
    e2b_api_key: str = ""
    e2b_default_template: str = "base"  # Default sandbox template ID

    # Tirith pre-exec scanning (optional, for AI agent sandboxes)
    tirith_enabled: bool = False           # Set to true to enable scanning
    tirith_path: str = ""                  # Path to tirith binary inside sandbox; empty = auto-detect
    tirith_timeout: int = 10               # Seconds to wait for tirith check
    tirith_fail_mode: str = "open"         # "open" = allow on scanner failure; "closed" = block

    # Agent dispatch webhook
    agent_webhook_secret: str = ""  # Shared secret for Supabase webhook validation

    # QStash (job queue)
    qstash_token: str = ""
    qstash_url: str = ""  # QStash API base URL (e.g. https://qstash-us-east-1.upstash.io)
    qstash_current_signing_key: str = ""
    qstash_next_signing_key: str = ""
    qstash_worker_url: str = ""  # Base URL for worker endpoints (e.g. https://your-api.vercel.app)

    # Upstash Redis (rate limiting)
    upstash_redis_url: Optional[str] = None
    upstash_redis_token: Optional[str] = None

    # Cloudflare Turnstile (bot protection)
    turnstile_secret_key: str = ""

    # Token encryption (OAuth tokens at rest)
    token_encryption_key: str = ""           # Fernet key for encrypting OAuth tokens at rest
    token_encryption_key_previous: str = ""  # Previous key for zero-downtime key rotation

    # Sentry
    sentry_dsn: str = ""

    # Environment
    api_env: str = "development"

    @model_validator(mode="after")
    def validate_tirith_settings(self):
        """Fail fast on invalid Tirith scanning configuration."""
        self.tirith_fail_mode = self.tirith_fail_mode.strip().lower()
        self.tirith_path = self.tirith_path.strip()

        if self.tirith_timeout <= 0:
            raise ValueError("TIRITH_TIMEOUT must be greater than 0")

        if self.tirith_fail_mode not in {"open", "closed"}:
            raise ValueError("TIRITH_FAIL_MODE must be 'open' or 'closed'")

        return self

    @model_validator(mode="after")
    def validate_token_encryption_settings(self):
        """Fail fast on invalid token-encryption rollout configuration."""
        current_key = self.token_encryption_key
        previous_key = self.token_encryption_key_previous

        if previous_key and not current_key:
            raise ValueError(
                "TOKEN_ENCRYPTION_KEY_PREVIOUS requires TOKEN_ENCRYPTION_KEY to also be set"
            )

        if current_key:
            try:
                Fernet(current_key.encode())
            except Exception as exc:
                raise ValueError(
                    "TOKEN_ENCRYPTION_KEY must be a valid Fernet key"
                ) from exc

        if previous_key:
            try:
                Fernet(previous_key.encode())
            except Exception as exc:
                raise ValueError(
                    "TOKEN_ENCRYPTION_KEY_PREVIOUS must be a valid Fernet key"
                ) from exc

        return self

    class Config:
        # Load from ENV_FILE if set, otherwise .env
        # Usage: ENV_FILE=.env.test make start
        env_file = os.getenv("ENV_FILE", ".env")
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields in .env


# Initialize settings - will load from environment variables
try:
    settings = Settings()
except Exception as e:
    import sys
    print(f"❌ ERROR loading settings: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc(file=sys.stderr)
    # Re-raise to fail fast
    raise
