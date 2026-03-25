# Microsoft Azure App Configuration

This document contains the Azure AD app configuration for Microsoft/Outlook OAuth integration.

## Azure App Registration

| Setting | Value |
|---------|-------|
| App Name | `core-app` |
| Application (Client) ID | `YOUR_MICROSOFT_CLIENT_ID` |
| Directory (Tenant) ID | `YOUR_TENANT_ID` |
| Object ID | `YOUR_OBJECT_ID` |
| Supported Account Types | All Microsoft account users (Multitenant + Personal) |

## Client Secret

| Setting | Value |
|---------|-------|
| Description | `core-app-secret` |
| Secret ID | `YOUR_SECRET_ID` |
| Value | `YOUR_MICROSOFT_CLIENT_SECRET` |
| Expires | January 6, 2028 |

> **WARNING**: The secret value above is sensitive. In production, store it securely in environment variables or a secrets manager. This doc is for internal reference only.

## Redirect URIs

### Web Platform (Supabase OAuth)
```
https://YOUR_PROJECT.supabase.co/auth/v1/callback
```

### iOS / macOS Platform
| Setting | Value |
|---------|-------|
| Bundle ID | `app.10x.core` |
| Redirect URI | `msauth.app.10x.core://auth` |

## API Permissions (Microsoft Graph)

All permissions are **Delegated** type with admin consent granted.

| Permission | Description | Admin Consent |
|------------|-------------|---------------|
| `openid` | Sign users in | Granted |
| `email` | View users' email address | Granted |
| `profile` | View users' basic profile | Granted |
| `offline_access` | Maintain access (refresh tokens) | Granted |
| `User.Read` | Sign in and read user profile | Granted |
| `Mail.Read` | Read user mail | Granted |
| `Mail.Send` | Send mail as a user | Granted |
| `Calendars.ReadWrite` | Have full access to user calendars | Granted |

## Environment Variables

Add these to your backend environment (Vercel, `.env`, etc.):

```bash
MICROSOFT_CLIENT_ID=YOUR_MICROSOFT_CLIENT_ID
MICROSOFT_CLIENT_SECRET=YOUR_MICROSOFT_CLIENT_SECRET
MICROSOFT_TENANT_ID=common
MICROSOFT_NOTIFICATION_URL=https://your-api.vercel.app/api/webhooks/microsoft
```

## iOS Configuration

Add to `Development.xcconfig`:
```
MICROSOFT_CLIENT_ID = YOUR_MICROSOFT_CLIENT_ID
```

Add to `Info.plist` URL schemes:
```xml
<key>CFBundleURLTypes</key>
<array>
    <dict>
        <key>CFBundleURLSchemes</key>
        <array>
            <string>msauth.app.10x.core</string>
        </array>
    </dict>
</array>
```

## Supabase Configuration

Enable Azure provider in Supabase Dashboard → Authentication → Providers:

| Setting | Value |
|---------|-------|
| Provider | Azure |
| Client ID | `YOUR_MICROSOFT_CLIENT_ID` |
| Client Secret | `YOUR_MICROSOFT_CLIENT_SECRET` |
| Azure Tenant URL | `https://login.microsoftonline.com/common` |

## Important Links

- [Azure Portal - App Registration](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps)
- [Microsoft Graph API Docs](https://learn.microsoft.com/en-us/graph/overview)
- [MSAL iOS Docs](https://learn.microsoft.com/en-us/entra/msal/objc/)

## Setup Checklist

- [x] Azure app registered (Multitenant + Personal accounts)
- [x] Redirect URIs configured (Web + iOS)
- [x] Client secret created (expires 2028)
- [x] API permissions added and admin consent granted
- [x] Supabase Azure provider configured
- [x] Backend environment variables added (Vercel + .env)
- [x] `api/config.py` updated with Microsoft settings
- [x] Database migration created (`20260106000000_microsoft_support.sql`)
- [x] Database migration applied (January 6, 2026)
- [x] Phase 3: Provider abstraction layer created
- [x] Phase 4: Microsoft OAuth implementation (backend ready)
- [x] Auth service updated for multi-provider support
- [x] Phase 9: iOS MSAL integration (PR #47)
- [ ] Phase 5: Microsoft email sync (Outlook)
- [ ] Phase 6: Microsoft calendar sync
- [ ] Phase 7: Microsoft webhooks

## Database Schema Changes

Migration file: `supabase/migrations/20260106000000_microsoft_support.sql`

### New Columns

| Table | Column | Type | Purpose |
|-------|--------|------|---------|
| `ext_connections` | `delta_link` | TEXT | Microsoft Graph deltaLink for incremental sync |
| `push_subscriptions` | `client_state` | TEXT | Microsoft webhook validation secret |

### Schema Comparison (Google vs Microsoft)

| Column | Google Usage | Microsoft Usage |
|--------|--------------|-----------------|
| `ext_connections.delta_link` | NULL | Stores `@odata.deltaLink` |
| `push_subscriptions.history_id` | Gmail history position | NULL |
| `push_subscriptions.sync_token` | Calendar sync token | NULL |
| `push_subscriptions.client_state` | NULL | Webhook validation secret |

## Phase 3: Provider Abstraction Layer

New files created for multi-provider support:

### Protocol Definitions
- `api/services/auth_protocols.py` - Abstract interfaces (OAuthProvider, EmailSyncProvider, CalendarSyncProvider, WebhookProvider)
- `api/services/provider_factory.py` - Factory to route to correct provider implementation

### Google Provider Wrappers
```
api/services/google/
├── __init__.py
├── google_oauth_provider.py        # Wraps existing auth.py
├── google_email_sync_provider.py   # Wraps existing sync_gmail.py
├── google_calendar_sync_provider.py
└── google_webhook_provider.py      # Wraps existing watch_manager.py
```

### Microsoft Provider Implementations
```
api/services/microsoft/
├── __init__.py
├── microsoft_oauth_provider.py     # Full implementation
├── microsoft_email_sync_provider.py  # Stub (Phase 5)
├── microsoft_calendar_sync_provider.py  # Stub (Phase 6)
└── microsoft_webhook_provider.py   # Stub (Phase 7)
```

### Usage Example
```python
from api.services.provider_factory import ProviderFactory

# Get provider based on connection type
oauth = ProviderFactory.get_oauth_provider("microsoft")
tokens = oauth.exchange_auth_code(code, redirect_uri)
user_info = oauth.get_user_info(tokens["access_token"])

# Refresh token (IMPORTANT: Microsoft returns NEW refresh token!)
new_tokens = oauth.refresh_access_token(connection_data)
# Must save new_tokens["refresh_token"] to database!
```

---

*Created: January 6, 2026*
*Updated: January 6, 2026 - Phase 3 complete*
