-- Ensure each connection/provider pair has at most one active push subscription.
-- Deduplicate existing active rows first so the unique index can be created safely.

WITH ranked_active_subscriptions AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY ext_connection_id, provider
            ORDER BY updated_at DESC, created_at DESC, id DESC
        ) AS row_num
    FROM public.push_subscriptions
    WHERE is_active = true
      AND ext_connection_id IS NOT NULL
      AND provider IS NOT NULL
)
UPDATE public.push_subscriptions AS push_subscriptions
SET
    is_active = false,
    updated_at = now()
FROM ranked_active_subscriptions
WHERE push_subscriptions.id = ranked_active_subscriptions.id
  AND ranked_active_subscriptions.row_num > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_push_subscriptions_active_connection_provider
ON public.push_subscriptions USING btree (ext_connection_id, provider)
WHERE is_active = true
  AND ext_connection_id IS NOT NULL
  AND provider IS NOT NULL;
