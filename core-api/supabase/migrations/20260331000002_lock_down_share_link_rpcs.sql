-- Lock down share-link RPC exposure by removing unauthenticated execute
-- and trimming direct RPC payloads to the fields current callers need.

CREATE OR REPLACE FUNCTION "public"."validate_share_link"("p_link_token" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
  v_link RECORD;
BEGIN
  -- 1) Token lookup has precedence to avoid namespace ambiguity.
  SELECT * INTO v_link
  FROM public.permissions
  WHERE link_token = p_link_token
    AND grantee_type = 'link';

  -- 2) Fallback to slug lookup (case-insensitive).
  IF NOT FOUND THEN
    SELECT * INTO v_link
    FROM public.permissions
    WHERE lower(link_slug) = lower(p_link_token)
      AND grantee_type = 'link';
  END IF;

  IF NOT FOUND THEN
    RETURN NULL;
  END IF;

  -- Treat expired links as not found.
  IF v_link.expires_at IS NOT NULL AND v_link.expires_at < now() THEN
    RETURN NULL;
  END IF;

  RETURN jsonb_build_object(
    'resource_type', v_link.resource_type,
    'permission', v_link.permission
  );
END;
$$;

ALTER FUNCTION "public"."validate_share_link"("p_link_token" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."resolve_share_link_grant"("p_link_token" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
  v_link RECORD;
  v_user_id UUID;
  v_effective_permission TEXT;
BEGIN
  v_user_id := auth.uid();
  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Not authenticated';
  END IF;

  -- 1) Token lookup has precedence to avoid namespace ambiguity.
  SELECT * INTO v_link
  FROM public.permissions
  WHERE link_token = p_link_token
    AND grantee_type = 'link';

  -- 2) Fallback to slug lookup (case-insensitive).
  IF NOT FOUND THEN
    SELECT * INTO v_link
    FROM public.permissions
    WHERE lower(link_slug) = lower(p_link_token)
      AND grantee_type = 'link';
  END IF;

  IF NOT FOUND THEN
    RETURN NULL;
  END IF;

  -- Treat expired links as not found.
  IF v_link.expires_at IS NOT NULL AND v_link.expires_at < now() THEN
    RETURN NULL;
  END IF;

  -- If caller is the link creator, skip granting — just return info.
  IF v_user_id = v_link.granted_by THEN
    RETURN jsonb_build_object(
      'resource_type', v_link.resource_type,
      'resource_id', v_link.resource_id,
      'permission', v_link.permission
    );
  END IF;

  -- Grant or upgrade permission (never downgrade).
  INSERT INTO public.permissions (
    workspace_id, resource_type, resource_id,
    grantee_type, grantee_id, permission, granted_by
  ) VALUES (
    v_link.workspace_id, v_link.resource_type, v_link.resource_id,
    'user', v_user_id, v_link.permission, v_link.granted_by
  )
  ON CONFLICT (resource_type, resource_id, grantee_id)
  DO UPDATE SET
    permission = CASE
      WHEN public.permissions.expires_at IS NOT NULL AND public.permissions.expires_at <= now()
        THEN EXCLUDED.permission
      WHEN (CASE EXCLUDED.permission WHEN 'read' THEN 1 WHEN 'write' THEN 2 WHEN 'admin' THEN 3 ELSE 0 END) >
           (CASE public.permissions.permission WHEN 'read' THEN 1 WHEN 'write' THEN 2 WHEN 'admin' THEN 3 ELSE 0 END)
        THEN EXCLUDED.permission
      ELSE public.permissions.permission
    END,
    expires_at = CASE
      WHEN public.permissions.expires_at IS NOT NULL AND public.permissions.expires_at <= now() THEN NULL
      ELSE public.permissions.expires_at
    END
  RETURNING permission INTO v_effective_permission;

  RETURN jsonb_build_object(
    'resource_type', v_link.resource_type,
    'resource_id', v_link.resource_id,
    'permission', v_effective_permission
  );
END;
$$;

ALTER FUNCTION "public"."resolve_share_link_grant"("p_link_token" "text") OWNER TO "postgres";


REVOKE ALL ON FUNCTION "public"."validate_share_link"("p_link_token" "text") FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."validate_share_link"("p_link_token" "text") FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."validate_share_link"("p_link_token" "text") TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."validate_share_link"("p_link_token" "text") TO "service_role";

REVOKE ALL ON FUNCTION "public"."resolve_share_link_grant"("p_link_token" "text") FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."resolve_share_link_grant"("p_link_token" "text") FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."resolve_share_link_grant"("p_link_token" "text") TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."resolve_share_link_grant"("p_link_token" "text") TO "service_role";
