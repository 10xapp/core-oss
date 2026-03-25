-- Migration: Add welcome note creation to default workspace signup trigger
-- When a new user signs up, their auto-created "Personal" workspace now gets
-- a "Welcome to Core" note in the files app.

CREATE OR REPLACE FUNCTION "public"."create_default_workspace_for_user"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_workspace_id UUID;
    v_files_app_id UUID;
BEGIN
    -- Create the public.users row from auth metadata
    INSERT INTO public.users (id, email, name, avatar_url)
    VALUES (
        NEW.id,
        NEW.email,
        COALESCE(NEW.raw_user_meta_data->>'full_name', NEW.raw_user_meta_data->>'name'),
        NEW.raw_user_meta_data->>'avatar_url'
    )
    ON CONFLICT (id) DO NOTHING;

    -- Create the default workspace
    INSERT INTO public.workspaces (name, owner_id, is_default)
    VALUES ('Personal', NEW.id, TRUE)
    RETURNING id INTO v_workspace_id;

    -- Add user as owner
    INSERT INTO public.workspace_members (workspace_id, user_id, role)
    VALUES (v_workspace_id, NEW.id, 'owner');

    -- Create default apps
    INSERT INTO public.workspace_apps (workspace_id, app_type, is_public, position)
    VALUES
        (v_workspace_id, 'chat', TRUE, 0),
        (v_workspace_id, 'messages', TRUE, 1),
        (v_workspace_id, 'projects', TRUE, 2),
        (v_workspace_id, 'files', TRUE, 3),
        (v_workspace_id, 'email', TRUE, 4),
        (v_workspace_id, 'calendar', TRUE, 5);

    -- Get the files app ID
    SELECT id INTO v_files_app_id
    FROM public.workspace_apps
    WHERE workspace_id = v_workspace_id AND app_type = 'files';

    -- Create welcome note
    IF v_files_app_id IS NOT NULL THEN
        INSERT INTO public.documents (
            user_id, workspace_app_id, workspace_id,
            title, content, icon, type, position, tags
        )
        VALUES (
            NEW.id, v_files_app_id, v_workspace_id,
            'Welcome to Core!',
            E'## What is Core?\n\nCore is an all-in-one productivity workspace that brings together your email, calendar, projects, files, and team messaging — all powered by an AI agent that helps you get things done faster.\n\nThink of it as your unified hub for work — where everything connects and your AI assistant (the Core Agent) understands your full context.\n\n## Core Features\n\n- **Messaging** — Team channels for real-time discussions\n- **Projects Board** — Organize and track your work with a visual project board\n- **Files** — Upload, manage, and organize your files — including docs and notes that live right inside your file system\n- **Email** — Search, read, and send emails right from Core\n- **Calendar** — View, create, and manage your events\n- **Core Agent** — An AI assistant that can search across all your data, answer questions, and take actions on your behalf\n\n## Getting Started\n\n1. **Set up your workspace** — Create or join a workspace to get started\n2. **Connect your accounts** — Link your email and calendar for the full experience\n3. **Explore your files** — Create docs, upload files, and keep everything organized in one place\n4. **Try the AI agent** — Ask it anything about your emails, calendar, projects, or files\n5. **Invite your team** — Add teammates to collaborate in channels and shared workspaces\n\n## Platforms\n\n- **Web app** — Available now\n- **Desktop app** — Coming soon\n- **Mobile app** — Coming soon\n\n## Need Help?\n\nThe Core Agent is always here to help! Just ask it questions about your workspace, find information, or get things done.\n\nWelcome aboard — let''s build something great together!',
            '👋', 'note', 0, ARRAY[]::TEXT[]
        );
    END IF;

    RETURN NEW;
END;
$$;
