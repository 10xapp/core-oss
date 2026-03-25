-- Rename default "Dashboard" workspaces to "Personal"
UPDATE public.workspaces
SET name = 'Personal'
WHERE is_default = TRUE AND name = 'Dashboard';
