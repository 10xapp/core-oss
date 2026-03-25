// Re-export types from stores for convenience
export type { MiniApp, Workspace } from '../stores/workspaceStore';

// API app types (from backend)
export type MiniAppType = 'files' | 'dashboard' | 'projects' | 'agents';

// Available mini apps for UI
export const AVAILABLE_MINI_APPS: { type: MiniAppType; name: string; icon: string }[] = [
  { type: 'files', name: 'Files', icon: 'FolderOpen' },
  { type: 'dashboard', name: 'Personal', icon: 'SquaresFour' },
  { type: 'projects', name: 'Projects', icon: 'Briefcase' },
  { type: 'agents', name: 'Agents', icon: 'Robot' },
];
