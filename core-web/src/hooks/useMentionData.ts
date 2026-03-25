import { useCallback, useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useWorkspaceStore } from '../stores/workspaceStore';
import { useFilesStore } from '../stores/filesStore';
import { useMessagesStore } from '../stores/messagesStore';
import { useProjectMembers, useProjectBoards } from './queries/useProjects';
import { todoKeys } from './queries/keys';
import type { MentionMenuItem, MentionMenuLevel } from '../types/mention';
import type { Document, Channel, Todo } from '../api/client';

const APP_EMOJIS: Record<string, string> = {
  files: '📁',
  projects: '📋',
  messages: '💬',
  tasks: '✓',
};

// App types that support @ mention drill-down
const MENTIONABLE_APP_TYPES = ['files', 'projects', 'messages', 'tasks'];

export function useMentionData(workspaceId: string | null) {
  const workspace = useWorkspaceStore((s) => s.workspaces.find((w) => w.id === workspaceId));
  const activeDocsByFolder = useFilesStore((s) => s.documentsByFolder);
  const workspaceDocCache = useFilesStore((s) => s.workspaceDocCache);

  // Use active docs if available, otherwise check workspace cache for this workspace's files app
  const filesApp = (workspace?.apps || []).find((a) => a.type === 'files');
  const cachedDocs = filesApp ? workspaceDocCache[filesApp.id]?.documentsByFolder : null;
  const documentsByFolder = Object.keys(activeDocsByFolder).length > 0 ? activeDocsByFolder : (cachedDocs || {});
  const channels = useMessagesStore((s) => s.channels);
  const queryClient = useQueryClient();
  const { data: members } = useProjectMembers(workspaceId);

  // Reactively subscribe to project boards so they're fetched if not cached
  const projectsApp = (workspace?.apps || []).find((a) => a.type === 'projects');
  const { data: boards } = useProjectBoards(projectsApp?.id ?? null);

  // Ensure files data is loaded when mention hook is used
  const hasFileData = Object.keys(documentsByFolder).length > 0;
  useEffect(() => {
    if (filesApp && !hasFileData) {
      useFilesStore.getState().preloadBackground(filesApp.id);
    }
  }, [filesApp, hasFileData]);

  const getRootLevel = useCallback((): MentionMenuLevel => {
    const apps = (workspace?.apps || []).filter((a) => MENTIONABLE_APP_TYPES.includes(a.type));

    const appItems: MentionMenuItem[] = apps.map((app) => ({
      id: app.id,
      entityType: 'folder' as const,
      displayName: app.name,
      icon: APP_EMOJIS[app.type] || '•',
      hasChildren: true,
      appId: app.id,
      appType: app.type,
    }));

    const peopleItems: MentionMenuItem[] = (members || []).map((m) => ({
      id: m.user_id,
      entityType: 'person' as const,
      displayName: m.name || m.email || 'Unknown',
      avatarUrl: m.avatar_url,
      subtitle: m.email,
    }));

    return {
      title: 'Mention',
      items: [...appItems, ...peopleItems],
    };
  }, [workspace?.apps, members]);

  const getDrillDown = useCallback(
    (item: MentionMenuItem): MentionMenuLevel | null => {
      const appType = item.appType;
      const appId = item.appId;

      if (!appType || !appId) return null;

      switch (appType) {
        case 'files': {
          const rootDocs = documentsByFolder['__root__'] || documentsByFolder[appId] || [];
          const allDocs = Object.values(documentsByFolder).flat();
          // Use root docs if available, fall back to all docs
          const docs = rootDocs.length > 0 ? rootDocs : allDocs;
          return {
            title: item.displayName,
            items: docs.map((doc: Document) => ({
              id: doc.id,
              entityType: doc.is_folder || doc.type === 'folder' ? ('folder' as const) : ('file' as const),
              displayName: doc.title || 'Untitled',
              icon: doc.is_folder || doc.type === 'folder' ? '📁' : '📄',
              hasChildren: doc.is_folder || doc.type === 'folder',
              appId,
              appType: 'files',
            })),
          };
        }

        case 'projects': {
          return {
            title: item.displayName,
            items: (boards || []).map((board) => ({
              id: board.id,
              entityType: 'project_board' as const,
              displayName: board.name,
              icon: board.icon || '📋',
              subtitle: board.key,
            })),
          };
        }

        case 'messages': {
          return {
            title: item.displayName,
            items: channels.map((ch: Channel) => ({
              id: ch.id,
              entityType: 'channel' as const,
              displayName: ch.name,
              icon: ch.is_private ? '🔒' : '💬',
            })),
          };
        }

        case 'tasks': {
          const todos =
            queryClient.getQueryData<Todo[]>(todoKeys.list(appId, 'all')) || [];
          return {
            title: item.displayName,
            items: todos
              .filter((t) => !t.is_completed)
              .slice(0, 30)
              .map((todo) => ({
                id: todo.id,
                entityType: 'todo' as const,
                displayName: todo.title,
                icon: '✓',
              })),
          };
        }

        default:
          return null;
      }
    },
    [documentsByFolder, channels, boards, queryClient],
  );

  const getDrillDownForFolder = useCallback(
    (folderId: string, folderName: string, appId?: string): MentionMenuLevel | null => {
      const children = documentsByFolder[folderId] || [];
      return {
        title: folderName,
        items: children.map((doc: Document) => ({
          id: doc.id,
          entityType: doc.is_folder || doc.type === 'folder' ? ('folder' as const) : ('file' as const),
          displayName: doc.title || 'Untitled',
          icon: doc.is_folder || doc.type === 'folder' ? '📁' : '📄',
          hasChildren: doc.is_folder || doc.type === 'folder',
          appId,
          appType: 'files',
        })),
      };
    },
    [documentsByFolder],
  );

  const filterLevel = useCallback(
    (level: MentionMenuLevel, query: string): MentionMenuItem[] => {
      if (!query) return level.items;
      const q = query.toLowerCase();
      const filtered = level.items.filter(
        (item) =>
          item.displayName.toLowerCase().includes(q) ||
          (item.subtitle && item.subtitle.toLowerCase().includes(q)),
      );

      // At root level, also search across all files for direct matches
      const hasAppItems = level.items.some((i) => i.hasChildren);
      if (hasAppItems && q.length >= 1) {
        const allDocs = Object.values(documentsByFolder).flat();
        const seen = new Set<string>();
        const matchingDocs = allDocs.filter((doc) => {
          if (doc.is_folder || doc.type === 'folder') return false;
          if (seen.has(doc.id)) return false;
          seen.add(doc.id);
          return (doc.title || '').toLowerCase().includes(q);
        }).slice(0, 8);

        if (matchingDocs.length > 0) {
          const fileItems: MentionMenuItem[] = matchingDocs.map((doc) => ({
            id: doc.id,
            entityType: 'file' as const,
            displayName: doc.title || 'Untitled',
            icon: '📄',
          }));

          const appResults = filtered.filter((i) => i.hasChildren);
          const peopleResults = filtered.filter((i) => i.entityType === 'person');
          return [...appResults, ...fileItems, ...peopleResults];
        }
      }

      return filtered;
    },
    [documentsByFolder],
  );

  return {
    getRootLevel,
    getDrillDown,
    getDrillDownForFolder,
    filterLevel,
  };
}
