/**
 * Query Key Factories
 *
 * Type-safe query keys for TanStack Query.
 * Using factories ensures consistent keys across the app and enables
 * precise cache invalidation.
 */

// Todos
export const todoKeys = {
  all: ['todos'] as const,
  lists: () => [...todoKeys.all, 'list'] as const,
  list: (workspaceAppId: string, tab: 'all' | 'today' | 'habits') =>
    [...todoKeys.lists(), workspaceAppId, tab] as const,
  detail: (id: string) => [...todoKeys.all, 'detail', id] as const,
};

// Calendar
export const calendarKeys = {
  all: ['calendar'] as const,
  events: () => [...calendarKeys.all, 'events'] as const,
  eventsByRange: (startDate: string, endDate: string) =>
    [...calendarKeys.events(), startDate, endDate] as const,
  detail: (id: string) => [...calendarKeys.all, 'detail', id] as const,
};

// Email
export const emailKeys = {
  all: ['emails'] as const,
  folders: () => [...emailKeys.all, 'folders'] as const,
  folder: (folder: string, accountIds?: string[]) =>
    [...emailKeys.folders(), folder, accountIds ? [...accountIds].sort().join(',') : 'all'] as const,
  counts: (accountIds?: string[]) =>
    [...emailKeys.all, 'counts', accountIds ? [...accountIds].sort().join(',') : 'all'] as const,
  detail: (id: string) => [...emailKeys.all, 'detail', id] as const,
  thread: (threadId: string) => [...emailKeys.all, 'thread', threadId] as const,
};

// Messages
export const messageKeys = {
  all: ['messages'] as const,
  channels: (workspaceAppId: string) =>
    [...messageKeys.all, 'channels', workspaceAppId] as const,
  dms: (workspaceAppId: string) =>
    [...messageKeys.all, 'dms', workspaceAppId] as const,
  channel: (channelId: string) =>
    [...messageKeys.all, 'channel', channelId] as const,
  thread: (messageId: string) =>
    [...messageKeys.all, 'thread', messageId] as const,
};

// Projects
export const projectKeys = {
  all: ['projects'] as const,
  boards: (workspaceAppId: string) =>
    [...projectKeys.all, 'boards', workspaceAppId] as const,
  board: (boardId: string) =>
    [...projectKeys.all, 'board', boardId] as const,
  boardData: (boardId: string) =>
    [...projectKeys.all, 'boardData', boardId] as const,
  states: (boardId: string) =>
    [...projectKeys.all, 'states', boardId] as const,
  issues: (boardId: string) =>
    [...projectKeys.all, 'issues', boardId] as const,
  labels: (boardId: string) =>
    [...projectKeys.all, 'labels', boardId] as const,
  comments: (issueId: string) =>
    [...projectKeys.all, 'comments', issueId] as const,
  members: (workspaceId: string) =>
    [...projectKeys.all, 'members', workspaceId] as const,
};

// Files
export const fileKeys = {
  all: ['files'] as const,
  documents: (workspaceAppId: string, folderId?: string) =>
    [...fileKeys.all, 'documents', workspaceAppId, folderId ?? 'root'] as const,
  document: (id: string) => [...fileKeys.all, 'document', id] as const,
};

// Conversations (Chat)
export const conversationKeys = {
  all: ['conversations'] as const,
  list: (workspaceId?: string) =>
    [...conversationKeys.all, 'list', workspaceId ?? 'personal'] as const,
  detail: (id: string) => [...conversationKeys.all, 'detail', id] as const,
  messages: (conversationId: string) =>
    [...conversationKeys.all, 'messages', conversationId] as const,
};
