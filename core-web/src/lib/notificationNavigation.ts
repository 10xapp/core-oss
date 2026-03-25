import type { NavigateFunction } from 'react-router-dom';
import type { Notification } from '../stores/notificationStore';
import { useWorkspaceStore } from '../stores/workspaceStore';

const FILE_LIKE_RESOURCE_TYPES = new Set(['document', 'file', 'folder']);

export function navigateFromNotification(
  notification: Notification,
  navigate: NavigateFunction,
): void {
  const workspaceId = typeof notification.data?.workspace_id === 'string'
    ? notification.data.workspace_id
    : notification.workspace_id;

  const workspaceStore = useWorkspaceStore.getState();
  const workspace = workspaceId
    ? workspaceStore.workspaces.find((ws) => ws.id === workspaceId)
    : undefined;
  if (workspaceId && workspace) {
    workspaceStore.setActiveWorkspace(workspace.id);
  }

  const resourceType = notification.resource_type;
  if (notification.type === 'calendar_invite' || resourceType === 'calendar_event') {
    const eventId = typeof notification.data?.event_id === 'string'
      ? notification.data.event_id
      : notification.resource_id;
    const startsAt = typeof notification.data?.starts_at === 'string'
      ? notification.data.starts_at
      : null;
    const accountEmail = typeof notification.data?.account_email === 'string'
      ? notification.data.account_email
      : null;

    const params = new URLSearchParams();
    if (eventId) params.set('event_id', eventId);
    if (startsAt) params.set('date', startsAt);
    if (accountEmail) params.set('account_email', accountEmail);
    params.set('focus', String(Date.now()));

    const calendarPath = workspaceId
      ? `/workspace/${workspaceId}/calendar`
      : '/calendar';
    const target = params.size > 0 ? `${calendarPath}?${params.toString()}` : calendarPath;
    navigate(target);
    return;
  }

  if (!workspaceId) return;

  if (
    notification.type === 'file_shared'
    || notification.type === 'file_edited'
    || (resourceType ? FILE_LIKE_RESOURCE_TYPES.has(resourceType) : false)
  ) {
    const targetDocumentId = typeof notification.data?.document_id === 'string'
      ? notification.data.document_id
      : resourceType === 'document' || resourceType === 'folder'
        ? notification.resource_id
        : null;

    if (targetDocumentId) {
      navigate(`/workspace/${workspaceId}/files/${targetDocumentId}`);
      return;
    }

    navigate(`/workspace/${workspaceId}/files`);
    return;
  }

  if (resourceType === 'issue') {
    const boardId = notification.data?.board_id;
    const projectsApp = workspace?.apps?.find((app) => app.type === 'projects');
    if (projectsApp && boardId) {
      navigate(`/workspace/${workspaceId}/projects`);
    }
  }
}
