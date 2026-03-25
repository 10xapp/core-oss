import { useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { supabase } from '../lib/supabase';
import { useAuthStore } from '../stores/authStore';
import { useWorkspaceStore } from '../stores/workspaceStore';
import { useMessagesStore, pendingReactions } from '../stores/messagesStore';
import { useFilesStore, updateNoteVersionToken } from '../stores/filesStore';
import { useNotificationStore } from '../stores/notificationStore';
import { usePermissionStore } from '../stores/permissionStore';
import { getChannelMessage, getChannel } from '../api/client';
import { playMessageNotification } from '../lib/notificationSound';
import { showMessageNotification } from '../lib/messageNotification';
import { getCachedChannelMetadata } from '../lib/messageChannelMetadata';
import { showNotificationToast } from '../lib/notificationToast';
import { shouldSuppressDocumentRealtime } from '../lib/documentRealtimeGuard';
import {
  markRealtimeEvent,
  noteVisible,
  shouldResubscribeOnResume,
  markResubscribeAttempt,
  revalidateActiveData,
} from '../lib/revalidation';
import { projectKeys } from './queries/keys';
import { mergeBlocksPreservingFileUrls } from '../lib/blockMerge';
import type { RealtimeChannel } from '@supabase/supabase-js';

// Module-level deduplication set — survives StrictMode remounts and prevents double notifications
const notifiedMessageIds = new Set<string>();

// Module-level storage for notification message - MessagesView will read this
let _pendingNotificationMessage: { channelId: string; message: any } | null = null;

export function getPendingNotificationMessage() {
  return _pendingNotificationMessage;
}

export function clearPendingNotificationMessage() {
  _pendingNotificationMessage = null;
}

function setPendingNotificationMessage(channelId: string, message: any) {
  _pendingNotificationMessage = { channelId, message };
}

/**
 * Unified global realtime hook — single Supabase channel for ALL tables.
 * Replaces useGlobalMessagesNotification, useMessagesRealtime,
 * useReactionsRealtime, and useChannelsRealtime.
 *
 * Listens to:
 * - channel_messages (INSERT/UPDATE/DELETE) → live messages + notifications
 * - message_reactions (INSERT/DELETE) → live reaction updates
 * - channels (INSERT/UPDATE/DELETE) → live channel list updates
 * - documents (INSERT/UPDATE/DELETE) → live file/folder updates
 *
 * The subscription only tears down on login/logout (currentUserId change).
 * All other dependencies are accessed via refs to avoid reconnects.
 */
export function useGlobalRealtime(enabled = true) {
  const subscriptionRef = useRef<RealtimeChannel | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const hasSubscribedOnceRef = useRef(false);

  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const currentUserId = useAuthStore((s) => s.user?.id);
  const workspaces = useWorkspaceStore((s) => s.workspaces);

  // Stable refs — avoids re-subscribing on navigation or store updates
  const navigateRef = useRef(navigate);
  const queryClientRef = useRef(queryClient);
  const workspacesRef = useRef(workspaces);
  const currentUserIdRef = useRef(currentUserId);

  navigateRef.current = navigate;
  queryClientRef.current = queryClient;
  workspacesRef.current = workspaces;
  currentUserIdRef.current = currentUserId;

  useEffect(() => {
    if (!enabled || !currentUserId) {
      if (subscriptionRef.current) {
        subscriptionRef.current.unsubscribe();
        subscriptionRef.current = null;
      }
      return;
    }

    console.log('[GlobalRealtime] Setting up unified realtime subscription');

    // Batch reorder triggers N UPDATE events — coalesce into one fetchDocuments call.
    let documentDebounceTimer: ReturnType<typeof setTimeout> | null = null;
    const debouncedFetchDocuments = () => {
      if (documentDebounceTimer) clearTimeout(documentDebounceTimer);
      documentDebounceTimer = setTimeout(() => {
        documentDebounceTimer = null;
        const filesState = useFilesStore.getState();
        if (filesState.workspaceAppId) {
          filesState.fetchDocuments(filesState.currentFolderId);
        }
      }, 500);
    };

    const commentIssueMap = new Map<string, string>();
    const findIssueIdForComment = (commentId: string): string | null => {
      const mappedIssueId = commentIssueMap.get(commentId);
      if (mappedIssueId) return mappedIssueId;

      const cachedCommentQueries = queryClientRef.current.getQueriesData<
        Array<{ id?: string; issue_id?: string }>
      >({
        queryKey: projectKeys.all,
        predicate: (query) => query.queryKey[1] === 'comments',
      });

      for (const [queryKey, comments] of cachedCommentQueries) {
        if (!Array.isArray(queryKey) || queryKey[1] !== 'comments') continue;
        const issueId = typeof queryKey[2] === 'string' ? queryKey[2] : null;
        if (!issueId || !Array.isArray(comments)) continue;

        const hasComment = comments.some((comment) => comment?.id === commentId);
        if (hasComment) {
          commentIssueMap.set(commentId, issueId);
          return issueId;
        }
      }

      return null;
    };

    const recordRealtimeEvent = () => {
      markRealtimeEvent();
    };

    const channel = supabase
      .channel('global-realtime')

      // ================================================================
      // channel_messages INSERT → notifications + live message list
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'channel_messages',
        },
        async (payload) => {
          recordRealtimeEvent();
          const newMessage = payload.new as any;
          const userId = currentUserIdRef.current;

          // Skip own messages (optimistic updates handle them)
          if (newMessage.user_id === userId) return;

          if (newMessage.thread_parent_id) {
            // --- Thread reply ---
            // Deduplicate BEFORE async calls to prevent race conditions
            if (notifiedMessageIds.has(newMessage.id)) return;
            notifiedMessageIds.add(newMessage.id);
            setTimeout(() => notifiedMessageIds.delete(newMessage.id), 30000);

            // Play sound immediately (before async calls)
            playMessageNotification();

            try {
              const result = await getChannelMessage(newMessage.id);
              const message = result.message;

              // Add thread reply to state - don't increment reply_count here
              // The DB trigger handles the count, and the UPDATE event will sync it
              useMessagesStore.setState((state) => ({
                threadReplies: [...state.threadReplies, message],
              }));

              // Visual notification (sound already played above)
              showMessageNotification({
                senderName: message.user?.name || message.user?.email || 'Someone',
                senderAvatar: message.user?.avatar_url,
                content: message.content || '',
                isThreadReply: true,
              });

              // Update thread participants cache
              if (message.user) {
                const messageUser = message.user;
                useMessagesStore.setState((state) => {
                  const currentParticipants = state.threadParticipants[newMessage.thread_parent_id] || [];
                  if (currentParticipants.some((p) => p.id === messageUser.id)) return state;
                  return {
                    threadParticipants: {
                      ...state.threadParticipants,
                      [newMessage.thread_parent_id]: [
                        ...currentParticipants,
                        {
                          id: messageUser.id,
                          avatar_url: messageUser.avatar_url,
                          name: messageUser.name,
                          email: messageUser.email,
                        },
                      ].slice(0, 4),
                    },
                  };
                });
              }
            } catch (err) {
              console.error('[GlobalRealtime] Failed to fetch thread reply:', err);
              showMessageNotification({
                senderName: 'Someone',
                content: typeof newMessage.content === 'string' ? newMessage.content : '',
                isThreadReply: true,
              });
            }
          } else {
            // --- Main message ---
            // Deduplicate
            if (notifiedMessageIds.has(newMessage.id)) return;
            notifiedMessageIds.add(newMessage.id);
            setTimeout(() => notifiedMessageIds.delete(newMessage.id), 30000);

            // Play sound immediately (before async calls to prevent race conditions)
            const storeState = useMessagesStore.getState();
            const isViewingMessages = window.location.pathname.includes('/messages');
            const isActiveChannelAtSnapshot = isViewingMessages && newMessage.channel_id === storeState.activeChannelId;
            if (!isActiveChannelAtSnapshot) {
              playMessageNotification();
            }

            // 1) Add message to store (independent of notification)
            try {
              const messageResult = await getChannelMessage(newMessage.id);
              const message = messageResult.message;

              useMessagesStore.setState((state) => {
                if (state.messages.some((m) => m.id === newMessage.id)) return state;

                const isViewingMessagesNow = window.location.pathname.includes('/messages');
                const isActiveChannelNow = isViewingMessagesNow && newMessage.channel_id === state.activeChannelId;

                // Only add to active messages if this message belongs to the active channel
                const updatedMessages = isActiveChannelNow
                  ? [...state.messages, message]
                  : state.messages;

                // Always update the cache for the correct channel
                const cachedMessages = state.messagesCache[newMessage.channel_id] || [];
                const alreadyCached = cachedMessages.some((m) => m.id === newMessage.id);

                return {
                  ...state,
                  messages: updatedMessages,
                  unreadCounts: isActiveChannelNow
                    ? state.unreadCounts
                    : {
                        ...state.unreadCounts,
                        [newMessage.channel_id]: (state.unreadCounts[newMessage.channel_id] || 0) + 1,
                      },
                  messagesCache: alreadyCached
                    ? state.messagesCache
                    : {
                        ...state.messagesCache,
                        [newMessage.channel_id]: [...cachedMessages, message],
                      },
                };
              });

              // 2) Notification (can fail without affecting message display)
              const cachedChannel = getCachedChannelMetadata(newMessage.channel_id);
              let channelName = cachedChannel?.channelName;
              let workspaceAppId = cachedChannel?.workspaceAppId;

              try {
                const channelResult = await getChannel(newMessage.channel_id);
                const ch = channelResult.channel;
                channelName = ch.name;
                workspaceAppId = ch.workspace_app_id;
              } catch (notifErr) {
                // Notification failed but message is already in the store
                console.error('[GlobalRealtime] Notification failed:', notifErr);
              }

              const workspace = workspaceAppId
                ? workspacesRef.current.find((ws) =>
                    ws.apps?.some((app) => app.id === workspaceAppId)
                  )
                : undefined;

              // Sound already played above (before async calls)
              showMessageNotification({
                senderName: message.user?.name || message.user?.email || 'Someone',
                senderAvatar: message.user?.avatar_url,
                content: message.content || '',
                channelName,
                onClick: workspace
                  ? () => {
                      // Store the message for MessagesView to read after navigation
                      setPendingNotificationMessage(newMessage.channel_id, message);

                      // Navigate to the channel
                      navigateRef.current(`/workspace/${workspace.id}/messages/${newMessage.channel_id}`);
                    }
                  : undefined,
              });
            } catch (err) {
              console.error('[GlobalRealtime] Failed to fetch message:', err);
              const cachedChannel = getCachedChannelMetadata(newMessage.channel_id);
              const workspace = cachedChannel?.workspaceAppId
                ? workspacesRef.current.find((ws) =>
                    ws.apps?.some((app) => app.id === cachedChannel.workspaceAppId)
                  )
                : undefined;

              showMessageNotification({
                senderName: 'Someone',
                content: typeof newMessage.content === 'string' ? newMessage.content : '',
                channelName: cachedChannel?.channelName,
                onClick: workspace
                  ? () => {
                      navigateRef.current(`/workspace/${workspace.id}/messages/${newMessage.channel_id}`);
                    }
                  : undefined,
              });
            }
          }
        }
      )

      // ================================================================
      // channel_messages UPDATE → live edits
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'channel_messages',
        },
        (payload) => {
          recordRealtimeEvent();
          const updated = payload.new as any;
          useMessagesStore.setState((state) => {
            const mapMsg = (m: any) => {
              if (m.id !== updated.id) return m;
              const mergedBlocks = mergeBlocksPreservingFileUrls(m.blocks, updated.blocks);
              return {
                ...m,
                ...updated,
                blocks: Array.isArray(mergedBlocks) ? mergedBlocks : m.blocks,
              };
            };

            // Update cache for the message's channel
            const channelId = updated.channel_id;
            const cachedMessages = state.messagesCache[channelId];
            const updatedCache = cachedMessages
              ? { ...state.messagesCache, [channelId]: cachedMessages.map(mapMsg) }
              : state.messagesCache;

            return {
              messages: state.messages.map(mapMsg),
              threadReplies: state.threadReplies.map(mapMsg),
              messagesCache: updatedCache,
            };
          });
        }
      )

      // ================================================================
      // channel_messages DELETE → live removal
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'DELETE',
          schema: 'public',
          table: 'channel_messages',
        },
        (payload) => {
          recordRealtimeEvent();
          const deleted = payload.old as any;
          useMessagesStore.setState((state) => {
            // Update cache for the message's channel
            const channelId = deleted.channel_id;
            const cachedMessages = state.messagesCache[channelId];
            const updatedCache = cachedMessages
              ? { ...state.messagesCache, [channelId]: cachedMessages.filter((m: any) => m.id !== deleted.id) }
              : state.messagesCache;

            return {
              messages: state.messages.filter((m) => m.id !== deleted.id),
              threadReplies: state.threadReplies.filter((m) => m.id !== deleted.id),
              messagesCache: updatedCache,
            };
          });
        }
      )

      // ================================================================
      // message_reactions INSERT → live reaction add
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'message_reactions',
        },
        (payload) => {
          recordRealtimeEvent();
          const reaction = payload.new as any;

          // Skip own reactions - the store's addReaction already handles them
          // Check both ref and auth store directly for robustness
          const currentUserId = currentUserIdRef.current || useAuthStore.getState().user?.id;
          if (reaction.user_id === currentUserId) return;

          // Skip if there's a pending reaction operation for this message+emoji
          // This prevents race conditions where realtime fires before addReaction completes
          const reactionKey = `${reaction.message_id}:${reaction.emoji}`;
          if (pendingReactions.has(reactionKey)) return;

          useMessagesStore.setState((state) => {
            if (!state.messages.some((m) => m.id === reaction.message_id)) return state;
            return {
              messages: state.messages.map((m) => {
                if (m.id !== reaction.message_id) return m;
                // Check if reaction already exists (dedup)
                const exists = (m.reactions || []).some(
                  (r) => r.id === reaction.id || (r.emoji === reaction.emoji && r.user_id === reaction.user_id)
                );
                if (exists) return m;
                return { ...m, reactions: [...(m.reactions || []), reaction] };
              }),
            };
          });
        }
      )

      // ================================================================
      // message_reactions DELETE → live reaction remove
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'DELETE',
          schema: 'public',
          table: 'message_reactions',
        },
        (payload) => {
          recordRealtimeEvent();
          const deleted = payload.old as any;
          useMessagesStore.setState((state) => ({
            messages: state.messages.map((m) =>
              m.id === deleted.message_id
                ? { ...m, reactions: (m.reactions || []).filter((r) => r.id !== deleted.id) }
                : m
            ),
          }));
        }
      )

      // ================================================================
      // channels INSERT → live channel add
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'channels',
        },
        (payload) => {
          recordRealtimeEvent();
          const ch = payload.new as any;

          // DMs are handled by dedicated DM fetch/state paths.
          if (ch.is_dm || !ch.workspace_app_id) return;

          // Private channels are added via the creator's own addChannel action.
          // Other members pick them up via fetchChannels after being invited.
          if (ch.is_private) return;

          useMessagesStore.setState((state) => {
            const workspaceAppId = String(ch.workspace_app_id);
            const existingCache = state.workspaceCache[workspaceAppId] || {
              channels: [],
              dms: [],
              activeChannelId: null,
              lastFetched: 0,
            };
            const cacheAlreadyHasChannel = (existingCache.channels || []).some((c) => c.id === ch.id);
            const nextCacheChannels = cacheAlreadyHasChannel
              ? existingCache.channels
              : [...(existingCache.channels || []), ch];
            const nextCacheActiveChannelId = existingCache.activeChannelId
              && [...nextCacheChannels, ...(existingCache.dms || [])].some((item) => item.id === existingCache.activeChannelId)
              ? existingCache.activeChannelId
              : nextCacheChannels[0]?.id || existingCache.dms?.[0]?.id || null;
            const nextWorkspaceCache = {
              ...state.workspaceCache,
              [workspaceAppId]: {
                ...existingCache,
                channels: nextCacheChannels,
                activeChannelId: nextCacheActiveChannelId,
                lastFetched: Date.now(),
              },
            };

            if (state.workspaceAppId !== workspaceAppId) {
              return cacheAlreadyHasChannel ? state : { workspaceCache: nextWorkspaceCache };
            }

            if (state.channels.some((c) => c.id === ch.id)) {
              return { workspaceCache: nextWorkspaceCache };
            }

            const nextActiveChannelId = state.activeChannelId || nextCacheActiveChannelId;
            return {
              channels: [...state.channels, ch],
              workspaceCache: nextWorkspaceCache,
              activeChannelId: nextActiveChannelId,
              messages: nextActiveChannelId ? (state.messagesCache[nextActiveChannelId] || []) : state.messages,
            };
          });
        }
      )

      // ================================================================
      // channels UPDATE → live channel edit
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'channels',
        },
        (payload) => {
          recordRealtimeEvent();
          const updated = payload.new as any;
          if (updated.is_dm || !updated.workspace_app_id) return;

          useMessagesStore.setState((state) => {
            const existingCache = state.workspaceCache[updated.workspace_app_id];
            const nextWorkspaceCache = existingCache
              ? {
                  ...state.workspaceCache,
                  [updated.workspace_app_id]: {
                    ...existingCache,
                    channels: existingCache.channels.map((c) =>
                      c.id === updated.id ? { ...c, ...updated } : c
                    ),
                    lastFetched: Date.now(),
                  },
                }
              : state.workspaceCache;

            return {
              channels: state.workspaceAppId === updated.workspace_app_id
                ? state.channels.map((c) => (c.id === updated.id ? { ...c, ...updated } : c))
                : state.channels,
              workspaceCache: nextWorkspaceCache,
            };
          });
        }
      )

      // ================================================================
      // channels DELETE → live channel remove
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'DELETE',
          schema: 'public',
          table: 'channels',
        },
        (payload) => {
          recordRealtimeEvent();
          const deleted = payload.old as any;
          if (deleted.is_dm || !deleted.workspace_app_id) return;

          useMessagesStore.setState((state) => {
            const workspaceAppId = String(deleted.workspace_app_id);
            const existingCache = state.workspaceCache[workspaceAppId];
            const nextCacheChannels = (existingCache?.channels || []).filter((c) => c.id !== deleted.id);
            const nextCacheActiveChannelId = existingCache?.activeChannelId === deleted.id
              ? nextCacheChannels[0]?.id || existingCache?.dms?.[0]?.id || null
              : existingCache?.activeChannelId || null;
            const nextWorkspaceCache = existingCache
              ? {
                  ...state.workspaceCache,
                  [workspaceAppId]: {
                    ...existingCache,
                    channels: nextCacheChannels,
                    activeChannelId: nextCacheActiveChannelId,
                    lastFetched: Date.now(),
                  },
                }
              : state.workspaceCache;

            if (state.workspaceAppId !== workspaceAppId) {
              return { workspaceCache: nextWorkspaceCache };
            }

            const nextChannels = state.channels.filter((c) => c.id !== deleted.id);
            const activeWasDeleted = state.activeChannelId === deleted.id;
            const nextActiveChannelId = activeWasDeleted
              ? (nextChannels[0]?.id || state.dms[0]?.id || null)
              : state.activeChannelId;
            return {
              channels: nextChannels,
              workspaceCache: nextWorkspaceCache,
              visitedChannelIds: state.visitedChannelIds.filter((id) => id !== deleted.id),
              activeChannelId: nextActiveChannelId,
              messages: nextActiveChannelId ? (state.messagesCache[nextActiveChannelId] || (activeWasDeleted ? [] : state.messages)) : [],
            };
          });
        }
      )

      // ================================================================
      // channel_members INSERT → current user added to a channel
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'channel_members',
        },
        (payload) => {
          recordRealtimeEvent();
          const member = payload.new as any;
          if (member.user_id === currentUserIdRef.current) {
            // Current user was added to a channel — refetch to pick up private channels
            const messagesState = useMessagesStore.getState();
            if (messagesState.workspaceAppId) {
              messagesState.fetchChannels(true);
            }
          }
        }
      )

      // ================================================================
      // channel_members DELETE → current user removed from a channel
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'DELETE',
          schema: 'public',
          table: 'channel_members',
        },
        (payload) => {
          recordRealtimeEvent();
          const member = payload.old as any;
          if (member.user_id === currentUserIdRef.current) {
            // Current user was removed — refetch to drop inaccessible channels
            const messagesState = useMessagesStore.getState();
            if (messagesState.workspaceAppId) {
              messagesState.fetchChannels(true);
            }
          }
        }
      )

      // ================================================================
      // documents INSERT/UPDATE/DELETE → invalidate filesStore (debounced)
      // Debounced because batch reorder triggers N UPDATE events.
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'documents',
        },
        (payload) => {
          recordRealtimeEvent();
          const doc = payload.new as any;
          if (shouldSuppressDocumentRealtime(doc.id, doc.workspace_app_id)) return;
          debouncedFetchDocuments();
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'documents',
        },
        (payload) => {
          recordRealtimeEvent();
          const doc = payload.new as any;
          if (shouldSuppressDocumentRealtime(doc.id, doc.workspace_app_id)) return;
          // Keep the optimistic-lock token current so a concurrent auto-save
          // doesn't 409 against a teammate's recent write.
          if (doc.id && doc.updated_at) {
            updateNoteVersionToken(doc.id, doc.updated_at);
          }
          debouncedFetchDocuments();
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'DELETE',
          schema: 'public',
          table: 'documents',
        },
        (payload) => {
          recordRealtimeEvent();
          const doc = payload.old as any;
          if (shouldSuppressDocumentRealtime(doc.id, doc.workspace_app_id)) return;
          debouncedFetchDocuments();
        }
      )

      // ================================================================
      // project_issue_comments INSERT/UPDATE/DELETE → invalidate React Query cache
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'project_issue_comments',
        },
        (payload) => {
          recordRealtimeEvent();
          const comment = payload.new as { id?: string; issue_id?: string };
          if (comment.id && comment.issue_id) {
            commentIssueMap.set(comment.id, comment.issue_id);
          }
          if (comment.issue_id) {
            queryClientRef.current.invalidateQueries({
              queryKey: projectKeys.comments(comment.issue_id),
            });
          }
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'project_issue_comments',
        },
        (payload) => {
          recordRealtimeEvent();
          const comment = payload.new as { id?: string; issue_id?: string };
          if (comment.id && comment.issue_id) {
            commentIssueMap.set(comment.id, comment.issue_id);
          }
          if (comment.issue_id) {
            queryClientRef.current.invalidateQueries({
              queryKey: projectKeys.comments(comment.issue_id),
            });
          }
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'DELETE',
          schema: 'public',
          table: 'project_issue_comments',
        },
        (payload) => {
          recordRealtimeEvent();
          const comment = payload.old as { id?: string; issue_id?: string };
          if (comment.id) {
            commentIssueMap.delete(comment.id);
          }
          if (comment.issue_id) {
            queryClientRef.current.invalidateQueries({
              queryKey: projectKeys.comments(comment.issue_id),
            });
          }
        }
      )

      // ================================================================
      // project_comment_reactions INSERT/DELETE → invalidate React Query cache
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'project_comment_reactions',
        },
        (payload) => {
          recordRealtimeEvent();
          const reaction = payload.new as { comment_id?: string };
          if (reaction.comment_id) {
            const issueId = findIssueIdForComment(reaction.comment_id);
            if (issueId) {
              queryClientRef.current.invalidateQueries({
                queryKey: projectKeys.comments(issueId),
              });
            } else {
              queryClientRef.current.invalidateQueries({
                queryKey: projectKeys.all,
                predicate: (query) => query.queryKey[1] === 'comments',
              });
            }
          }
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'DELETE',
          schema: 'public',
          table: 'project_comment_reactions',
        },
        (payload) => {
          recordRealtimeEvent();
          const reaction = payload.old as { comment_id?: string };
          if (reaction.comment_id) {
            const issueId = findIssueIdForComment(reaction.comment_id);
            if (issueId) {
              queryClientRef.current.invalidateQueries({
                queryKey: projectKeys.comments(issueId),
              });
            } else {
              queryClientRef.current.invalidateQueries({
                queryKey: projectKeys.all,
                predicate: (query) => query.queryKey[1] === 'comments',
              });
            }
          }
        }
      )

      // ================================================================
      // notifications INSERT → notification bell + toast
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'notifications',
          filter: `user_id=eq.${currentUserId}`,
        },
        (payload) => {
          recordRealtimeEvent();
          const notification = payload.new as any;

          // Update store (badge count + feed)
          useNotificationStore.getState().handleRealtimeInsert(notification);

          // Play sound
          playMessageNotification();

          // Show toast
          showNotificationToast(notification);
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'notifications',
          filter: `user_id=eq.${currentUserId}`,
        },
        (payload) => {
          recordRealtimeEvent();
          const notification = payload.new as any;
          const previousNotification = payload.old as any;
          const notificationStore = useNotificationStore.getState();
          const hadLocalRow = notificationStore.notifications.some((item) => item.id === notification.id);
          const hasPreviousState =
            typeof previousNotification?.read === 'boolean'
            && typeof previousNotification?.archived === 'boolean';

          notificationStore.handleRealtimeUpdate(notification, previousNotification);

          // Reconcile unread badge when the row was off-screen and the payload
          // does not include enough prior state to compute a safe delta locally.
          if (!hadLocalRow && !hasPreviousState) {
            void notificationStore.fetchUnreadCount();
          }
        }
      )

      // ================================================================
      // permissions INSERT/DELETE → revalidate shared workspaces
      // ================================================================
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'permissions',
          filter: `grantee_id=eq.${currentUserId}`,
        },
        () => {
          recordRealtimeEvent();
          void useWorkspaceStore.getState().fetchWorkspaces();
          void usePermissionStore.getState().fetchSharedWithMe();
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'DELETE',
          schema: 'public',
          table: 'permissions',
          filter: `grantee_id=eq.${currentUserId}`,
        },
        () => {
          recordRealtimeEvent();
          void useWorkspaceStore.getState().fetchWorkspaces();
          void usePermissionStore.getState().fetchSharedWithMe();
        }
      )

      .subscribe((status) => {
        console.log('[GlobalRealtime] Subscription status:', status);
        if (status === 'SUBSCRIBED') {
          if (hasSubscribedOnceRef.current) {
            revalidateActiveData('global-realtime-resubscribe');
          }
          hasSubscribedOnceRef.current = true;
          if (reconnectTimerRef.current) {
            clearTimeout(reconnectTimerRef.current);
            reconnectTimerRef.current = null;
          }
          return;
        }

        if (status === 'TIMED_OUT' || status === 'CHANNEL_ERROR' || status === 'CLOSED') {
          if (!reconnectTimerRef.current) {
            reconnectTimerRef.current = setTimeout(() => {
              if (subscriptionRef.current !== channel) return;
              if (channel.state === 'joined' || channel.state === 'joining' || channel.state === 'leaving') {
                reconnectTimerRef.current = null;
                return;
              }
              console.log('[GlobalRealtime] Reconnecting after status:', status);
              markResubscribeAttempt();
              void channel
                .unsubscribe()
                .catch((error) => {
                  console.warn('[GlobalRealtime] Failed to unsubscribe during reconnect:', error);
                })
                .finally(() => {
                  if (subscriptionRef.current !== channel) {
                    reconnectTimerRef.current = null;
                    return;
                  }
                  if (channel.state === 'joined' || channel.state === 'joining') {
                    reconnectTimerRef.current = null;
                    return;
                  }
                  channel.subscribe();
                  reconnectTimerRef.current = null;
                });
            }, 1500);
          }
        }
      });

    subscriptionRef.current = channel;

    return () => {
      console.log('[GlobalRealtime] Unsubscribing');
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      channel.unsubscribe();
      subscriptionRef.current = null;
      hasSubscribedOnceRef.current = false;
    };
  }, [currentUserId, enabled]); // Only re-subscribe on login/logout, or when explicitly enabled/disabled

  // Reconnect when page becomes visible after being idle
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState !== 'visible') return;
      noteVisible();
      if (!subscriptionRef.current) return;
      if (!shouldResubscribeOnResume()) return;

      console.log('[GlobalRealtime] Page visible - forcing resubscribe');
      const currentChannel = subscriptionRef.current;
      if (currentChannel.state === 'joining' || currentChannel.state === 'leaving') return;
      markResubscribeAttempt();
      void currentChannel
        .unsubscribe()
        .catch((error) => {
          console.warn('[GlobalRealtime] Failed to unsubscribe on visibility reconnect:', error);
        })
        .finally(() => {
          if (subscriptionRef.current !== currentChannel) return;
          currentChannel.subscribe();
        });
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, []);
}
