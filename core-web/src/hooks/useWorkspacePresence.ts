import { useEffect, useRef } from "react";
import { supabase } from "../lib/supabase";
import { useAuthStore } from "../stores/authStore";
import { usePresenceStore } from "../stores/presenceStore";
import {
  noteVisible,
  shouldResubscribeOnResume,
  markResubscribeAttempt,
} from "../lib/revalidation";
import type { RealtimeChannel } from "@supabase/supabase-js";
import {
  getPresenceSessionId,
  toWorkspacePresenceSnapshot,
  type PresenceTrackPayload,
} from "../lib/presence";

const TYPING_TIMEOUT_MS = 3000;
const TYPING_DEBOUNCE_MS = 2000;
const PRESENCE_RECONNECT_DELAY_MS = 1500;
const PRESENCE_REFRESH_INTERVAL_MS = 30000;

interface ChannelContext {
  channel: RealtimeChannel;
  reconnectTimer: ReturnType<typeof setTimeout> | null;
}

const presenceSessionId = getPresenceSessionId();
const presenceChannels = new Map<string, ChannelContext>();
const rebuildingWorkspaceIds = new Set<string>();

let activeTypingWorkspaceId: string | null = null;
let activeTypingChannel: RealtimeChannel | null = null;
let lastTypingBroadcast = 0;

function clearReconnectTimer(workspaceId: string) {
  const context = presenceChannels.get(workspaceId);
  if (!context?.reconnectTimer) return;
  clearTimeout(context.reconnectTimer);
  context.reconnectTimer = null;
}

function teardownWorkspaceChannel(workspaceId: string) {
  void removeWorkspaceChannel(workspaceId, { clearPresence: true });
}

function teardownAllPresenceChannels(clearPresence = true) {
  for (const workspaceId of [...presenceChannels.keys()]) {
    void removeWorkspaceChannel(workspaceId, { clearPresence });
  }
}

function buildPresenceTrackPayload(currentUserId: string): PresenceTrackPayload {
  return {
    user_id: currentUserId,
    session_id: presenceSessionId,
    online_at: new Date().toISOString(),
  };
}

async function trackPresence(
  workspaceId: string,
  channel: RealtimeChannel,
  currentUserId: string,
) {
  try {
    const status = await channel.track(buildPresenceTrackPayload(currentUserId));
    if (status !== "ok") {
      console.warn(`[WorkspacePresence] Track failed for ${workspaceId}:`, status);
      return false;
    }
    return true;
  } catch (error) {
    console.warn(`[WorkspacePresence] Track threw for ${workspaceId}:`, error);
    return false;
  }
}

function isChannelHealthy(channel: RealtimeChannel) {
  return channel.state === "joined" || channel.state === "joining";
}

function forceRemoveChannel(channel: RealtimeChannel) {
  const realtimeChannel = channel as RealtimeChannel & { teardown?: () => void };
  realtimeChannel.teardown?.();

  const realtimeClient = (
    supabase as typeof supabase & {
      realtime?: { _remove?: (channel: RealtimeChannel) => void };
    }
  ).realtime;
  realtimeClient?._remove?.(channel);
}

async function removeWorkspaceChannel(
  workspaceId: string,
  options: { clearPresence: boolean },
) {
  const context = presenceChannels.get(workspaceId);
  if (!context) return;

  if (activeTypingWorkspaceId === workspaceId) {
    activeTypingWorkspaceId = null;
    activeTypingChannel = null;
  }
  clearReconnectTimer(workspaceId);
  presenceChannels.delete(workspaceId);
  if (options.clearPresence) {
    usePresenceStore.getState().clearWorkspacePresence(workspaceId);
  }

  try {
    const status = await supabase.removeChannel(context.channel);
    if (status !== "ok") {
      console.warn(`[WorkspacePresence] Forced channel cleanup for ${workspaceId}:`, status);
      forceRemoveChannel(context.channel);
    }
  } catch (error) {
    console.warn(`[WorkspacePresence] Failed to remove ${workspaceId}:`, error);
    forceRemoveChannel(context.channel);
  }
}

export function useWorkspacePresence(
  activeWorkspaceId: string | undefined,
  workspaceIds: string[],
) {
  const currentUserId = useAuthStore((s) => s.user?.id);
  const typingTimersRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map());
  const activeWsRef = useRef<string | undefined>(activeWorkspaceId);
  const currentUserIdRef = useRef<string | undefined>(currentUserId);
  const workspaceIdsRef = useRef<Set<string>>(new Set());
  const createWorkspaceChannelRef = useRef<((workspaceId: string, userId: string) => void) | null>(null);
  const scheduleWorkspaceChannelRecoveryRef = useRef<((workspaceId: string, channel: RealtimeChannel, reason: string) => void) | null>(null);
  activeWsRef.current = activeWorkspaceId;
  currentUserIdRef.current = currentUserId;

  const normalizedWorkspaceIds = [...new Set(workspaceIds.filter(Boolean))].sort();
  const workspaceIdsKey = JSON.stringify(normalizedWorkspaceIds);
  workspaceIdsRef.current = new Set(normalizedWorkspaceIds);

  function scheduleWorkspaceChannelRecovery(
    workspaceId: string,
    channel: RealtimeChannel,
    reason: string,
  ) {
    const context = presenceChannels.get(workspaceId);
    if (!context || context.channel !== channel || context.reconnectTimer) return;

    context.reconnectTimer = setTimeout(() => {
      const liveContext = presenceChannels.get(workspaceId);
      if (!liveContext || liveContext.channel !== channel) return;

      if (isChannelHealthy(channel)) {
        liveContext.reconnectTimer = null;
        return;
      }

      liveContext.reconnectTimer = null;
      void rebuildWorkspaceChannel(workspaceId, channel, reason);
    }, PRESENCE_RECONNECT_DELAY_MS);
  }

  async function rebuildWorkspaceChannel(
    workspaceId: string,
    channel: RealtimeChannel,
    reason: string,
  ) {
    const context = presenceChannels.get(workspaceId);
    if (!context || context.channel !== channel) return;
    if (rebuildingWorkspaceIds.has(workspaceId)) return;

    const userId = currentUserIdRef.current;
    if (!userId) {
      teardownWorkspaceChannel(workspaceId);
      return;
    }

    if (!workspaceIdsRef.current.has(workspaceId)) {
      teardownWorkspaceChannel(workspaceId);
      return;
    }

    rebuildingWorkspaceIds.add(workspaceId);
    console.log(`[WorkspacePresence] Rebuilding ${workspaceId} after ${reason}`);

    try {
      await removeWorkspaceChannel(workspaceId, { clearPresence: false });
      createWorkspaceChannel(workspaceId, userId);
    } finally {
      rebuildingWorkspaceIds.delete(workspaceId);
    }
  }

  function createWorkspaceChannel(workspaceId: string, userId: string) {
    const channel = supabase.channel(`workspace:${workspaceId}`, {
      config: { presence: { key: presenceSessionId } },
    });
    const context: ChannelContext = {
      channel,
      reconnectTimer: null,
    };
    presenceChannels.set(workspaceId, context);

    channel
      .on("presence", { event: "sync" }, () => {
        const liveContext = presenceChannels.get(workspaceId);
        if (!liveContext || liveContext.channel !== channel) return;
        usePresenceStore
          .getState()
          .setWorkspacePresenceSnapshot(
            workspaceId,
            toWorkspacePresenceSnapshot(
              channel.presenceState<PresenceTrackPayload>(),
            ),
          );
      })
      // Typing listeners gated to active workspace only
      .on("broadcast", { event: "typing" }, (msg) => {
        if (activeWsRef.current !== workspaceId) return;
        const { userId: typingUserId, userName: name, channelId } = msg.payload;
        if (!channelId || typingUserId === userId) return;

        usePresenceStore.getState().addTypingUser({
          userId: typingUserId,
          userName: name,
          channelId,
          timestamp: Date.now(),
        });

        const timerKey = `${channelId}:${typingUserId}`;
        const existing = typingTimersRef.current.get(timerKey);
        if (existing) clearTimeout(existing);

        const timer = setTimeout(() => {
          usePresenceStore.getState().removeTypingUser(channelId, typingUserId);
          typingTimersRef.current.delete(timerKey);
        }, TYPING_TIMEOUT_MS);

        typingTimersRef.current.set(timerKey, timer);
      })
      .on("broadcast", { event: "stop_typing" }, (msg) => {
        if (activeWsRef.current !== workspaceId) return;
        const { userId: typingUserId, channelId } = msg.payload;
        if (!channelId) return;
        usePresenceStore.getState().removeTypingUser(channelId, typingUserId);
        const timerKey = `${channelId}:${typingUserId}`;
        const existing = typingTimersRef.current.get(timerKey);
        if (existing) {
          clearTimeout(existing);
          typingTimersRef.current.delete(timerKey);
        }
      })
      .subscribe(async (status) => {
        const liveContext = presenceChannels.get(workspaceId);
        if (!liveContext || liveContext.channel !== channel) return;

        console.log(`[WorkspacePresence] ${workspaceId} status:`, status);

        if (status === "SUBSCRIBED") {
          clearReconnectTimer(workspaceId);
          const tracked = await trackPresence(workspaceId, channel, userId);
          if (!tracked) {
            scheduleWorkspaceChannelRecovery(workspaceId, channel, "track failure");
          }
          return;
        }

        if (status === "TIMED_OUT" || status === "CHANNEL_ERROR" || status === "CLOSED") {
          scheduleWorkspaceChannelRecovery(workspaceId, channel, status);
        }
      });
  }

  createWorkspaceChannelRef.current = createWorkspaceChannel;
  scheduleWorkspaceChannelRecoveryRef.current = scheduleWorkspaceChannelRecovery;

  // Subscribe to presence on ALL the user's workspaces simultaneously.
  useEffect(() => {
    const workspaceIdList: string[] = workspaceIdsKey ? JSON.parse(workspaceIdsKey) : [];

    if (!currentUserId || workspaceIdList.length === 0) {
      teardownAllPresenceChannels();
      usePresenceStore.getState().clearAll();
      return;
    }

    const targetWorkspaceIds = new Set(workspaceIdList);

    // Remove channels for workspaces we're no longer in
    for (const workspaceId of [...presenceChannels.keys()]) {
      if (!targetWorkspaceIds.has(workspaceId)) {
        teardownWorkspaceChannel(workspaceId);
      }
    }

    // Add channels for new workspaces
    for (const workspaceId of workspaceIdList) {
      if (presenceChannels.has(workspaceId) || rebuildingWorkspaceIds.has(workspaceId)) continue;
      createWorkspaceChannelRef.current?.(workspaceId, currentUserId);
    }
  }, [currentUserId, workspaceIdsKey]);

  // Update active typing channel when workspace switches
  useEffect(() => {
    typingTimersRef.current.forEach((timer) => clearTimeout(timer));
    typingTimersRef.current.clear();
    usePresenceStore.setState({ typingUsers: {} });

    if (!activeWorkspaceId || !currentUserId) {
      activeTypingWorkspaceId = null;
      activeTypingChannel = null;
      return;
    }

    const context = presenceChannels.get(activeWorkspaceId);
    if (!context) {
      activeTypingWorkspaceId = null;
      activeTypingChannel = null;
      return;
    }

    activeTypingWorkspaceId = activeWorkspaceId;
    activeTypingChannel = context.channel;
  }, [activeWorkspaceId, currentUserId]);

  // Resubscribe all channels on resume/focus
  useEffect(() => {
    const workspaceIdList: string[] = workspaceIdsKey ? JSON.parse(workspaceIdsKey) : [];
    if (!currentUserId || workspaceIdList.length === 0) return;

    const handleResume = () => {
      if (document.visibilityState === "hidden") return;
      noteVisible();
      if (!shouldResubscribeOnResume()) return;

      for (const [workspaceId, context] of presenceChannels.entries()) {
        if (context.channel.state === "joined") {
          void trackPresence(workspaceId, context.channel, currentUserId);
          continue;
        }

        if (context.channel.state === "joining") {
          continue;
        }

        console.log(`[WorkspacePresence] Refreshing ${workspaceId} on resume`);
        markResubscribeAttempt();
        scheduleWorkspaceChannelRecoveryRef.current?.(workspaceId, context.channel, "resume");
      }
    };

    document.addEventListener("visibilitychange", handleResume);
    window.addEventListener("focus", handleResume);
    return () => {
      document.removeEventListener("visibilitychange", handleResume);
      window.removeEventListener("focus", handleResume);
    };
  }, [currentUserId, workspaceIdsKey]);

  // Reassert presence periodically while the tab is visible. This repairs
  // sessions after transient transport issues without forcing channel churn.
  useEffect(() => {
    const workspaceIdList: string[] = workspaceIdsKey ? JSON.parse(workspaceIdsKey) : [];
    if (!currentUserId || workspaceIdList.length === 0) return;

    const interval = window.setInterval(() => {
      if (document.visibilityState === "hidden") return;

      for (const [workspaceId, context] of presenceChannels.entries()) {
        if (context.channel.state === "joined") {
          void trackPresence(workspaceId, context.channel, currentUserId);
          continue;
        }

        if (context.channel.state === "joining") {
          continue;
        }

        scheduleWorkspaceChannelRecoveryRef.current?.(
          workspaceId,
          context.channel,
          "refresh interval",
        );
      }
    }, PRESENCE_REFRESH_INTERVAL_MS);

    return () => {
      window.clearInterval(interval);
    };
  }, [currentUserId, workspaceIdsKey]);

  // Cleanup on unmount
  useEffect(() => {
    const typingTimers = typingTimersRef.current;
    return () => {
      teardownAllPresenceChannels();
      typingTimers.forEach((timer) => clearTimeout(timer));
      typingTimers.clear();
      usePresenceStore.getState().clearAll();
    };
  }, []);
}

// Standalone broadcast functions — usable from any component
export function broadcastTyping(channelId: string) {
  if (!activeTypingChannel) return;
  const now = Date.now();
  if (now - lastTypingBroadcast < TYPING_DEBOUNCE_MS) return;
  lastTypingBroadcast = now;

  const state = useAuthStore.getState();
  const userId = state.user?.id;
  if (!userId) return;

  const payload = {
    userId,
    userName: state.userProfile?.name || state.user?.email || "Someone",
    channelId,
  };

  if (activeTypingChannel.state === "joined") {
    void activeTypingChannel.send({
      type: "broadcast",
      event: "typing",
      payload,
    });
    return;
  }

  void activeTypingChannel.httpSend("typing", payload).catch(() => {});
}

export function stopTyping(channelId: string) {
  if (!activeTypingChannel) return;
  const userId = useAuthStore.getState().user?.id;
  if (!userId) return;

  const payload = { userId, channelId };

  if (activeTypingChannel.state === "joined") {
    void activeTypingChannel.send({
      type: "broadcast",
      event: "stop_typing",
      payload,
    });
    return;
  }

  void activeTypingChannel.httpSend("stop_typing", payload).catch(() => {});
}
