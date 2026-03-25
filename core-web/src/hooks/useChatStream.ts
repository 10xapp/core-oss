import { useState, useRef, useCallback } from 'react';
import { streamMessage, regenerateMessage, type ContentPart } from '../api/client';
import { WebContentBuilder } from '../lib/WebContentBuilder';

interface DisplayMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  content_parts?: ContentPart[];
}

interface UseChatStreamParams {
  activeConversationRef: React.MutableRefObject<string | null>;
  setMessages: React.Dispatch<React.SetStateAction<DisplayMessage[]>>;
  selectedWorkspaceIds: string[];
  workspaceId?: string;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type StreamEvent = any;

/**
 * Processes a stream of events from the API, updating streaming state and
 * finalizing the assistant message when done.
 */
function processStreamEvents(
  stream: AsyncGenerator<StreamEvent>,
  convId: string,
  streamingConversationRef: React.MutableRefObject<string | null>,
  builderRef: React.MutableRefObject<WebContentBuilder | null>,
  setStreamingContent: React.Dispatch<React.SetStateAction<string>>,
  setStreamingParts: React.Dispatch<React.SetStateAction<ContentPart[]>>,
  setIsWaitingForResponse: React.Dispatch<React.SetStateAction<boolean>>,
  setStreamStatus: React.Dispatch<React.SetStateAction<string | null>>,
  setMessages: React.Dispatch<React.SetStateAction<DisplayMessage[]>>,
  _setLoading: React.Dispatch<React.SetStateAction<boolean>>,
  mark: (label: string) => void,
) {
  return (async () => {
    // storageBuilder: exact stream payload for final persisted assistant message
    const storageBuilder = new WebContentBuilder();
    // displayBuilder: what the user sees progressively during streaming
    const displayBuilder = new WebContentBuilder();
    builderRef.current = displayBuilder;

    let firstEvent = true;
    let firstToken = true;
    let revealedText = '';
    let pendingTextSegments: string[] = [];
    let pendingChars = 0;
    let doneReceived = false;
    let doneEventMessageId: string | undefined;
    let didFinalize = false;

    let flushScheduled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    const splitIntoSegments = (input: string): string[] => {
      // Split into whitespace and non-whitespace segments.
      // Preserves all whitespace including leading spaces from API deltas.
      return input.match(/\s+|[^\s]+/g) ?? [];
    };

    const clearScheduledFlush = () => {
      flushScheduled = false;
      if (timeoutId != null) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    };

    const flushSnapshot = () => {
      setStreamingContent(revealedText);
      setStreamingParts(displayBuilder.getSnapshot());
    };

    // How many word-segments to reveal per tick.
    // During normal streaming: 1 word per ~35ms for a smooth per-word animation.
    // When draining after stream ends or buffer is large: reveal faster to catch up.
    const getRevealWordCount = (): number => {
      if (doneReceived) {
        if (pendingTextSegments.length > 80) return 12;
        if (pendingTextSegments.length > 40) return 8;
        if (pendingTextSegments.length > 20) return 4;
        return 2;
      }
      // When buffer is building up, reveal slightly faster to keep pace
      if (pendingTextSegments.length > 60) return 4;
      if (pendingTextSegments.length > 30) return 2;
      return 1;
    };

    const drainQueuedText = (maxWords: number | null): boolean => {
      if (!pendingTextSegments.length) return false;

      let wordsRevealed = 0;
      const limit = maxWords ?? Number.MAX_SAFE_INTEGER;
      let drained = '';

      while (pendingTextSegments.length > 0 && wordsRevealed < limit) {
        const next = pendingTextSegments.shift()!;
        drained += next;
        pendingChars = Math.max(0, pendingChars - next.length);
        // Only count non-whitespace segments as words
        if (next.trim()) wordsRevealed++;
      }

      if (!drained) return false;
      displayBuilder.appendText(drained);
      revealedText += drained;
      return true;
    };

    const finalizeStream = (messageId?: string) => {
      if (didFinalize) return;
      didFinalize = true;
      clearScheduledFlush();
      pendingTextSegments = [];
      pendingChars = 0;

      const finalParts = storageBuilder.finalize();
      const finalText = storageBuilder.getFullText();

      setMessages((prev) => [
        ...prev,
        {
          id: messageId || `assistant-${Date.now()}`,
          role: 'assistant',
          content: finalText,
          content_parts: finalParts.length > 0 ? finalParts : undefined,
        },
      ]);
      builderRef.current = null;
      setStreamingContent('');
      setStreamingParts([]);
      setStreamStatus(null);
      setIsWaitingForResponse(false);

      if (streamingConversationRef.current === convId) {
        streamingConversationRef.current = null;
      }
    };

    const flushAllQueuedText = () => {
      clearScheduledFlush();
      const changed = drainQueuedText(null);
      if (changed) {
        flushSnapshot();
      }
    };

    const runRevealFrame = () => {
      timeoutId = null;
      flushScheduled = false;

      // Stream was cancelled/switched.
      if (streamingConversationRef.current !== convId) {
        clearScheduledFlush();
        return;
      }

      const changed = drainQueuedText(getRevealWordCount());
      if (changed) {
        flushSnapshot();
      }
      if (pendingTextSegments.length > 0) {
        scheduleSnapshotFlush();
        return;
      }

      if (doneReceived) {
        finalizeStream(doneEventMessageId);
      }
    };

    // Throttle state updates to ~35ms (~28 updates/sec) for smooth per-word animation
    const scheduleSnapshotFlush = () => {
      if (flushScheduled) return;
      flushScheduled = true;
      timeoutId = setTimeout(runRevealFrame, 35);
    };

    for await (const event of stream) {
      // Check if we're still streaming for this conversation
      if (streamingConversationRef.current !== convId) {
        break; // User switched to new chat, abandon this stream
      }

      if (firstEvent) { mark('first_event (' + event.type + ')'); firstEvent = false; }

      switch (event.type) {
        case 'tool_call':
          flushAllQueuedText();
          if (event.name) {
            if (event.phase === 'start') {
              storageBuilder.addToolCallStart(event.name, event.args);
              displayBuilder.addToolCallStart(event.name, event.args);
              flushSnapshot();
              setIsWaitingForResponse(false);
            } else if (event.phase === 'end') {
              storageBuilder.updateToolCallEnd(event.name, event.duration_ms, event.status);
              displayBuilder.updateToolCallEnd(event.name, event.duration_ms, event.status);
              flushSnapshot();
            }
          }
          break;

        case 'content':
          if (event.delta) {
            if (firstToken) { mark('first_token'); firstToken = false; }
            storageBuilder.appendText(event.delta);
            const nextSegments = splitIntoSegments(event.delta);
            if (nextSegments.length > 0) {
              pendingTextSegments.push(...nextSegments);
              for (const segment of nextSegments) {
                pendingChars += segment.length;
              }
              scheduleSnapshotFlush();
            }
            setIsWaitingForResponse(false);
          }
          break;

        case 'display':
          flushAllQueuedText();
          storageBuilder.addDisplay(event);
          displayBuilder.addDisplay(event);
          flushSnapshot();
          setIsWaitingForResponse(false);
          break;

        case 'action':
          flushAllQueuedText();
          storageBuilder.addAction(event);
          displayBuilder.addAction(event);
          flushSnapshot();
          setIsWaitingForResponse(false);
          break;

        case 'sources':
          flushAllQueuedText();
          if (event.sources) {
            storageBuilder.addSources(event.sources);
            displayBuilder.addSources(event.sources);
            flushSnapshot();
          }
          break;

        case 'status':
          setStreamStatus(event.message || event.description || null);
          break;

        case 'done': {
          mark('done');
          doneReceived = true;
          doneEventMessageId = typeof event.message_id === 'string' ? event.message_id : undefined;
          setIsWaitingForResponse(false);

          if (pendingTextSegments.length > 0) {
            scheduleSnapshotFlush();
          } else {
            finalizeStream(doneEventMessageId);
          }
          break;
        }

        case 'error': {
          const errorMsg = event.error || event.message || 'Sorry, there was an error processing your request.';
          console.error('Stream error:', errorMsg);
          pendingTextSegments = [];
          pendingChars = 0;
          doneReceived = false;
          didFinalize = true;
          clearScheduledFlush();
          streamingConversationRef.current = null;
          builderRef.current = null;
          setStreamingContent('');
          setStreamingParts([]);
          setStreamStatus(null);
          setIsWaitingForResponse(false);
          setMessages((prev) => [
            ...prev,
            {
              id: `error-${Date.now()}`,
              role: 'assistant',
              content: errorMsg,
            },
          ]);
          break;
        }

        // ping events are ignored
      }
    }

    if (doneReceived && !didFinalize) {
      if (streamingConversationRef.current === convId) {
        if (pendingTextSegments.length > 0) {
          scheduleSnapshotFlush();
        } else {
          finalizeStream(doneEventMessageId);
        }
      } else {
        pendingTextSegments = [];
        pendingChars = 0;
        clearScheduledFlush();
        builderRef.current = null;
        setStreamingContent('');
        setStreamingParts([]);
        setStreamStatus(null);
        setIsWaitingForResponse(false);
      }
      return;
    }

    pendingTextSegments = [];
    pendingChars = 0;
    clearScheduledFlush();

    // Clear streaming ref when stream ends without a done event (cancel/switch).
    if (streamingConversationRef.current === convId) {
      streamingConversationRef.current = null;
    }
  })();
}

export function useChatStream({
  activeConversationRef,
  setMessages,
  selectedWorkspaceIds,
  workspaceId,
}: UseChatStreamParams) {
  const [streamingContent, setStreamingContent] = useState('');
  const [streamingParts, setStreamingParts] = useState<ContentPart[]>([]);
  const [isWaitingForResponse, setIsWaitingForResponse] = useState(false);
  const [streamStatus, setStreamStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const streamingConversationRef = useRef<string | null>(null);
  const builderRef = useRef<WebContentBuilder | null>(null);

  const hasStreamingContent = streamingParts.length > 0 || streamingContent.length > 0 || isWaitingForResponse;

  const handleStopStreaming = useCallback(() => {
    // Clear the streaming ref to signal the stream loop to stop
    streamingConversationRef.current = null;

    // Commit partial content from builder
    const builder = builderRef.current;
    const finalParts = builder?.finalize();
    const finalText = builder?.getFullText() || streamingContent;

    if (finalText || (finalParts && finalParts.length > 0)) {
      setMessages((prev) => [
        ...prev,
        {
          id: `assistant-${Date.now()}`,
          role: 'assistant',
          content: finalText,
          content_parts: finalParts,
        },
      ]);
    }

    // Reset streaming state
    builderRef.current = null;
    setStreamingContent('');
    setStreamingParts([]);
    setStreamStatus(null);
    setIsWaitingForResponse(false);
    setLoading(false);
  }, [streamingContent, setMessages]);

  const sendMessage = useCallback(async (
    userMessage: string,
    convId: string,
    attachmentIds?: string[],
    attachmentParts?: ContentPart[],
  ) => {
    const t0 = performance.now();
    const mark = (label: string) => console.log(`⏱ ${label}: ${(performance.now() - t0).toFixed(0)}ms`);

    setLoading(true);
    setIsWaitingForResponse(true);
    setStreamingContent('');
    setStreamingParts([]);
    setStreamStatus(null);

    // Add user message immediately
    const tempUserId = `temp-${Date.now()}`;
    setMessages((prev) => [
      ...prev,
      {
        id: tempUserId,
        role: 'user',
        content: userMessage,
        ...(attachmentParts && attachmentParts.length > 0 ? { content_parts: attachmentParts } : {}),
      },
    ]);

    try {
      // Track which conversation this stream belongs to
      streamingConversationRef.current = convId;

      const stream = streamMessage(convId, userMessage, {
        attachmentIds,
        workspaceIds: selectedWorkspaceIds.length > 0 ? selectedWorkspaceIds : workspaceId ? [workspaceId] : undefined,
      });

      await processStreamEvents(
        stream,
        convId,
        streamingConversationRef,
        builderRef,
        setStreamingContent,
        setStreamingParts,
        setIsWaitingForResponse,
        setStreamStatus,
        setMessages,
        setLoading,
        mark,
      );
    } catch (err) {
      console.error('Failed to send message:', err);
      builderRef.current = null;
      setStreamingContent('');
      setStreamingParts([]);
      setStreamStatus(null);
      setIsWaitingForResponse(false);
      setMessages((prev) => [
        ...prev,
        {
          id: `error-${Date.now()}`,
          role: 'assistant',
          content: 'Sorry, there was an error connecting to the server.',
        },
      ]);
    } finally {
      setLoading(false);
    }
  }, [selectedWorkspaceIds, workspaceId, setMessages]);

  const handleRegenerate = useCallback(async (messageId: string) => {
    const convId = activeConversationRef.current;
    if (!convId || loading) return;

    const t0 = performance.now();
    const mark = (label: string) => console.log(`⏱ regen ${label}: ${(performance.now() - t0).toFixed(0)}ms`);

    // Remove the target message and any messages after it
    setMessages((prev) => {
      const idx = prev.findIndex((m) => m.id === messageId);
      if (idx === -1) return prev;
      return prev.slice(0, idx);
    });

    setLoading(true);
    setIsWaitingForResponse(true);
    setStreamingContent('');
    setStreamingParts([]);
    setStreamStatus(null);

    try {
      streamingConversationRef.current = convId;

      const stream = regenerateMessage(convId, messageId, {
        workspaceIds: selectedWorkspaceIds.length > 0 ? selectedWorkspaceIds : workspaceId ? [workspaceId] : undefined,
      });

      await processStreamEvents(
        stream,
        convId,
        streamingConversationRef,
        builderRef,
        setStreamingContent,
        setStreamingParts,
        setIsWaitingForResponse,
        setStreamStatus,
        setMessages,
        setLoading,
        mark,
      );
    } catch (err) {
      console.error('Failed to regenerate message:', err);
      builderRef.current = null;
      setStreamingContent('');
      setStreamingParts([]);
      setStreamStatus(null);
      setIsWaitingForResponse(false);
      setMessages((prev) => [
        ...prev,
        {
          id: `error-${Date.now()}`,
          role: 'assistant',
          content: 'Sorry, there was an error regenerating the response.',
        },
      ]);
    } finally {
      setLoading(false);
    }
  }, [activeConversationRef, loading, selectedWorkspaceIds, workspaceId, setMessages]);

  return {
    streamingContent,
    streamingParts,
    isWaitingForResponse,
    streamStatus,
    loading,
    hasStreamingContent,
    sendMessage,
    handleStopStreaming,
    handleRegenerate,
    streamingConversationRef,
    builderRef,
    // Expose setters needed by ChatView for reset scenarios
    setStreamingContent,
    setStreamingParts,
    setStreamStatus,
    setIsWaitingForResponse,
    setLoading,
  };
}
