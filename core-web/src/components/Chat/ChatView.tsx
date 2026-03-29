import { useState, useRef, useEffect, useCallback } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { ArrowDownIcon, ClockIcon, TrashIcon, EllipsisHorizontalIcon, PencilIcon } from '@heroicons/react/24/outline';
import { motion, AnimatePresence } from 'motion/react';
import { Plus } from 'lucide-react';
import { Icon } from '../ui/Icon';
import { getMessages, createConversation, type Message, type ContentPart } from '../../api/client';
import { useConversationStore } from '../../stores/conversationStore';
import { useWorkspaceStore } from '../../stores/workspaceStore';
import { useUIStore } from '../../stores/uiStore';
import type { MentionData } from '../../types/mention';
import { useChatAttachments, type UploadedAttachmentInfo } from '../../hooks/useChatAttachments';
import { useChatStream } from '../../hooks/useChatStream';
import { useScrollBehavior } from '../../hooks/useScrollBehavior';
import ChatMessage from './ChatMessage';
import ChatInput from './ChatInput';
import Dropdown from '../Dropdown/Dropdown';
import { isHeicFile } from '../../lib/heicConverter';
import { SIDEBAR } from '../../lib/sidebar';

interface DisplayMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  content_parts?: ContentPart[];
}

export default function ChatView() {
  const { conversationId: urlConversationId, workspaceId } = useParams();
  const navigate = useNavigate();
  const location = useLocation();

  // Helper to build chat URL preserving workspace context
  const getChatUrl = useCallback((conversationId?: string) => {
    const base = workspaceId ? `/workspace/${workspaceId}/chat` : '/chat';
    return conversationId ? `${base}/${conversationId}` : base;
  }, [workspaceId]);
  const addConversation = useConversationStore((state) => state.addConversation);
  const setActiveConversationId = useConversationStore((state) => state.setActiveConversationId);
  const activeConversationId = useConversationStore((state) => state.activeConversationId);
  const conversations = useConversationStore((state) => state.conversations);
  const removeConversation = useConversationStore((state) => state.removeConversation);
  const fetchConversations = useConversationStore((state) => state.fetchConversations);
  const hasChattedThisSession = useConversationStore((state) => state.hasChattedThisSession);
  const workspaces = useWorkspaceStore((state) => state.workspaces);
  const [selectedWorkspaceIds, setSelectedWorkspaceIds] = useState<string[]>([]);
  const [mentions, setMentions] = useState<MentionData[]>([]);
  const [messages, setMessages] = useState<DisplayMessage[]>([]);
  const [input, setInput] = useState('');
  const [loadingMessages, setLoadingMessages] = useState(false);
  const showPreviousChats = useUIStore((state) => state.isChatHistoryOpen);
  const setShowPreviousChats = useUIStore((state) => state.setChatHistoryOpen);
  const [openConversationMenuId, setOpenConversationMenuId] = useState<string | null>(null);
  const [editingConversationId, setEditingConversationId] = useState<string | null>(null);
  const [editingTitle, setEditingTitle] = useState('');
  const conversationMenuRefs = useRef<Map<string, HTMLButtonElement>>(new Map());

  // Attachment upload hook
  const { attachments: pendingAttachments, isUploading, addFiles, removeAttachment, uploadAll, clearAll } = useChatAttachments();

  // Mention handlers
  const activeWorkspaceId = useWorkspaceStore((state) => state.activeWorkspaceId);
  const mentionWorkspaceId = workspaceId || activeWorkspaceId || null;

  const handleMentionSelect = useCallback((data: MentionData) => {
    setMentions((prev) => {
      if (prev.some((m) => m.entityId === data.entityId)) return prev;
      return [...prev, data];
    });
  }, []);

  const handleRemoveMention = useCallback((entityId: string) => {
    setMentions((prev) => prev.filter((m) => m.entityId !== entityId));
  }, []);

  // Drag-drop state
  const [isDragging, setIsDragging] = useState(false);
  const dragCounter = useRef(0);

  const updateConversationTitle = useConversationStore((state) => state.updateConversationTitle);

  const handleRenameConversation = (conversationId: string, currentTitle: string) => {
    setOpenConversationMenuId(null);
    setEditingConversationId(conversationId);
    setEditingTitle(currentTitle || 'Untitled');
  };

  const handleRenameSubmit = (conversationId: string) => {
    const trimmed = editingTitle.trim();
    if (trimmed) {
      updateConversationTitle(conversationId, trimmed);
    }
    setEditingConversationId(null);
    setEditingTitle('');
  };

  const handleRenameKeyDown = (e: React.KeyboardEvent, conversationId: string) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleRenameSubmit(conversationId);
    } else if (e.key === 'Escape') {
      setEditingConversationId(null);
      setEditingTitle('');
    }
  };

  // Track conversation ID internally to avoid re-renders on URL change
  const activeConversationRef = useRef<string | null>(urlConversationId || null);

  // Ref for the last user message to scroll it to top
  const lastUserMessageRef = useRef<HTMLDivElement>(null);

  // When true, the user explicitly requested a new chat — skip redirect to recent
  const wantsNewChatRef = useRef(false);

  // When true, we just created a conversation in sendMessage — skip loadMessages
  const justCreatedConversationRef = useRef(false);

  // Scroll container ref
  const scrollContainerRef = useRef<HTMLDivElement>(null);

  // Track streaming content height
  const streamingRef = useRef<HTMLDivElement>(null);

  // Chat stream hook
  const {
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
    setStreamingContent,
    setStreamingParts,
    setStreamStatus,
    setIsWaitingForResponse,
    setLoading,
  } = useChatStream({
    activeConversationRef,
    setMessages,
    selectedWorkspaceIds,
    workspaceId,
  });

  // Scroll behavior hook
  const {
    containerHeight,
    setContainerHeight,
    aiResponseTop,
    setAiResponseTop,
    availableAiSpace,
    setAvailableAiSpace,
    streamingHeight,
    setStreamingHeight,
    showScrollButton,
    scrollToBottom,
  } = useScrollBehavior({
    messages,
    hasStreamingContent,
    scrollContainerRef,
    lastUserMessageRef,
    streamingRef,
  });

  // Handle initial message from SearchHeader navigation
  const initialMessageHandled = useRef(false);
  useEffect(() => {
    const state = location.state as { initialMessage?: string } | null;
    if (state?.initialMessage && !initialMessageHandled.current) {
      initialMessageHandled.current = true;
      setInput(state.initialMessage);
      // Clear the state to prevent re-triggering
      navigate(location.pathname, { replace: true, state: {} });
    }
  }, [location.state, location.pathname, navigate]);

  // Fetch conversations when the previous chats sidebar is opened
  useEffect(() => {
    if (showPreviousChats) {
      fetchConversations();
    }
  }, [showPreviousChats, fetchConversations]);

  // Sync ref with URL when navigating to existing conversations
  useEffect(() => {
    if (urlConversationId) {
      activeConversationRef.current = urlConversationId;
    }
  }, [urlConversationId]);

  // Reset state when activeConversationId is cleared (e.g., clicking "New Chat" in sidebar)
  useEffect(() => {
    if (activeConversationId === null && activeConversationRef.current !== null) {
      setMessages([]);
      setStreamingContent('');
      setStreamingParts([]);
      setStreamStatus(null);
      setIsWaitingForResponse(false);
      setLoading(false);
      setContainerHeight(null);
      setAiResponseTop(0);
      setAvailableAiSpace(0);
      setStreamingHeight(0);
      activeConversationRef.current = null;
      streamingConversationRef.current = null; // Cancel any ongoing stream
      builderRef.current = null;
    }
  }, [activeConversationId, setStreamingContent, setStreamingParts, setStreamStatus, setIsWaitingForResponse, setLoading, setContainerHeight, setAiResponseTop, setAvailableAiSpace, setStreamingHeight, streamingConversationRef, builderRef]);

  // Load messages when URL conversation changes (navigating to existing conversation)
  useEffect(() => {
    if (!urlConversationId) {
      // If user explicitly clicked "New Chat", honour that
      if (wantsNewChatRef.current) {
        wantsNewChatRef.current = false;
        activeConversationRef.current = null;
        return;
      }
      // If we just created a conversation in sendMessage, skip the redirect —
      // the URL was already updated via replaceState and messages are managed locally
      if (justCreatedConversationRef.current) {
        justCreatedConversationRef.current = false;
        return;
      }
      // If user has chatted this session, resume the most recent conversation
      if (hasChattedThisSession && conversations.length > 0) {
        const mostRecent = conversations[0];
        navigate(getChatUrl(mostRecent.id), { replace: true });
        return;
      }
      // New chat - just reset the ref, sidebar handles activeConversationId
      activeConversationRef.current = null;
      return;
    }

    // Set active conversation for sidebar highlighting
    setActiveConversationId(urlConversationId);

    async function loadMessages() {
      setLoadingMessages(true);
      try {
        const data = await getMessages(urlConversationId!);
        setMessages(
          data.map((msg: Message) => ({
            id: msg.id,
            role: msg.role,
            content: msg.content,
            content_parts: msg.content_parts,
          }))
        );
        // Scroll to bottom instantly (no smooth animation) after messages render
        requestAnimationFrame(() => {
          const container = scrollContainerRef.current;
          if (container) {
            container.scrollTop = container.scrollHeight;
          }
        });
      } catch (err) {
        console.error('Failed to load messages:', err);
      } finally {
        setLoadingMessages(false);
      }
    }

    loadMessages();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlConversationId, setActiveConversationId, hasChattedThisSession]);

  // Drag-drop handlers
  const handleDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter.current++;
    if (e.dataTransfer.types.includes('Files')) {
      setIsDragging(true);
    }
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter.current--;
    if (dragCounter.current === 0) {
      setIsDragging(false);
    }
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    dragCounter.current = 0;

    const files = Array.from(e.dataTransfer.files).filter(f => f.type.startsWith('image/') || isHeicFile(f));
    if (files.length > 0) {
      addFiles(files);
    }
  }, [addFiles]);

  // Submit handler that handles attachment upload before sending
  const buildAttachmentParts = (uploaded: UploadedAttachmentInfo[]): ContentPart[] => {
    return uploaded.map((attachment) => ({
      id: `attachment-${attachment.attachmentId}`,
      type: 'attachment',
      data: {
        attachment_id: attachment.attachmentId,
        width: attachment.width,
        height: attachment.height,
      },
    }));
  };

  const handleSubmitWithAttachments = async () => {
    const hasText = input.trim().length > 0;
    const hasAttachments = pendingAttachments.length > 0;
    if ((!hasText && !hasAttachments) || loading) return;

    let attachmentIds: string[] | undefined;
    let attachmentParts: ContentPart[] | undefined;

    if (hasAttachments) {
      // Need a conversation ID for uploads
      let convId = activeConversationRef.current;
      if (!convId) {
        const conversation = await createConversation();
        convId = conversation.id;
        activeConversationRef.current = convId;
        justCreatedConversationRef.current = true;
        window.history.replaceState(null, '', getChatUrl(convId));
        addConversation(conversation);
        setActiveConversationId(convId);
      }

      const result = await uploadAll(convId);
      attachmentIds = result.attachmentIds;
      attachmentParts = buildAttachmentParts(result.uploadedAttachments);
      if (result.hadErrors) {
        return;
      }
      clearAll();
    }

    // Append mention hints so the AI knows which documents the user referenced
    let userMessage = input.trim();
    if (mentions.length > 0) {
      const mentionHints = mentions
        .map((m) => `[User referenced: ${m.displayName} (${m.entityType}, id: ${m.entityId})]`)
        .join('\n');
      userMessage = `${userMessage}\n\n${mentionHints}`;
    }
    setInput('');
    setMentions([]);

    // Create conversation if needed (silently update URL, no re-render)
    let convId = activeConversationRef.current;
    if (!convId) {
      const conversation = await createConversation();
      convId = conversation.id;
      activeConversationRef.current = convId;
      // Flag to prevent the loadMessages effect from firing when hasChattedThisSession changes
      justCreatedConversationRef.current = true;
      // Update URL silently without triggering re-render
      window.history.replaceState(null, '', getChatUrl(convId));
      // Add to sidebar and mark as active
      addConversation(conversation);
      setActiveConversationId(convId);
    }

    await sendMessage(userMessage, convId, attachmentIds, attachmentParts);
  };

  // Treat as non-empty while loading or when we know a redirect to active conversation is pending
  const isRestoringConversation = !urlConversationId && !!activeConversationId && !wantsNewChatRef.current && messages.length === 0;
  const isEmpty = messages.length === 0 && !hasStreamingContent && !loadingMessages && !isRestoringConversation;

  return (
    <div className="flex-1 flex h-full overflow-hidden">
    {/* Previous Chats - left panel */}
    <AnimatePresence initial={false}>
      {showPreviousChats && (
        <motion.div
          initial={{ width: 0, opacity: 0 }}
          animate={{ width: 212, opacity: 1 }}
          exit={{ width: 0, opacity: 0 }}
          transition={{ duration: 0.2, ease: [0.4, 0, 0.2, 1] }}
          className={`h-full flex flex-col overflow-hidden shrink-0 ${SIDEBAR.bg} border-r border-black/5`}
        >
          <div className="h-12 flex items-center justify-between pl-3 pr-2 shrink-0">
            <h2 className="text-base font-semibold text-text-body whitespace-nowrap">Chat</h2>
            <button
              onClick={() => {
                wantsNewChatRef.current = true;
                setActiveConversationId(null);
                setMessages([]);
                setStreamingContent('');
                setStreamingParts([]);
                setStreamStatus(null);
                window.history.pushState(null, '', getChatUrl());
                navigate(getChatUrl(), { replace: true });
              }}
              className="p-1 rounded bg-white border border-black/10 hover:border-black/20 text-text-secondary hover:text-text-body transition-colors"
              title="New chat"
            >
              <Icon icon={Plus} size={16} />
            </button>
          </div>
          <div className="flex-1 overflow-y-auto px-2 pb-4">
            {conversations.length === 0 ? (
              <p className="text-sm text-text-tertiary px-1 py-2">No chats yet</p>
            ) : (
              <>
              <p className={`text-xs ${SIDEBAR.item} px-1 pb-2`}>Your chats</p>
              <div className="space-y-0.5">
                {conversations.map((conversation) => (
                  <div key={conversation.id} className="group relative">
                    {editingConversationId === conversation.id ? (
                      <div className="px-3 py-2.5">
                        <input
                          type="text"
                          value={editingTitle}
                          onChange={(e) => setEditingTitle(e.target.value)}
                          onKeyDown={(e) => handleRenameKeyDown(e, conversation.id)}
                          onBlur={() => handleRenameSubmit(conversation.id)}
                          autoFocus
                          className="w-full text-sm bg-white border border-border-gray rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-primary"
                        />
                      </div>
                    ) : (
                      <>
                        <button
                          onClick={() => {
                            navigate(getChatUrl(conversation.id));
                          }}
                          className={`w-full flex items-center text-left px-2 h-[32px] rounded-md text-sm transition-colors ${
                            activeConversationId === conversation.id
                              ? SIDEBAR.selected
                              : `${SIDEBAR.item} hover:bg-black/5`
                          }`}
                        >
                          <span className="block truncate pr-6">{conversation.title || 'Untitled'}</span>
                        </button>
                        <div className="absolute right-2 top-1/2 -translate-y-1/2">
                          <button
                            ref={(el) => {
                              if (el) conversationMenuRefs.current.set(conversation.id, el);
                            }}
                            onClick={(e) => {
                              e.stopPropagation();
                              setOpenConversationMenuId(openConversationMenuId === conversation.id ? null : conversation.id);
                            }}
                            className="p-1.5 text-text-tertiary hover:text-text-body hover:bg-bg-gray-light rounded-lg opacity-0 group-hover:opacity-100 transition-all"
                            title="More options"
                          >
                            <EllipsisHorizontalIcon className="w-3.5 h-3.5" />
                          </button>
                          <Dropdown
                            isOpen={openConversationMenuId === conversation.id}
                            onClose={() => setOpenConversationMenuId(null)}
                            trigger={{ current: conversationMenuRefs.current.get(conversation.id) || null }}
                          >
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                handleRenameConversation(conversation.id, conversation.title || '');
                              }}
                              className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
                            >
                              <PencilIcon className="w-3.5 h-3.5" />
                              Rename
                            </button>
                            <button
                              onClick={async (e) => {
                                e.stopPropagation();
                                setOpenConversationMenuId(null);
                                try {
                                  await removeConversation(conversation.id);
                                  if (activeConversationId === conversation.id) {
                                    wantsNewChatRef.current = true;
                                    setMessages([]);
                                    setStreamingContent('');
                                    setStreamingParts([]);
                                    setStreamStatus(null);
                                    setActiveConversationId(null);
                                    window.history.pushState(null, '', getChatUrl());
                                    navigate(getChatUrl(), { replace: true });
                                  }
                                } catch (err) {
                                  console.error('Failed to delete chat:', err);
                                }
                              }}
                              className="w-full px-3 py-1.5 text-left text-sm text-red-500 hover:bg-bg-gray flex items-center gap-2"
                            >
                              <TrashIcon className="w-3.5 h-3.5" />
                              Delete
                            </button>
                          </Dropdown>
                        </div>
                      </>
                    )}
                  </div>
                ))}
              </div>
              </>
            )}
          </div>
        </motion.div>
      )}
    </AnimatePresence>

    <div className="relative flex-1 flex flex-col bg-[#FCFCFC] overflow-hidden rounded-lg">

        {/* Main chat area */}
      <div
        className="relative flex-1 flex flex-col overflow-hidden"
        onDragEnter={handleDragEnter}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
      >
      {/* Toggle sidebar button - top left */}
      <div className="absolute top-3 left-3 z-10">
        <button
          onClick={() => setShowPreviousChats(!showPreviousChats)}
          className="p-1.5 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded-lg transition-colors"
          title="Previous chats"
        >
          <ClockIcon className="w-4 h-4 stroke-2" />
        </button>
      </div>
      {/* Drag-over overlay */}
      {isDragging && (
        <div className="absolute inset-0 z-50 bg-bg-white/80 border-2 border-dashed border-text-tertiary rounded-xl flex items-center justify-center pointer-events-none">
          <p className="text-text-secondary text-sm font-medium">Drop images to attach</p>
        </div>
      )}
      {/* Loading skeleton */}
      {(loadingMessages || isRestoringConversation) && (
        <div className="flex-1 overflow-hidden pt-6 animate-pulse">
          {/* User message skeleton */}
          <div className="py-3">
            <div className="max-w-3xl mx-auto px-4 flex justify-end">
              <div className="bg-bg-gray rounded-[18px] px-4 py-2 max-w-[50%]">
                <div className="h-4 bg-border-secondary/50 rounded w-40" />
              </div>
            </div>
          </div>
          {/* Assistant message skeleton */}
          <div className="py-3">
            <div className="max-w-3xl mx-auto px-4 space-y-3">
              <div className="h-4 bg-bg-gray rounded w-full" />
              <div className="h-4 bg-bg-gray rounded w-[90%]" />
              <div className="h-4 bg-bg-gray rounded w-[75%]" />
              <div className="h-4 bg-bg-gray rounded w-[60%]" />
            </div>
          </div>
          {/* Second user message skeleton */}
          <div className="py-3">
            <div className="max-w-3xl mx-auto px-4 flex justify-end">
              <div className="bg-bg-gray rounded-[18px] px-4 py-2 max-w-[50%]">
                <div className="h-4 bg-border-secondary/50 rounded w-56" />
              </div>
            </div>
          </div>
          {/* Second assistant message skeleton */}
          <div className="py-3">
            <div className="max-w-3xl mx-auto px-4 space-y-3">
              <div className="h-4 bg-bg-gray rounded w-full" />
              <div className="h-4 bg-bg-gray rounded w-[85%]" />
              <div className="h-4 bg-bg-gray rounded w-[70%]" />
            </div>
          </div>
        </div>
      )}

      {/* Messages area */}
      {!isEmpty && !loadingMessages && !isRestoringConversation && (
        <div ref={scrollContainerRef} className="flex-1 overflow-auto">
          <div
            className="relative pt-6 pb-[140px]"
            style={{
              minHeight: containerHeight
                ? containerHeight + Math.max(0, streamingHeight - availableAiSpace + 30)
                : undefined
            }}
          >
            {/* All messages in normal flow */}
            {messages.map((message, index) => {
              const isLastUserMessage = message.role === 'user' &&
                !messages.slice(index + 1).some(m => m.role === 'user');

              return (
                <div key={message.id} ref={isLastUserMessage ? lastUserMessageRef : null}>
                  <ChatMessage
                    role={message.role}
                    content={message.content}
                    contentParts={message.content_parts}
                    messageId={message.id}
                    onRegenerate={message.role === 'assistant' ? handleRegenerate : undefined}
                  />
                </div>
              );
            })}

            {/* Streaming AI - absolutely positioned so it doesn't affect container height initially */}
            {hasStreamingContent && (
              <div
                ref={streamingRef}
                className="absolute left-0 right-0"
                style={{ top: aiResponseTop }}
              >
                {isWaitingForResponse ? (
                  // Waiting indicator with optional status text
                  <div className="group py-4">
                    <div className="max-w-3xl mx-auto px-4">
                      <div className="max-w-[85%] flex items-center gap-2">
                        <span className="inline-block w-3 h-3 bg-text-body rounded-full animate-thinking-dot" />
                        {streamStatus && (
                          <span className="text-sm text-text-tertiary animate-pulse">{streamStatus}</span>
                        )}
                      </div>
                    </div>
                  </div>
                ) : (streamingContent || streamingParts.length > 0) ? (
                  <>
                    {/* Status indicator above streaming content */}
                    {streamStatus && (
                      <div className="max-w-3xl mx-auto px-4 pb-1">
                        <div className="max-w-[85%]">
                          <span className="text-xs text-text-tertiary">{streamStatus}</span>
                        </div>
                      </div>
                    )}
                    {/* Stream content with content parts rendering */}
                    <ChatMessage
                      role="assistant"
                      content={streamingContent}
                      contentParts={streamingParts.length > 0 ? streamingParts : undefined}
                      isStreaming
                    />
                  </>
                ) : null}
              </div>
            )}

            {/* Background fill for the response area */}
            {containerHeight && (
              <div
                className="absolute left-0 right-0 bg-bg-white"
                style={{
                  top: aiResponseTop,
                  height: Math.max(availableAiSpace, streamingHeight) + 160,
                  zIndex: -1
                }}
                aria-hidden="true"
              />
            )}
          </div>
        </div>
      )}

      {/* Single input - always rendered, position changes based on state */}
      {!loadingMessages && (
        <div className={`absolute left-0 right-0 px-4 flex flex-col items-center ${isEmpty ? 'top-1/2 -translate-y-1/2' : 'bottom-0 pb-4 pt-8 bg-[linear-gradient(transparent_50%,var(--color-bg-white)_50%)]'}`}>
          {isEmpty && (
            <h1 className="text-2xl font-semibold text-text-body mb-8 text-center leading-9">
              What's on the agenda?
            </h1>
          )}
          {/* Scroll to bottom button */}
          {showScrollButton && !isEmpty && (
            <button
              onClick={scrollToBottom}
              className="mb-3 w-8 h-8 rounded-full bg-bg-white border border-border-gray shadow-sm flex items-center justify-center text-text-tertiary hover:text-text-body hover:bg-bg-gray-light transition-colors"
              aria-label="Scroll to bottom"
            >
              <ArrowDownIcon className="w-4 h-4 stroke-2" />
            </button>
          )}
          <div style={{ width: '90%', maxWidth: '750px' }}>
            <ChatInput
              value={input}
              onChange={setInput}
              onSubmit={handleSubmitWithAttachments}
              onStop={handleStopStreaming}
              disabled={loading}
              isStreaming={hasStreamingContent}
              placeholder="Ask anything"
              pendingAttachments={pendingAttachments}
              onAddFiles={addFiles}
              onRemoveAttachment={removeAttachment}
              isUploading={isUploading}
              selectedWorkspaceIds={selectedWorkspaceIds}
              workspaces={workspaces}
              onWorkspaceChange={setSelectedWorkspaceIds}
              workspaceLocked={messages.length > 0}
              mentions={mentions}
              onMentionSelect={handleMentionSelect}
              onRemoveMention={handleRemoveMention}
              workspaceId={mentionWorkspaceId}
            />
            {/* Suggestion prompts */}
            {isEmpty && (
              <ul className="mt-4 divide-y divide-border-light px-4">
                {[
                  'What should I work on today?',
                  'Check my recent emails',
                  'Summarize my upcoming calendar events',
                  'What should I focus on this week?',
                ].map((prompt) => (
                  <li key={prompt}>
                    <button
                      onClick={() => { setInput(prompt); }}
                      className="flex w-full items-center gap-2 py-3.5 text-left text-sm text-text-tertiary hover:text-text-body transition-colors cursor-pointer"
                    >
                      <span>{prompt}</span>
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      )}
      </div>
      </div>
    </div>
  );
}
