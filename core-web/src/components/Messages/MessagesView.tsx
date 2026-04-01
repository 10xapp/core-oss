import { avatarGradient } from "../../utils/avatarGradient";
import {
  useState,
  useEffect,
  useLayoutEffect,
  useRef,
  useCallback,
  useMemo,
} from "react";
import { createPortal } from "react-dom";
import { useParams, useNavigate } from "react-router-dom";
import { motion, AnimatePresence } from "motion/react";
import {
  PlusIcon,
  HashtagIcon,
  XMarkIcon,
  ChevronRightIcon,
  ChevronDownIcon,
  LockClosedIcon,
  PencilIcon,
  TrashIcon,
  ChatBubbleOvalLeftIcon,
  FaceSmileIcon,
  DocumentIcon,
  ArrowDownTrayIcon,
  EllipsisHorizontalIcon,
  ArrowUturnLeftIcon,
} from "@heroicons/react/24/outline";
import Dropdown from "../Dropdown/Dropdown";
import { Plus, Users } from 'lucide-react';
import { Icon } from '../ui/Icon';
import { useMessagesStore } from "../../stores/messagesStore";
import { useWorkspaceStore } from "../../stores/workspaceStore";
import { useAuthStore } from "../../stores/authStore";
import { usePresenceStore } from "../../stores/presenceStore";
import { broadcastTyping, stopTyping } from "../../hooks/useWorkspacePresence";
import { getPendingNotificationMessage, clearPendingNotificationMessage } from "../../hooks/useGlobalRealtime";

import { ThreadParticipantAvatars } from "./ThreadParticipantAvatars";
import { MessageComposer, InlineEditEditor } from "./MessageComposer";
import { useKeyboardNavigation } from "../../hooks/useKeyboardNavigation";
import type {
  ContentBlock,
  ChannelMessage,
  DMChannel,
  ChannelMember,
} from "../../api/client";
import {
  getWorkspaceMembers,
  getPresignedUploadUrl,
  confirmFileUpload,
  getChannelMembers,
  addChannelMember,
  removeChannelMember,
  type WorkspaceMember,
} from "../../api/client";
import { isHeicFile, convertHeicToJpeg } from "../../lib/heicConverter";
import { MENTION_ICONS } from "../../types/mention";
import type { MentionEntityType } from "../../types/mention";
import { useMentionNavigation } from "../../hooks/useMentionNavigation";
import { HeaderButtons } from "../MiniAppHeader";
import MessagesSettingsDropdown from "./MessagesSettingsDropdown";
import { resolveUploadMimeType } from "../../utils/uploadMime";
import { SIDEBAR } from "../../lib/sidebar";
import { sanitizeStrictHtml } from "../../utils/sanitizeHtml";

// Common emoji set for quick reactions
const COMMON_EMOJIS = [
  "👍",
  "❤️",
  "😂",
  "😮",
  "😢",
  "🎉",
  "🔥",
  "👀",
  "✅",
  "🙏",
];

// Emoji picker component
function EmojiPicker({
  onSelect,
  onClose,
  position,
}: {
  onSelect: (emoji: string) => void;
  onClose: () => void;
  position: { top: number; left: number };
}) {
  const pickerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (pickerRef.current && !pickerRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [onClose]);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handleEscape);
    return () => document.removeEventListener("keydown", handleEscape);
  }, [onClose]);

  return createPortal(
    <div
      ref={pickerRef}
      className="fixed bg-white rounded-lg shadow-lg border border-border-gray p-2 z-[9999]"
      style={{ top: position.top, left: position.left }}
    >
      <div className="flex gap-1 flex-wrap max-w-[200px]">
        {COMMON_EMOJIS.map((emoji) => (
          <button
            key={emoji}
            onClick={() => {
              onSelect(emoji);
              onClose();
            }}
            className="w-8 h-8 flex items-center justify-center text-lg hover:bg-bg-gray rounded transition-colors"
          >
            {emoji}
          </button>
        ))}
      </div>
    </div>,
    document.body,
  );
}

// Helper to check if a file is an image
function isImageFile(mimeType: string): boolean {
  return mimeType?.startsWith("image/");
}

// Helper to check if a file is a video
function isVideoFile(mimeType: string): boolean {
  return mimeType?.startsWith("video/");
}

// Helper to get image dimensions from a File object
function getImageDimensions(
  file: File,
): Promise<{ width: number; height: number } | null> {
  return new Promise((resolve) => {
    if (!file.type.startsWith("image/")) {
      resolve(null);
      return;
    }

    const img = new Image();
    const url = URL.createObjectURL(file);

    img.onload = () => {
      URL.revokeObjectURL(url);
      resolve({ width: img.naturalWidth, height: img.naturalHeight });
    };

    img.onerror = () => {
      URL.revokeObjectURL(url);
      resolve(null);
    };

    img.src = url;
  });
}

// Helper to get video dimensions from a File object
function getVideoDimensions(
  file: File,
): Promise<{ width: number; height: number } | null> {
  return new Promise((resolve) => {
    if (!file.type.startsWith("video/")) {
      resolve(null);
      return;
    }

    const video = document.createElement("video");
    const url = URL.createObjectURL(file);

    video.onloadedmetadata = () => {
      URL.revokeObjectURL(url);
      resolve({ width: video.videoWidth, height: video.videoHeight });
    };

    video.onerror = () => {
      URL.revokeObjectURL(url);
      resolve(null);
    };

    video.src = url;
  });
}

// Helper to parse text and render URLs as clickable links
function renderTextWithLinks(text: string | undefined | null): React.ReactNode {
  if (!text) return null;
  // URL regex that matches http, https, and www URLs
  const urlRegex = /(https?:\/\/[^\s<]+|www\.[^\s<]+)/gi;
  const parts = text.split(urlRegex);

  return parts.map((part, i) => {
    if (urlRegex.test(part)) {
      // Reset regex lastIndex after test
      urlRegex.lastIndex = 0;
      const href = part.startsWith("www.") ? `https://${part}` : part;
      return (
        <a
          key={i}
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          className="text-brand-primary hover:underline"
        >
          {part}
        </a>
      );
    }
    return part;
  });
}

// Helper to render text content from blocks (text, mention, code)
function renderTextContent(
  blocks: ContentBlock[],
  _options?: { onSharedMessageClick?: (channelId: string) => void },
): React.ReactNode {
  const textBlocks = blocks.filter((b) => b.type !== "file");
  if (textBlocks.length === 0) return null;

  return textBlocks.map((block, index) => {
    switch (block.type) {
      case "text":
        if (block.data.html) {
          return (
            <span
              key={index}
              className="tiptap-rendered"
              dangerouslySetInnerHTML={{ __html: sanitizeStrictHtml((block.data.html as string) || "") }}
            />
          );
        }
        return (
          <span key={index}>
            {renderTextWithLinks(((block.data.content ?? block.data.text) as string) || "")}
          </span>
        );
      case "mention": {
        const entityType = block.data.entity_type as MentionEntityType | undefined;
        const icon = entityType ? MENTION_ICONS[entityType] : "👤";
        return (
          <span
            key={index}
            className="mention"
            data-entity-type={entityType}
            data-entity-id={block.data.entity_id as string}
          >
            {icon} {block.data.display_name as string}
          </span>
        );
      }
      case "code":
        return (
          <code
            key={index}
            className="px-1.5 py-0.5 bg-bg-gray rounded text-sm font-mono"
          >
            {block.data.content as string}
          </code>
        );
      case "shared_message":
        // Handled at Message component level (rendered above the message)
        return null;
      default:
        return <span key={index}>{JSON.stringify(block.data)}</span>;
    }
  });
}

// Video preview component that auto-sizes to video dimensions
function VideoPreview({ url, filename, width, height }: { url: string; filename: string; width?: number; height?: number }) {
  const maxWidth = 384;
  const maxHeight = 320;
  const [containerWidth, setContainerWidth] = useState<number | null>(() => {
    if (width && height) {
      const scaleW = maxWidth / width;
      const scaleH = maxHeight / height;
      const scale = Math.min(scaleW, scaleH, 1);
      return Math.round(width * scale);
    }
    return null;
  });

  const handleLoadedMetadata = (e: React.SyntheticEvent<HTMLVideoElement>) => {
    if (containerWidth !== null) return;
    const video = e.currentTarget;
    const vw = video.videoWidth;
    const vh = video.videoHeight;
    if (vw && vh) {
      const scaleW = maxWidth / vw;
      const scaleH = maxHeight / vh;
      const scale = Math.min(scaleW, scaleH, 1);
      setContainerWidth(Math.round(vw * scale));
    }
  };

  return (
    <div className="mt-2">
      <div
        style={containerWidth ? { width: containerWidth } : { maxWidth }}
        className="rounded-lg border border-border-gray overflow-hidden"
      >
        <video
          src={url}
          controls
          preload="metadata"
          className="block w-full rounded-lg"
          style={{ maxHeight }}
          onLoadedMetadata={handleLoadedMetadata}
        >
          Your browser does not support video playback.
        </video>
      </div>
      <a
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1.5 mt-1 text-xs text-text-tertiary hover:text-text-secondary transition-colors"
      >
        <span>{filename}</span>
        <ArrowDownTrayIcon className="w-3 h-3" />
      </a>
    </div>
  );
}

// Image component with loading placeholder to prevent layout shift
function ImageWithPlaceholder({
  url,
  clickUrl,
  filename,
  displayWidth,
  displayHeight,
  onImageClick,
}: {
  url: string;
  clickUrl?: string;
  filename: string;
  displayWidth: number;
  displayHeight: number;
  onImageClick?: (url: string) => void;
}) {
  const [isLoaded, setIsLoaded] = useState(false);
  const [hasError, setHasError] = useState(false);

  return (
    <div className="mt-2">
      <button
        type="button"
        onClick={() => onImageClick?.(clickUrl || url)}
        className="block cursor-zoom-in"
      >
        {/* Container with fixed dimensions to prevent layout shift */}
        <div
          style={{
            width: displayWidth,
            height: displayHeight,
            minWidth: displayWidth,
            minHeight: displayHeight,
            maxWidth: displayWidth,
            maxHeight: displayHeight,
          }}
          className="rounded-lg border border-border-gray overflow-hidden relative bg-gray-100 flex-shrink-0"
        >
          {/* Loading skeleton with shimmer effect - always present, hidden when loaded */}
          <div
            className="absolute inset-0 bg-gray-100"
            style={{ display: isLoaded || hasError ? 'none' : 'block' }}
          >
            <div className="absolute inset-0 animate-pulse bg-gradient-to-r from-transparent via-white/60 to-transparent" />
          </div>

          {/* Error state */}
          {hasError && (
            <div className="absolute inset-0 flex items-center justify-center bg-gray-50">
              <div className="text-center text-xs text-gray-400">
                <DocumentIcon className="w-8 h-8 mx-auto mb-1 opacity-50" />
                Failed to load
              </div>
            </div>
          )}

          {/* Actual image */}
          <img
            src={url}
            alt={filename}
            width={displayWidth}
            height={displayHeight}
            loading="lazy"
            decoding="async"
            onLoad={() => setIsLoaded(true)}
            onError={() => setHasError(true)}
            style={{
              display: "block",
              width: "100%",
              height: "100%",
              objectFit: "cover",
              opacity: isLoaded ? 1 : 0,
              transition: "opacity 0.15s ease-in-out",
            }}
            className="hover:opacity-90"
          />
        </div>
      </button>
    </div>
  );
}

// Helper to render file blocks (images, attachments) - separate from text
function renderFileContent(blocks: ContentBlock[], onImageClick?: (url: string) => void): React.ReactNode {
  const fileBlocks = blocks.filter((b) => b.type === "file");
  if (fileBlocks.length === 0) return null;

  return fileBlocks.map((block, index) => {
    const inlineUrl =
      (block.data.chat_url as string | undefined) ||
      (block.data.url as string | undefined) ||
      (block.data.preview_url as string | undefined) ||
      (block.data.full_url as string | undefined);
    const fullUrl = (block.data.full_url as string | undefined) || inlineUrl;
    const filename = block.data.filename as string;
    const mimeType = block.data.mime_type as string;
    const width = block.data.width as number | undefined;
    const height = block.data.height as number | undefined;
    const isImage = isImageFile(mimeType);
    const isVideo = isVideoFile(mimeType);

    // If URL is stripped from cache, still render placeholder with dimensions
    if (!inlineUrl) {
      if (isImage && width && height) {
        const maxWidth = 384;
        const maxHeight = 256;
        const scaleW = maxWidth / width;
        const scaleH = maxHeight / height;
        const scale = Math.min(scaleW, scaleH, 1);
        const displayWidth = Math.round(width * scale);
        const displayHeight = Math.round(height * scale);

        return (
          <div key={`file-${index}`} className="mt-2">
            <div
              style={{
                width: displayWidth,
                height: displayHeight,
                minWidth: displayWidth,
                minHeight: displayHeight,
              }}
              className="rounded-lg border border-border-gray bg-gray-100 flex-shrink-0 relative overflow-hidden"
            >
              <div className="absolute inset-0 animate-pulse bg-gradient-to-r from-transparent via-white/60 to-transparent" />
            </div>
          </div>
        );
      }
      // For files without URL, don't render anything (will update when fresh data loads)
      return null;
    }

    if (isVideo) {
      return (
        <VideoPreview key={`file-${index}-${inlineUrl}`} url={inlineUrl} filename={filename} width={width} height={height} />
      );
    }

    if (isImage) {
      // Calculate display dimensions (max 384px wide, 256px tall)
      const maxWidth = 384;
      const maxHeight = 256;
      let displayWidth = width || maxWidth;
      let displayHeight = height || maxHeight;

      if (width && height) {
        // Scale down to fit within max bounds while preserving aspect ratio
        const scaleW = maxWidth / width;
        const scaleH = maxHeight / height;
        const scale = Math.min(scaleW, scaleH, 1); // Don't scale up
        displayWidth = Math.round(width * scale);
        displayHeight = Math.round(height * scale);
      }

      return (
        <ImageWithPlaceholder
          key={`file-${index}-${inlineUrl}`}
          url={inlineUrl}
          clickUrl={fullUrl || inlineUrl}
          filename={filename}
          displayWidth={displayWidth}
          displayHeight={displayHeight}
          onImageClick={onImageClick}
        />
      );
    }

    return (
      <a
        key={`file-${index}-${inlineUrl}`}
        href={inlineUrl}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-2 px-3 py-2 mt-2 bg-bg-gray rounded-lg hover:bg-bg-gray-dark transition-colors"
      >
        <DocumentIcon className="w-4.5 h-4.5 text-text-secondary" />
        <span className="text-sm text-text-body">{filename}</span>
        <ArrowDownTrayIcon className="w-4 h-4 text-text-tertiary" />
      </a>
    );
  });
}

// Format timestamp for display
function formatMessageTime(timestamp: string): string {
  const date = new Date(timestamp);
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

// Message component - Slack-style left-aligned with inline editing
function Message({
  message,
  onEdit,
  onDelete,
  onReaction,
  onRemoveReaction,
  onReply,
  onShare,
  onNavigateToChannel,
  currentUserId,
  isEditing,
  editingContent,
  onSaveEdit,
  onCancelEdit,
  isInThread,
  failedAvatarIds,
  onAvatarError,
  onImageClick,
  getUserInfo,
}: {
  message: ChannelMessage;
  onEdit?: () => void;
  onDelete?: () => void;
  onReaction?: (emoji: string) => void;
  onRemoveReaction?: (emoji: string) => void;
  onReply?: () => void;
  onShare?: () => void;
  onNavigateToChannel?: (channelId: string) => void;
  currentUserId?: string;
  isEditing?: boolean;
  editingContent?: string;
  onSaveEdit?: (content: string, html?: string) => void;
  onCancelEdit?: () => void;
  isInThread?: boolean;
  failedAvatarIds?: Set<string>;
  onAvatarError?: (id: string) => void;
  onImageClick?: (url: string) => void;
  getUserInfo?: (userId: string) => { name?: string; email?: string } | undefined;
}) {
  const [showActions, setShowActions] = useState(false);
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const [emojiPickerPosition, setEmojiPickerPosition] = useState({
    top: 0,
    left: 0,
  });
  const [hoveredReaction, setHoveredReaction] = useState<string | null>(null);
  const hoverTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reactionButtonRef = useRef<HTMLButtonElement>(null);
  const isOwner = currentUserId === message.user_id;
  const navigateToMention = useMentionNavigation();

  const handleContentClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      // Handle mention clicks
      const mentionTarget = (e.target as HTMLElement).closest?.('.mention[data-entity-id]') as HTMLElement | null;
      if (mentionTarget) {
        const entityType = mentionTarget.getAttribute('data-entity-type') as MentionEntityType | undefined;
        const entityId = mentionTarget.getAttribute('data-entity-id');
        if (entityType && entityId) {
          e.preventDefault();
          navigateToMention(entityType, entityId);
        }
      }
    },
    [navigateToMention],
  );

  const handleShowEmojiPicker = () => {
    if (reactionButtonRef.current) {
      const rect = reactionButtonRef.current.getBoundingClientRect();
      setEmojiPickerPosition({
        top: rect.bottom + 4,
        left: Math.max(rect.left - 80, 8), // Center it and keep on screen
      });
    }
    setShowEmojiPicker(true);
  };

  // Check if current user has reacted with a specific emoji
  const hasUserReacted = (emoji: string) => {
    return message.reactions?.some(
      (r) => r.emoji === emoji && r.user_id === currentUserId,
    );
  };

  // Toggle reaction - add if not present, remove if already reacted
  const handleToggleReaction = (emoji: string) => {
    if (hasUserReacted(emoji)) {
      onRemoveReaction?.(emoji);
    } else {
      onReaction?.(emoji);
    }
  };

  // Separate shared_message (reply preview) blocks from content blocks
  const replyBlock = message.blocks.find((b) => b.type === "shared_message");
  const contentBlocks = message.blocks.filter((b) => b.type !== "shared_message");

  const textContent =
    contentBlocks.length > 0
      ? renderTextContent(contentBlocks, { onSharedMessageClick: onNavigateToChannel })
      : message.content;
  const fileContent =
    contentBlocks.length > 0 ? renderFileContent(contentBlocks, onImageClick) : null;

  // Build reply preview data
  const replyData = replyBlock ? (replyBlock.data as {
    original_message_id?: string;
    original_user_name: string;
    original_user_avatar?: string;
    original_channel_id: string;
    original_channel_name: string;
    original_content: string;
    original_blocks: ContentBlock[];
    original_created_at: string;
  }) : null;

  const replySnippet = replyData
    ? (replyData.original_blocks && replyData.original_blocks.length > 0
        ? replyData.original_blocks
            .map((b) => {
              if (b.type === "text") return (b.data.content ?? b.data.text) as string || "";
              if (b.type === "mention") return `@${b.data.display_name as string}`;
              if (b.type === "file") return `[${b.data.filename as string || "file"}]`;
              return "";
            })
            .filter(Boolean)
            .join(" ")
        : replyData.original_content || "")
    : "";

  return (
    <div
      id={`msg-${message.id}`}
      className={`group relative px-4 py-2 transition-colors duration-500 ${isEditing ? "bg-blue-50/50" : "hover:bg-bg-gray/50"}`}
      onMouseEnter={() => !isEditing && setShowActions(true)}
      onMouseLeave={() => setShowActions(false)}
    >
      {/* Reply preview — Discord-style, above the message */}
      {replyData && (
        <div className="relative mb-2.5">
          {/* Connector line: starts at current message's avatar center, goes up then right to reply content */}
          <div
            className="absolute border-l-2 border-t-2 border-text-tertiary/30 rounded-tl-lg pointer-events-none"
            style={{ left: 20, bottom: -10, width: 22, top: 8 }}
          />
          <div
            className="flex items-center gap-1.5 cursor-pointer group/reply"
            style={{ marginLeft: 47 }}
            onClick={() => {
              // Scroll to the original message if it's in the current view
              if (replyData.original_message_id) {
                const el = document.getElementById(`msg-${replyData.original_message_id}`);
                if (el) {
                  el.scrollIntoView({ behavior: "smooth", block: "center" });
                  el.classList.add("bg-brand-primary/5");
                  setTimeout(() => el.classList.remove("bg-brand-primary/5"), 1500);
                  return;
                }
              }
              // Fallback: navigate to the channel
              onNavigateToChannel?.(replyData.original_channel_id);
            }}
            role="button"
            tabIndex={0}
            title="Go to original message"
          >
            {replyData.original_user_avatar ? (
              <img
                src={replyData.original_user_avatar}
                className="w-4 h-4 rounded-full object-cover shrink-0"
                alt=""
              />
            ) : (
              <div
                className="w-4 h-4 rounded-full shrink-0 flex items-center justify-center text-white text-[8px] font-semibold"
                style={{ background: avatarGradient(replyData.original_user_name || "?") }}
              >
                {(replyData.original_user_name || "?").charAt(0).toUpperCase()}
              </div>
            )}
            <span className="text-[13px] font-semibold text-text-secondary group-hover/reply:underline">
              {replyData.original_user_name}
            </span>
            <span className="text-[13px] text-text-tertiary truncate">
              {replySnippet}
            </span>
          </div>
        </div>
      )}

      <div className="flex items-start gap-2.5">
      {/* Avatar */}
      {message.agent_id ? (
        <div
          className="w-10 h-10 rounded-lg flex-shrink-0 mt-[4px] flex items-center justify-center text-white font-semibold text-sm"
          style={{ background: "linear-gradient(135deg, #6366F1 0%, #8B5CF6 100%)" }}
        >
          {(message.agent?.name || "A").charAt(0).toUpperCase()}
        </div>
      ) : message.user?.avatar_url &&
      message.user.avatar_url.length > 0 &&
      !failedAvatarIds?.has(message.id) ? (
        <img
          src={message.user.avatar_url}
          alt={message.user.name || message.user.email || "User"}
          className="w-10 h-10 rounded-lg flex-shrink-0 object-cover mt-[4px]"
          onError={() => onAvatarError?.(message.id)}
        />
      ) : (
        <div
          className="w-10 h-10 rounded-lg flex-shrink-0 mt-[4px] flex items-center justify-center text-white font-semibold text-sm"
          style={{
            background: avatarGradient(message.user?.name || message.user?.email || message.user_id || "?"),
          }}
        >
          {(message.user?.name || message.user?.email || "?").charAt(0).toUpperCase()}
        </div>
      )}
      {/* Message content */}
      <div className="flex-1 min-w-0">
        <div className="flex items-baseline gap-2">
          <span className="font-medium text-text-body">
            {message.agent_id
              ? (message.agent?.name || "Agent")
              : (message.user?.name || message.user?.email || "Unknown")}
          </span>
          {message.agent_id && (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-indigo-100 text-indigo-600 font-medium">
              Agent
            </span>
          )}
          <span className="text-xs text-text-tertiary">
            {formatMessageTime(message.created_at)}
          </span>
          {message.is_edited && (
            <span className="text-xs text-text-tertiary">(edited)</span>
          )}
        </div>

        {/* Show editor or content */}
        {isEditing ? (
          <InlineEditEditor
            initialContent={editingContent || ""}
            initialHtml={
              message.blocks.find((b) => b.type === "text" && b.data.html)?.data
                .html as string | undefined
            }
            onSave={(content, html) => {
              onSaveEdit?.(content, html);
            }}
            onCancel={() => onCancelEdit?.()}
          />
        ) : (
          <>
            {fileContent && (
              <div className={textContent ? "mb-2" : ""}>
                {fileContent}
              </div>
            )}
            {textContent && (
              <div
                className="text-text-body whitespace-pre-wrap text-[15px] leading-relaxed break-words"
                onClick={handleContentClick}
              >
                {textContent}
              </div>
            )}
          </>
        )}

        {/* Reactions - hide while editing */}
        {!isEditing && message.reactions && message.reactions.length > 0 && (
          <div className="flex flex-wrap gap-1 mt-1">
            {Object.entries(
              message.reactions.reduce(
                (acc, r) => {
                  if (!acc[r.emoji]) {
                    acc[r.emoji] = [];
                  }
                  acc[r.emoji].push(r.user_id);
                  return acc;
                },
                {} as Record<string, string[]>,
              ),
            ).map(([emoji, userIds]) => {
              const userReacted = hasUserReacted(emoji);
              // Build tooltip text with user names
              const userNames = userIds.map((userId) => {
                if (userId === currentUserId) return "You";
                const userInfo = getUserInfo?.(userId);
                return userInfo?.name || userInfo?.email || "Unknown";
              });
              // Format: "You, Alice, Bob" or "You and 2 others"
              const tooltipText =
                userNames.length <= 3
                  ? userNames.join(", ")
                  : `${userNames.slice(0, 2).join(", ")} and ${userNames.length - 2} other${userNames.length - 2 > 1 ? "s" : ""}`;

              return (
                <div key={emoji} className="relative">
                  <button
                    onClick={() => handleToggleReaction(emoji)}
                    onMouseEnter={() => {
                      hoverTimeoutRef.current = setTimeout(() => {
                        setHoveredReaction(emoji);
                      }, 150);
                    }}
                    onMouseLeave={() => {
                      if (hoverTimeoutRef.current) {
                        clearTimeout(hoverTimeoutRef.current);
                      }
                      setHoveredReaction(null);
                    }}
                    className={`px-2 py-0.5 rounded-full text-xs transition-colors ${
                      userReacted
                        ? "bg-brand-primary/10 border border-brand-primary/20 hover:bg-brand-primary/20"
                        : "bg-gray-100 border border-gray-100 hover:bg-gray-200"
                    }`}
                  >
                    {emoji} {userIds.length}
                  </button>
                  {hoveredReaction === emoji && (
                    <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 px-2 py-1 text-xs text-white bg-gray-800 rounded whitespace-nowrap z-50">
                      {tooltipText}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
        {/* Thread indicator - hide while editing or in thread view */}
        {!isEditing && !isInThread && message.reply_count > 0 && (
          <button
            onClick={onReply}
            className="mt-1 flex items-center gap-1.5 text-xs text-brand-primary hover:underline"
          >
            <ThreadParticipantAvatars
              messageId={message.id}
              replyCount={message.reply_count}
            />
            <span>
              {message.reply_count}{" "}
              {message.reply_count === 1 ? "reply" : "replies"}
            </span>
          </button>
        )}
      </div>
      </div>{/* end flex wrapper for avatar + content */}

      {/* Message actions - hide while editing */}
      {showActions && !isEditing && (
        <div className="absolute top-1 right-4 z-10 flex items-center gap-0.5 bg-white border border-border-gray rounded-lg shadow-sm p-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
          <button
            ref={reactionButtonRef}
            onClick={handleShowEmojiPicker}
            className="p-1 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded"
            title="Add reaction"
          >
            <FaceSmileIcon className="w-4 h-4" />
          </button>
          {!isInThread && (
            <button
              onClick={onReply}
              className="p-1 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded"
              title="Reply in thread"
            >
              <ChatBubbleOvalLeftIcon className="w-4 h-4" />
            </button>
          )}
          <button
            onClick={onShare}
            className="p-1 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded"
            title="Reply"
          >
            <ArrowUturnLeftIcon className="w-4 h-4" />
          </button>
          {isOwner && (
            <>
              <button
                onClick={onEdit}
                className="p-1 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded"
              >
                <PencilIcon className="w-4 h-4" />
              </button>
              <button
                onClick={onDelete}
                className="p-1 text-text-tertiary hover:text-red-500 hover:bg-bg-gray rounded"
              >
                <TrashIcon className="w-4 h-4" />
              </button>
            </>
          )}
        </div>
      )}

      {/* Emoji picker */}
      {showEmojiPicker && (
        <EmojiPicker
          position={emojiPickerPosition}
          onSelect={(emoji) => handleToggleReaction(emoji)}
          onClose={() => setShowEmojiPicker(false)}
        />
      )}
    </div>
  );
}

// Cached message list component - stays mounted to preserve images
function CachedChannelMessages({
  channelId,
  isActive,
  messages,
  currentUserId,
  onEdit,
  onDelete,
  onReaction,
  onRemoveReaction,
  onReply,
  onShare,
  onNavigateToChannel,
  editingMessageId,
  editingContent,
  onSaveEdit,
  onCancelEdit,
  failedAvatarIds,
  onAvatarError,
  onImageClick,
  getUserInfo,
}: {
  channelId: string;
  isActive: boolean;
  messages: ChannelMessage[];
  currentUserId?: string;
  onEdit: (message: ChannelMessage) => void;
  onDelete: (messageId: string) => void;
  onReaction: (messageId: string, emoji: string) => void;
  onRemoveReaction: (messageId: string, emoji: string) => void;
  onReply: (messageId: string) => void;
  onShare: (message: ChannelMessage) => void;
  onNavigateToChannel?: (channelId: string) => void;
  editingMessageId: string | null;
  editingContent: string;
  onSaveEdit: (content: string, html?: string) => void;
  onCancelEdit: () => void;
  failedAvatarIds: Set<string>;
  onAvatarError: (id: string) => void;
  onImageClick?: (url: string) => void;
  getUserInfo?: (userId: string) => { name?: string; email?: string } | undefined;
}) {
  // Deduplicate messages
  const uniqueMessages = messages.filter(
    (msg, index, self) => index === self.findIndex((m) => m.id === msg.id),
  );

  // Helper to format date divider labels
  const formatDateLabel = (date: Date): string => {
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    const msgDate = new Date(
      date.getFullYear(),
      date.getMonth(),
      date.getDate(),
    );

    if (msgDate.getTime() === today.getTime()) return "Today";
    if (msgDate.getTime() === yesterday.getTime()) return "Yesterday";
    return date.toLocaleDateString("en-US", {
      weekday: "long",
      month: "long",
      day: "numeric",
      year: date.getFullYear() !== now.getFullYear() ? "numeric" : undefined,
    });
  };

  return (
    <div
      className={`pt-4 pb-2 ${isActive ? "" : "hidden"}`}
      data-channel-id={channelId}
    >
      {uniqueMessages.map((message, index) => {
        const msgDate = new Date(message.created_at);
        const prevDate =
          index > 0 ? new Date(uniqueMessages[index - 1].created_at) : null;
        // Only show divider between messages from different days (skip the first message — channel banner covers it)
        const showDivider =
          prevDate != null &&
          (msgDate.getFullYear() !== prevDate.getFullYear() ||
            msgDate.getMonth() !== prevDate.getMonth() ||
            msgDate.getDate() !== prevDate.getDate());

        return (
          <div key={message.id}>
            {showDivider && (
              <div className="flex items-center gap-3 my-4 px-6">
                <div className="flex-1 border-t border-border-gray" />
                <span className="text-xs font-medium text-text-tertiary">
                  {formatDateLabel(msgDate)}
                </span>
                <div className="flex-1 border-t border-border-gray" />
              </div>
            )}
            <Message
              message={message}
              currentUserId={currentUserId}
              onEdit={() => onEdit(message)}
              onDelete={() => onDelete(message.id)}
              onReaction={(emoji) => onReaction(message.id, emoji)}
              onRemoveReaction={(emoji) => onRemoveReaction(message.id, emoji)}
              onReply={() => onReply(message.id)}
              onShare={() => onShare(message)}
              onNavigateToChannel={onNavigateToChannel}
              isEditing={editingMessageId === message.id}
              editingContent={
                editingMessageId === message.id ? editingContent : ""
              }
              onSaveEdit={onSaveEdit}
              onCancelEdit={onCancelEdit}
              failedAvatarIds={failedAvatarIds}
              onAvatarError={onAvatarError}
              onImageClick={onImageClick}
              getUserInfo={getUserInfo}
            />
          </div>
        );
      })}
    </div>
  );
}

function ForwardPreviewBar({
  message,
  onCancel,
}: {
  message: ChannelMessage;
  onCancel: () => void;
}) {
  const previewText = message.blocks
    ?.map((block) => {
      if (block.type === "text") return (block.data.content ?? block.data.text) as string || "";
      if (block.type === "mention") return `@${block.data.display_name as string}`;
      if (block.type === "file") return `[${block.data.filename as string || "file"}]`;
      return "";
    })
    .filter(Boolean)
    .join(" ") || "";

  return (
    <div className="flex items-center gap-1.5 px-4 py-2 bg-bg-gray/60 border border-b-0 border-border-gray rounded-t-lg">
      <div className="w-3 h-3 border-l-2 border-t-2 border-text-tertiary/30 rounded-tl-md shrink-0 self-center" />
      {message.user?.avatar_url ? (
        <img
          src={message.user.avatar_url}
          className="w-4 h-4 rounded-full object-cover shrink-0"
          alt=""
        />
      ) : (
        <div
          className="w-4 h-4 rounded-full shrink-0 flex items-center justify-center text-white text-[8px] font-semibold"
          style={{ background: avatarGradient(message.user?.name || message.user?.email || "?") }}
        >
          {(message.user?.name || message.user?.email || "?").charAt(0).toUpperCase()}
        </div>
      )}
      <span className="text-xs font-semibold text-text-secondary shrink-0">
        {message.user?.name || message.user?.email || "Unknown"}
      </span>
      <span className="text-xs text-text-tertiary truncate flex-1 min-w-0">
        {previewText}
      </span>
      <button
        onClick={onCancel}
        className="p-1 text-text-tertiary hover:text-text-body rounded transition-colors shrink-0"
        title="Cancel"
      >
        <XMarkIcon className="w-4 h-4" />
      </button>
    </div>
  );
}

export default function MessagesView() {
  const { workspaceId, channelId: urlChannelId } = useParams<{ workspaceId: string; channelId?: string }>();
  const navigate = useNavigate();
  const { workspaces } = useWorkspaceStore();
  const workspace = workspaces.find((w) => w.id === workspaceId);
  const { user } = useAuthStore();

  // Messages store
  const {
    channels,
    dms,
    messages,
    messagesCache,
    workspaceCache,
    visitedChannelIds,
    threadReplies,
    activeChannelId,
    activeThreadId,
    unreadCounts,
    isLoadingChannels,
    isLoadingDMs,
    isLoadingMessages,
    isLoadingThread,
    error,
    workspaceAppId,
    setWorkspaceAppId,
    setActiveChannel,
    setActiveThread,
    clearThread,
    startDM,
    addChannel,
    editChannel,
    removeChannel,
    sendMessage,
    addOptimisticMessage,
    finalizeOptimisticMessage,
    removeOptimisticMessage,
    editMessage,
    removeMessage,
    addReaction,
    removeReaction,
    preloadAllChannels,
    markAsRead,
    fetchOlderMessages,
    hasMoreMessages,
    isLoadingOlderMessages,
  } = useMessagesStore();

  // Presence & typing (hook runs in App.tsx; broadcast functions are module-level)
  const onlineUserIds = usePresenceStore((s) => s.onlineUserIds);
  const typingUsers = usePresenceStore((s) => s.typingUsers);

  // Local state
  const [searchQuery] = useState("");
  const [showChannelsExpanded, setShowChannelsExpanded] = useState(true);
  const [showDMsExpanded, setShowDMsExpanded] = useState(true);
  const [showCreateChannel, setShowCreateChannel] = useState(false);
  const [showNewDM, setShowNewDM] = useState(false);
  const [newChannelName, setNewChannelName] = useState("");
  const [newChannelDescription, setNewChannelDescription] = useState("");
  const [newChannelPrivate, setNewChannelPrivate] = useState(false);
  const [isCreatingChannel, setIsCreatingChannel] = useState(false);
  const [editingMessageId, setEditingMessageId] = useState<string | null>(null);
  const [editingContent, setEditingContent] = useState("");
  const [workspaceMembers, setWorkspaceMembers] = useState<
    (WorkspaceMember & { email?: string; name?: string; avatar_url?: string })[]
  >([]);
  const [workspaceMembersForId, setWorkspaceMembersForId] = useState<string | null>(null);
  const [isLoadingMembers, setIsLoadingMembers] = useState(false);
  const [showChannelDetails, setShowChannelDetails] = useState(false);
  const [channelDetailsTab, setChannelDetailsTab] = useState<"channel" | "app">("channel");
  const [modalChannelId, setModalChannelId] = useState<string | null>(null);
  const [showAdvancedOptions, setShowAdvancedOptions] = useState(false);
  const channelContentRef = useRef<HTMLDivElement>(null);
  const [channelMembers, setChannelMembers] = useState<ChannelMember[]>([]);
  const [channelMembersForId, setChannelMembersForId] = useState<string | null>(null);
  const [isLoadingChannelMembers, setIsLoadingChannelMembers] =
    useState(false);
  const [channelMembersError, setChannelMembersError] = useState<string | null>(
    null,
  );
  const [showAddMember, setShowAddMember] = useState(false);
  const [addMemberUserId, setAddMemberUserId] = useState("");
  const scrollableContainerRef = useRef<HTMLDivElement>(null);
  const loadMoreSentinelRef = useRef<HTMLDivElement>(null);
  const hasScrolledRef = useRef(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [pendingFiles, setPendingFiles] = useState<
    { file: File; preview?: string }[]
  >([]);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [lightboxImageUrl, setLightboxImageUrl] = useState<string | null>(null);

  const [forwardingMessage, setForwardingMessage] = useState<ChannelMessage | null>(null);
  const [threadPendingFiles, setThreadPendingFiles] = useState<
    { file: File; preview?: string }[]
  >([]);
  const [isThreadUploading, setIsThreadUploading] = useState(false);
  const threadFileInputRef = useRef<HTMLInputElement>(null);
  const [failedAvatarIds, setFailedAvatarIds] = useState<Set<string>>(
    new Set(),
  );
  const [editingChannelName, setEditingChannelName] = useState("");
  const [editingChannelDescription, setEditingChannelDescription] = useState("");
  const [isSavingChannel, setIsSavingChannel] = useState(false);
  const [showChannelsSectionMenu, setShowChannelsSectionMenu] = useState(false);
  const [showDMsSectionMenu, setShowDMsSectionMenu] = useState(false);
  const channelsSectionMenuRef = useRef<HTMLButtonElement>(null);
  const dmsSectionMenuRef = useRef<HTMLButtonElement>(null);
  const [showSettingsDropdown, setShowSettingsDropdown] = useState(false);
  const settingsButtonRef = useRef<HTMLButtonElement>(null);

  // Handle avatar image load errors
  const handleAvatarError = (id: string) => {
    setFailedAvatarIds((prev) => new Set(prev).add(id));
  };

  // Find the messages workspace app for this workspace
  const messagesApp = workspace?.apps?.find(
    (app: (typeof workspace.apps)[0]) => app.type === "messages",
  );

  // Store is updated BEFORE navigation (in Sidebar), so use store data directly
  // Fallback to cache only for direct URL navigation (not through sidebar)
  const isStoreInSync = workspaceAppId === messagesApp?.id;
  const targetCache = messagesApp?.id ? workspaceCache[messagesApp.id] : null;
  const safeDms = isStoreInSync ? dms : (targetCache?.dms || []);
  const safeChannels = isStoreInSync ? channels : (targetCache?.channels || []);
  // Use cached activeChannelId when store isn't synced to prevent "Select a conversation..." flash
  const safeActiveChannelId = isStoreInSync ? activeChannelId : (targetCache?.activeChannelId || null);
  const activeConversationId = safeActiveChannelId;

  // Only show workspace members if they belong to the current workspace
  const safeWorkspaceMembers = workspaceMembersForId === workspace?.id ? workspaceMembers : [];
  // Only show channel members if they belong to the target channel (modal channel if open, otherwise active channel)
  const targetChannelIdForMembers = modalChannelId || safeActiveChannelId;
  const safeChannelMembers = channelMembersForId === targetChannelIdForMembers ? channelMembers : [];
  const visibleError = uploadError || error;

  // Sync store when navigating directly via URL (not through sidebar click)
  // Handles both workspace changes and channel changes within the same workspace
  useEffect(() => {
    const needsWorkspaceSync = messagesApp?.id && workspaceAppId !== messagesApp.id;
    const needsChannelSync = urlChannelId && urlChannelId !== activeChannelId;

    if (needsWorkspaceSync) {
      // Switching workspaces - pass urlChannelId so setWorkspaceAppId sets the channel
      setWorkspaceAppId(messagesApp.id, urlChannelId);
    } else if (needsChannelSync) {
      // Same workspace, different channel - just update the channel
      setActiveChannel(urlChannelId);
    }
  }, [messagesApp?.id, workspaceAppId, setWorkspaceAppId, urlChannelId, activeChannelId, setActiveChannel]);

  // Clear forwarding state on channel switch
  useEffect(() => {
    setForwardingMessage(null);
  }, [activeChannelId]);

  // Escape key cancels forwarding
  useEffect(() => {
    if (!forwardingMessage) return;
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setForwardingMessage(null);
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [forwardingMessage]);

  // Eagerly fetch workspace members so teammates appear in the DMs sidebar immediately
  useEffect(() => {
    if (workspace?.id && (workspaceMembers.length === 0 || workspaceMembersForId !== workspace.id)) {
      getWorkspaceMembers(workspace.id).then((members) => {
        const filtered = members.filter((m) => m.user_id !== user?.id);
        setWorkspaceMembers(filtered);
        setWorkspaceMembersForId(workspace.id);
      }).catch((err) => {
        console.error("Failed to load workspace members:", err);
      });
    }
  }, [workspace?.id]);

  // Handle notification message from module-level variable
  // This ensures the message that triggered the notification is always displayed
  useEffect(() => {
    const pending = getPendingNotificationMessage();
    if (!pending) return;
    if (pending.channelId !== urlChannelId) return;
    // Wait until activeChannelId matches what we expect
    if (activeChannelId !== urlChannelId) return;

    const notificationMessage = pending.message;
    const channelId = pending.channelId;

    // Clear now that we're processing
    clearPendingNotificationMessage();

    // Check if message is already displayed
    const alreadyDisplayed = messages.some((m) => m.id === notificationMessage.id);
    if (alreadyDisplayed) return;

    // Add the message directly to the store
    useMessagesStore.setState((s) => {
      // Double-check conditions
      if (s.activeChannelId !== channelId) return s;
      if (s.messages.some((m) => m.id === notificationMessage.id)) return s;

      // Add to messages and cache
      const updatedMessages = [...s.messages, notificationMessage].sort(
        (a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
      );

      const existingCache = s.messagesCache[channelId] || [];
      const updatedCache = existingCache.some((m) => m.id === notificationMessage.id)
        ? existingCache
        : [...existingCache, notificationMessage].sort(
            (a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
          );

      return {
        messages: updatedMessages,
        messagesCache: {
          ...s.messagesCache,
          [channelId]: updatedCache,
        },
      };
    });
  }, [urlChannelId, activeChannelId, messages]);

  // When landing on /messages without a channelId, silently update URL to reflect persisted selection
  useEffect(() => {
    if (!urlChannelId && safeActiveChannelId && workspaceId) {
      window.history.replaceState(null, '', `/workspace/${workspaceId}/messages/${safeActiveChannelId}`);
    }
  }, [urlChannelId, safeActiveChannelId, workspaceId]);

  // Keep URL in sync when store changes the active channel programmatically
  useEffect(() => {
    if (activeChannelId && activeChannelId !== urlChannelId && workspaceId) {
      window.history.replaceState(null, '', `/workspace/${workspaceId}/messages/${activeChannelId}`);
    }
  }, [activeChannelId, workspaceId]);

  const latestVisibleMessageId = messages[messages.length - 1]?.id;

  const navigateToChannel = useCallback((channelId: string) => {
    navigate(`/workspace/${workspaceId}/messages/${channelId}`);
  }, [navigate, workspaceId]);

  // Track workspace ID to clear local state only when actually changing workspaces
  const prevWorkspaceIdRef = useRef(workspace?.id);
  useEffect(() => {
    if (workspace?.id && prevWorkspaceIdRef.current !== workspace.id) {
      prevWorkspaceIdRef.current = workspace.id;
      setWorkspaceMembers([]);
      setChannelMembers([]);
    }
  }, [workspace?.id]);

  // Keep the viewed conversation read. This is the single owner for
  // mark-as-read side effects: initial entry, direct URL loads, and
  // newly arrived messages in the open conversation.
  useEffect(() => {
    if (activeConversationId) {
      markAsRead(activeConversationId);
    }
  }, [activeConversationId, latestVisibleMessageId, markAsRead]);

  // Preload all channel messages in background once channels/DMs are loaded
  useEffect(() => {
    if (channels.length > 0 || dms.length > 0) {
      // Small delay to let initial channel load first, then preload others
      const timer = setTimeout(() => {
        preloadAllChannels();
      }, 500);
      return () => clearTimeout(timer);
    }
  }, [channels.length, dms.length, preloadAllChannels]);

  // Reset scroll flag when channel changes (must be useLayoutEffect to run before scroll logic)
  useLayoutEffect(() => {
    hasScrolledRef.current = false;
  }, [activeConversationId]);

  // Fetch channel members when active channel changes (for header count and modal)
  useEffect(() => {
    if (!activeConversationId) return;

    // Clear stale members and show loading state immediately
    setChannelMembers([]);
    setChannelMembersForId(null);
    setIsLoadingChannelMembers(true);

    const fetchMembers = async () => {
      try {
        const data = await getChannelMembers(activeConversationId);
        setChannelMembers(data.members || []);
        setChannelMembersForId(activeConversationId);
      } catch (err) {
        console.error(
          `[Channel Members] Failed to fetch members for header count:`,
          err,
        );
      } finally {
        setIsLoadingChannelMembers(false);
      }
    };

    fetchMembers();
  }, [activeConversationId]);

  // Preload workspace members for add member feature and DM sidebar
  useEffect(() => {
    if (!workspace?.id) {
      setWorkspaceMembers([]);
      return;
    }

    // Clear members immediately when workspace changes to prevent showing stale members
    setWorkspaceMembers([]);

    const fetchMembers = async () => {
      try {
        const members = await getWorkspaceMembers(workspace.id);
        const filtered = members.filter((m) => m.user_id !== user?.id);
        setWorkspaceMembers(filtered);
      } catch (err) {
        console.error("Failed to preload workspace members:", err);
        setWorkspaceMembers([]);
      }
    };

    fetchMembers();
  }, [workspace?.id, user?.id]);

  // With column-reverse, scrollTop=0 is already at the bottom.
  // We only need to ensure we snap to bottom on initial load and smooth-scroll for new messages.
  useLayoutEffect(() => {
    if (messages.length === 0 || isLoadingMessages) return;
    if (hasScrolledRef.current) return;

    const container = scrollableContainerRef.current;
    if (!container) return;

    // column-reverse: scrollTop=0 is the bottom. Ensure we're there on first load.
    requestAnimationFrame(() => {
      container.scrollTop = 0;
    });
    hasScrolledRef.current = true;
  }, [messages, isLoadingMessages]);

  // Smooth scroll to bottom for new messages after initial load
  // Track the last message ID to distinguish new messages from older messages being prepended
  const lastMessageIdRef = useRef<string | null>(null);
  useEffect(() => {
    if (messages.length === 0 || isLoadingMessages) return;
    if (!hasScrolledRef.current) return;

    const newLastId = messages[messages.length - 1]?.id;
    const prevLastId = lastMessageIdRef.current;
    lastMessageIdRef.current = newLastId;

    // Only auto-scroll if the newest message changed (new message arrived)
    // Don't scroll when older messages are prepended (last message stays the same)
    if (prevLastId && newLastId === prevLastId) return;

    const container = scrollableContainerRef.current;
    if (!container) return;

    // column-reverse: scrollTop=0 is the bottom
    container.scrollTo({ top: 0, behavior: "smooth" });
  }, [messages.length]);

  // Infinite scroll: load older messages when sentinel becomes visible
  const channelHasMore = safeActiveChannelId ? hasMoreMessages[safeActiveChannelId] : undefined;
  useEffect(() => {
    const sentinel = loadMoreSentinelRef.current;
    const container = scrollableContainerRef.current;
    if (!sentinel || !container || channelHasMore !== true) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && safeActiveChannelId) {
          fetchOlderMessages(safeActiveChannelId);
        }
      },
      { root: container, rootMargin: '200px' }
    );

    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [safeActiveChannelId, fetchOlderMessages, channelHasMore]);

  const currentChannel = safeChannels.find((c) => c.id === safeActiveChannelId);
  const modalChannel = safeChannels.find((c) => c.id === modalChannelId);
  const currentDM = safeDms.find((d) => d.id === safeActiveChannelId);
  const isInDM = !!currentDM;

  // For DMs, get display name from participants
  const getDMDisplayName = (dm: DMChannel) => {
    const otherParticipants =
      dm.participants?.filter((p) => p.id !== user?.id) || [];
    // Self-DM: all participants are the current user
    if (otherParticipants.length === 0) {
      const self = dm.participants?.find((p) => p.id === user?.id);
      const name = self?.name || self?.email?.split("@")[0] || "You";
      return `${name} (you)`;
    }
    return otherParticipants
      .map((p) => p.name || p.email?.split("@")[0] || "Unknown")
      .join(", ");
  };

  // Look up user info by ID for reaction tooltips
  // Uses workspace members as primary source, falls back to message authors
  const getUserInfo = useCallback(
    (userId: string): { name?: string; email?: string } | undefined => {
      // Check workspace members first
      const member = workspaceMembers.find((m) => m.user_id === userId);
      if (member) {
        return { name: member.name, email: member.email };
      }
      // Fall back to checking message authors in current channel
      const msgAuthor = messages.find((m) => m.user?.id === userId)?.user;
      if (msgAuthor) {
        return { name: msgAuthor.name, email: msgAuthor.email };
      }
      // Check thread replies too
      const threadAuthor = threadReplies.find((m) => m.user?.id === userId)?.user;
      if (threadAuthor) {
        return { name: threadAuthor.name, email: threadAuthor.email };
      }
      return undefined;
    },
    [workspaceMembers, messages, threadReplies]
  );

  // Deduplicate messages (safety net for realtime race conditions)
  const uniqueMessages = messages.filter(
    (msg, index, self) => index === self.findIndex((m) => m.id === msg.id),
  );

  const filteredChannels = safeChannels.filter(
    (ch) =>
      ch.name && ch.name.toLowerCase().includes(searchQuery.toLowerCase()),
  );

  // Combined list of navigable channel/DM IDs for keyboard navigation
  const sidebarNavIds = useMemo(() => {
    const ids: string[] = [];
    if (showChannelsExpanded) {
      filteredChannels.forEach((ch) => ids.push(ch.id));
    }
    if (showDMsExpanded) {
      safeDms.forEach((dm) => ids.push(dm.id));
    }
    return ids;
  }, [filteredChannels, safeDms, showChannelsExpanded, showDMsExpanded]);

  // Keyboard navigation context for zone switching
  const { activeZone, setActiveZone } = useKeyboardNavigation();
  const messagesSidebarRef = useRef<HTMLDivElement>(null);

  // Focus Messages sidebar when zone becomes active
  useEffect(() => {
    if (activeZone === "app-sidebar") {
      // Use setTimeout to ensure DOM is ready
      setTimeout(() => {
        messagesSidebarRef.current?.focus();
      }, 0);
    }
  }, [activeZone]);

  // Simple keyboard navigation - arrow keys change active channel directly
  const handleSidebarKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "ArrowLeft") {
        // Switch to main sidebar
        e.preventDefault();
        setActiveZone("main-sidebar");
        return;
      }

      // Only process up/down when this zone is active
      if (activeZone !== "app-sidebar") return;

      if (sidebarNavIds.length === 0) return;

      const currentIndex = safeActiveChannelId
        ? sidebarNavIds.indexOf(safeActiveChannelId)
        : -1;

      if (e.key === "ArrowDown") {
        e.preventDefault();
        const nextIndex =
          currentIndex < sidebarNavIds.length - 1 ? currentIndex + 1 : 0;
        navigateToChannel(sidebarNavIds[nextIndex]);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        const prevIndex =
          currentIndex > 0 ? currentIndex - 1 : sidebarNavIds.length - 1;
        navigateToChannel(sidebarNavIds[prevIndex]);
      }
    },
    [
      sidebarNavIds,
      safeActiveChannelId,
      navigateToChannel,
      setActiveZone,
      activeZone,
    ],
  );

  // Process and add files to pending list
  const addFiles = async (files: File[]) => {
    if (files.length === 0) return;
    setUploadError(null);

    const processedFiles: { file: File; preview?: string }[] = [];

    for (const rawFile of files) {
      let file = rawFile;

      if (isHeicFile(rawFile)) {
        try {
          file = await convertHeicToJpeg(rawFile);
        } catch (err) {
          console.error("HEIC conversion failed:", rawFile.name, err);
        }
      }

      const mimeType = resolveUploadMimeType(file);
      processedFiles.push({
        file,
        preview:
          mimeType.startsWith("image/") || mimeType.startsWith("video/")
            ? URL.createObjectURL(file)
            : undefined,
      });
    }

    setPendingFiles((prev) => [...prev, ...processedFiles]);
  };

  // Handle file selection from input element
  const handleFileInputChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files || files.length === 0) return;
    await addFiles(Array.from(files));
    // Reset input so same file can be selected again
    e.target.value = "";
  };

  // Handle file selection - either opens picker or processes provided files
  const handleFileSelect = (files?: File[]) => {
    if (files && files.length > 0) {
      addFiles(files);
    } else {
      fileInputRef.current?.click();
    }
  };

  // Thread file handling
  const addThreadFiles = async (files: File[]) => {
    if (files.length === 0) return;

    const processedFiles: { file: File; preview?: string }[] = [];

    for (const rawFile of files) {
      let file = rawFile;

      if (isHeicFile(rawFile)) {
        try {
          file = await convertHeicToJpeg(rawFile);
        } catch (err) {
          console.error("HEIC conversion failed:", rawFile.name, err);
        }
      }

      const mimeType = resolveUploadMimeType(file);
      processedFiles.push({
        file,
        preview:
          mimeType.startsWith("image/") || mimeType.startsWith("video/")
            ? URL.createObjectURL(file)
            : undefined,
      });
    }

    setThreadPendingFiles((prev) => [...prev, ...processedFiles]);
  };

  const handleThreadFileInputChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files || files.length === 0) return;
    await addThreadFiles(Array.from(files));
    e.target.value = "";
  };

  const handleThreadFileSelect = (files?: File[]) => {
    if (files && files.length > 0) {
      addThreadFiles(files);
    } else {
      threadFileInputRef.current?.click();
    }
  };

  const removeThreadPendingFile = (index: number) => {
    setThreadPendingFiles((prev) => {
      const removed = prev[index];
      if (removed.preview) URL.revokeObjectURL(removed.preview);
      return prev.filter((_, i) => i !== index);
    });
  };

  // Remove pending file
  const removePendingFile = (index: number) => {
    setPendingFiles((prev) => {
      const removed = prev[index];
      if (removed.preview) URL.revokeObjectURL(removed.preview);
      return prev.filter((_, i) => i !== index);
    });
  };

  // Upload files and get blocks
  const uploadFiles = async (
    files: { file: File; preview?: string }[],
    cachedDimensions?: Map<File, { width: number; height: number }>,
  ): Promise<{ blocks: ContentBlock[]; failedFiles: string[] }> => {
    if (!messagesApp) return { blocks: [], failedFiles: [] };

    const fileBlocks: ContentBlock[] = [];
    const failedFiles: string[] = [];
    const maxParallelUploads = 3;

    for (let i = 0; i < files.length; i += maxParallelUploads) {
      const batch = files.slice(i, i + maxParallelUploads);
      const batchResults = await Promise.all(
        batch.map(async ({ file }) => {
          try {
            const mimeType = resolveUploadMimeType(file);

            // Get presigned URL
            console.log(
              "📤 Getting presigned URL for:",
              file.name,
              mimeType,
              file.size,
            );
            const uploadInfo = await getPresignedUploadUrl({
              workspaceAppId: messagesApp.id,
              filename: file.name,
              contentType: mimeType,
              fileSize: file.size,
              createDocument: false, // Don't create a document in files app
            });
            console.log("📤 Got presigned URL:", uploadInfo);

            // Upload to R2 with required headers
            console.log("📤 Uploading to R2...");
            const uploadResponse = await fetch(uploadInfo.upload_url, {
              method: "PUT",
              headers: uploadInfo.headers,
              body: file,
            });

            console.log(
              "📤 R2 response:",
              uploadResponse.status,
              uploadResponse.statusText,
            );
            if (!uploadResponse.ok) {
              const errorText = await uploadResponse.text();
              console.error(
                "❌ R2 upload failed:",
                uploadResponse.status,
                errorText,
              );
              failedFiles.push(file.name);
              return null;
            }
            console.log("✅ R2 upload successful");

            // Confirm upload with workspace app ID
            const confirmResult = await confirmFileUpload(uploadInfo.file_id, {
              workspaceAppId: messagesApp.id,
              createDocument: false,
            });

            // Use the public_url from the confirmed file
            const fileUrl = confirmResult.file.public_url || uploadInfo.public_url;

            // Reuse dimensions from optimistic phase instead of recomputing
            const dimensions = cachedDimensions?.get(file);

            // Add file block with dimensions for images
            return {
              type: "file",
              data: {
                url: fileUrl,
                filename: file.name,
                mime_type: mimeType,
                size: file.size,
                file_id: confirmResult.file.id,
                r2_key: uploadInfo.r2_key,
                ...(dimensions && {
                  width: dimensions.width,
                  height: dimensions.height,
                }),
              },
            } as ContentBlock;
          } catch (err) {
            console.error("Failed to upload file:", file.name, err);
            failedFiles.push(file.name);
            return null;
          }
        }),
      );

      fileBlocks.push(
        ...batchResults.filter((block): block is ContentBlock => block !== null),
      );
    }

    return { blocks: fileBlocks, failedFiles };
  };

  const handleSendMessage = async (textBlocks?: ContentBlock[]) => {
    if (isUploading) return;

    const filesToUpload = [...pendingFiles];
    const hasFiles = filesToUpload.length > 0;
    const hasText = textBlocks && textBlocks.length > 0;

    const hasForward = !!forwardingMessage;

    if (!hasFiles && !hasText && !hasForward) return;

    // Build shared_message block for forwarding
    const forwardBlock: ContentBlock | null = hasForward ? {
      type: 'shared_message',
      data: {
        original_message_id: forwardingMessage.id,
        original_channel_id: forwardingMessage.channel_id,
        original_channel_name:
          safeChannels.find((c) => c.id === forwardingMessage.channel_id)?.name
          || (() => {
            const dm = safeDms.find((d) => d.id === forwardingMessage.channel_id);
            if (!dm) return "unknown";
            const other = dm.participants?.find((p) => p.id !== user?.id);
            return other?.name || other?.email?.split("@")[0] || "DM";
          })(),
        original_user_name: forwardingMessage.user?.name || forwardingMessage.user?.email || 'Unknown',
        original_user_avatar: forwardingMessage.user?.avatar_url,
        original_content: forwardingMessage.content,
        original_blocks: forwardingMessage.blocks,
        original_created_at: forwardingMessage.created_at,
      },
    } : null;

    // If no files, use the regular optimistic send
    if (!hasFiles) {
      setUploadError(null);
      const blocks: ContentBlock[] = [...(textBlocks || [])];
      if (forwardBlock) blocks.push(forwardBlock);
      setForwardingMessage(null);
      sendMessage(blocks);
      return;
    }

    // Clear pending files immediately for optimistic UI
    setUploadError(null);
    setPendingFiles([]);
    setIsUploading(true);
    let shouldRevokePreviews = false;

    // Build optimistic file blocks in parallel and cache dimensions for reuse
    const dimensionsMap = new Map<File, { width: number; height: number }>();
    const optimisticFileBlocks = await Promise.all(
      filesToUpload.map(async ({ file, preview }) => {
        const mimeType = resolveUploadMimeType(file);
        const dimensions = mimeType.startsWith("image/")
          ? await getImageDimensions(file)
          : mimeType.startsWith("video/")
            ? await getVideoDimensions(file)
            : null;
        if (dimensions) dimensionsMap.set(file, dimensions);
        return {
          type: "file",
          data: {
            url: preview || "",
            filename: file.name,
            mime_type: mimeType,
            size: file.size,
            ...(dimensions && { width: dimensions.width, height: dimensions.height }),
            _uploading: true,
          },
        } as ContentBlock;
      }),
    );

    const optimisticBlocks: ContentBlock[] = [...optimisticFileBlocks];
    if (hasText) {
      optimisticBlocks.push(...textBlocks!);
    }
    if (forwardBlock) {
      optimisticBlocks.push(forwardBlock);
    }
    setForwardingMessage(null);

    // Add optimistic message immediately
    const tempId = addOptimisticMessage(optimisticBlocks);

    // Upload files in background, passing cached dimensions to skip recomputation
    try {
      const { blocks: realFileBlocks, failedFiles } = await uploadFiles(
        filesToUpload,
        dimensionsMap,
      );
      if (failedFiles.length > 0) {
        const failedList = failedFiles.slice(0, 2).join(", ");
        const suffix = failedFiles.length > 2 ? ` and ${failedFiles.length - 2} more` : "";
        throw new Error(
          `Failed to upload image(s): ${failedList}${suffix}. Please retry.`,
        );
      }

      const realBlocks: ContentBlock[] = [...realFileBlocks];
      if (hasText) {
        realBlocks.push(...textBlocks!);
      }
      if (forwardBlock) {
        realBlocks.push(forwardBlock);
      }

      // Finalize immediately after upload/confirm to keep send latency low.
      const finalized = await finalizeOptimisticMessage(tempId, realBlocks);
      if (!finalized) {
        throw new Error("Failed to send message. Please retry.");
      }
      shouldRevokePreviews = true;
    } catch (err) {
      console.error("Failed to send message with files:", err);
      removeOptimisticMessage(tempId);
      setPendingFiles((prev) => [
        ...filesToUpload.filter((file) => !prev.includes(file)),
        ...prev,
      ]);
      setUploadError(
        err instanceof Error ? err.message : "Failed to upload image. Please retry.",
      );
    } finally {
      setIsUploading(false);
      if (shouldRevokePreviews) {
        // Revoke preview URLs after upload completes successfully
        filesToUpload.forEach(
          (pf) => pf.preview && URL.revokeObjectURL(pf.preview),
        );
      }
    }
  };

  const handleCreateChannel = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newChannelName.trim() || isCreatingChannel) return;

    setIsCreatingChannel(true);
    try {
      const channel = await addChannel(
        newChannelName,
        newChannelDescription || undefined,
        newChannelPrivate,
      );
      setNewChannelName("");
      setNewChannelDescription("");
      setNewChannelPrivate(false);
      setShowCreateChannel(false);
      navigateToChannel(channel.id);
    } catch (err) {
      console.error("Failed to create channel:", err);
    } finally {
      setIsCreatingChannel(false);
    }
  };

  const handleDeleteChannel = async (channelId: string) => {
    if (confirm("Are you sure you want to delete this channel?")) {
      await removeChannel(channelId);
    }
  };

  const handleLeaveChannel = async () => {
    if (!currentChannel || !user?.id) return;
    if (confirm("Are you sure you want to leave this channel?")) {
      try {
        await removeChannelMember(currentChannel.id, user.id);
      } catch (err) {
        console.error("Failed to leave channel:", err);
      }
    }
  };

  const handleDeleteMessage = async (messageId: string) => {
    if (confirm("Delete this message?")) {
      await removeMessage(messageId);
    }
  };

  const handleStartEdit = (message: (typeof messages)[0]) => {
    // Extract text content from blocks for editing
    const textContent =
      message.blocks
        .filter((block) => block.type === "text")
        .map((block) => block.data.content as string)
        .join("\n") || message.content;
    setEditingMessageId(message.id);
    setEditingContent(textContent);
  };

  const handleCancelEdit = () => {
    setEditingMessageId(null);
    setEditingContent("");
  };

  const handleSaveEdit = async (content: string, html?: string) => {
    if (!editingMessageId || !content.trim()) return;

    // Find the original message to preserve non-text blocks (shared_message, file, etc.)
    const originalMessage = messages.find((m) => m.id === editingMessageId)
      || threadReplies.find((m) => m.id === editingMessageId);

    // Preserve blocks that aren't text (e.g., shared_message, file)
    const preservedBlocks = originalMessage?.blocks.filter((b) => b.type !== "text") || [];

    const blocks: ContentBlock[] = [
      {
        type: "text",
        data: html
          ? { content: content.trim(), html }
          : { content: content.trim() },
      },
      ...preservedBlocks,
    ];

    await editMessage(editingMessageId, blocks);
    setEditingMessageId(null);
    setEditingContent("");
  };

  // Send thread reply
  const handleSendThreadReply = async (textBlocks?: ContentBlock[]) => {
    if (isThreadUploading || !activeThreadId) return;

    const filesToUpload = [...threadPendingFiles];
    const hasFiles = filesToUpload.length > 0;
    const hasText = textBlocks && textBlocks.length > 0;
    const hasForward = !!forwardingMessage;

    if (!hasFiles && !hasText && !hasForward) return;

    // Build shared_message block for forwarding
    const forwardBlock: ContentBlock | null = hasForward ? {
      type: 'shared_message',
      data: {
        original_message_id: forwardingMessage.id,
        original_channel_id: forwardingMessage.channel_id,
        original_channel_name:
          safeChannels.find((c) => c.id === forwardingMessage.channel_id)?.name
          || (() => {
            const dm = safeDms.find((d) => d.id === forwardingMessage.channel_id);
            if (!dm) return "unknown";
            const other = dm.participants?.find((p) => p.id !== user?.id);
            return other?.name || other?.email?.split("@")[0] || "DM";
          })(),
        original_user_name: forwardingMessage.user?.name || forwardingMessage.user?.email || 'Unknown',
        original_user_avatar: forwardingMessage.user?.avatar_url,
        original_content: forwardingMessage.content,
        original_blocks: forwardingMessage.blocks,
        original_created_at: forwardingMessage.created_at,
      },
    } : null;

    // If no files, use the regular optimistic send
    if (!hasFiles) {
      const blocks: ContentBlock[] = [...(textBlocks || [])];
      if (forwardBlock) blocks.push(forwardBlock);
      setForwardingMessage(null);
      sendMessage(blocks, activeThreadId);
      return;
    }

    // Clear pending files immediately for optimistic UI
    setThreadPendingFiles([]);
    setIsThreadUploading(true);

    try {
      const { blocks: fileBlocks, failedFiles } = await uploadFiles(filesToUpload);

      if (failedFiles.length > 0) {
        setUploadError(`Failed to upload: ${failedFiles.join(", ")}`);
      }

      const blocks: ContentBlock[] = [...(textBlocks || []), ...fileBlocks];
      if (forwardBlock) blocks.push(forwardBlock);
      setForwardingMessage(null);

      if (blocks.length > 0) {
        sendMessage(blocks, activeThreadId);
      }
    } catch (err) {
      console.error("Thread upload error:", err);
      setUploadError("Failed to upload files");
    } finally {
      setIsThreadUploading(false);
    }
  };

  // Get the parent message for thread view
  const threadParentMessage = activeThreadId
    ? uniqueMessages.find((m) => m.id === activeThreadId)
    : null;

  // Load workspace members for New DM modal
  const handleOpenNewDM = async () => {
    setShowNewDM(true);
    if (workspace?.id) {
      setIsLoadingMembers(true);
      try {
        const members = await getWorkspaceMembers(workspace.id);
        // Put current user first so "message yourself" is easy to find
        const sorted = [...members].sort((a, b) => {
          if (a.user_id === user?.id) return -1;
          if (b.user_id === user?.id) return 1;
          return 0;
        });
        setWorkspaceMembers(sorted);
        setWorkspaceMembersForId(workspace.id);
      } catch (err) {
        console.error("Failed to load members:", err);
      } finally {
        setIsLoadingMembers(false);
      }
    }
  };

  const handleStartDMWithUser = async (userId: string) => {
    setShowNewDM(false);
    await startDM([userId]);
  };

  // Channel details modal - unified modal for members and settings
  const handleOpenChannelDetails = async (tab: "channel" | "app" = "channel", channelId?: string) => {
    const targetChannelId = channelId || currentChannel?.id;
    if (!targetChannelId) return;
    const targetChannel = safeChannels.find(c => c.id === targetChannelId);
    if (!targetChannel) return;

    setModalChannelId(targetChannelId);
    setChannelDetailsTab(tab);
    setEditingChannelName(targetChannel.name);
    setEditingChannelDescription(targetChannel.description || "");
    setChannelMembersError(null);
    setShowAddMember(false);
    setShowChannelDetails(true);

    // Fetch members for this specific channel if not already loaded
    if (channelMembersForId !== targetChannelId) {
      setIsLoadingChannelMembers(true);
      setChannelMembers([]);
      try {
        const data = await getChannelMembers(targetChannelId);
        setChannelMembers(data.members || []);
        setChannelMembersForId(targetChannelId);
      } catch (err) {
        console.error(`[Channel Members] Failed to fetch members:`, err);
        setChannelMembersError("Failed to load members");
      } finally {
        setIsLoadingChannelMembers(false);
      }
    }
  };

  const handleSaveChannelSettings = async () => {
    if (!modalChannelId || !editingChannelName.trim()) return;

    setIsSavingChannel(true);
    try {
      await editChannel(modalChannelId, {
        name: editingChannelName.trim(),
        description: editingChannelDescription.trim() || undefined,
      });
      setShowChannelDetails(false);
      setModalChannelId(null);
      setShowAdvancedOptions(false);
    } catch (err) {
      console.error("Failed to update channel:", err);
    } finally {
      setIsSavingChannel(false);
    }
  };

  const handleAddChannelMember = async (userId: string) => {
    if (!modalChannelId || !userId) return;

    try {
      console.log(
        `[Channel Members] Adding user ${userId} to channel ${modalChannelId}`,
      );
      const result = await addChannelMember(modalChannelId, userId);
      console.log(
        `[Channel Members] Successfully added member:`,
        result.member,
      );
      setChannelMembers((prev) => [...prev, result.member]);
      setAddMemberUserId("");
      setShowAddMember(false);
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      console.error(
        `[Channel Members] Failed to add member ${userId} to channel ${modalChannelId}:`,
        err,
      );
      setChannelMembersError(`Failed to add member: ${errorMsg}`);
    }
  };

  const handleRemoveChannelMember = async (userId: string) => {
    if (!modalChannelId) return;

    try {
      console.log(
        `[Channel Members] Removing user ${userId} from channel ${modalChannelId}`,
      );
      await removeChannelMember(modalChannelId, userId);
      console.log(`[Channel Members] Successfully removed member ${userId}`);
      setChannelMembers((prev) => prev.filter((m) => m.user_id !== userId));
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      console.error(
        `[Channel Members] Failed to remove member ${userId} from channel ${modalChannelId}:`,
        err,
      );
      setChannelMembersError(`Failed to remove member: ${errorMsg}`);
    }
  };

  // Show placeholder if no messages app exists
  if (!messagesApp) {
    return (
      <div className="flex-1 flex items-center justify-center text-text-tertiary bg-bg-shell p-1">
        <div className="text-center bg-white rounded-lg p-8">
          <p className="text-lg mb-2">Messages app not enabled</p>
          <p className="text-sm">
            Enable the Messages app for this workspace to get started.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex h-full overflow-hidden">
      {/* Main content container */}
      <div className="flex-1 flex overflow-hidden bg-bg-mini-app">
        {/* Sidebar - lighter than main sidebar */}
        <div
          ref={messagesSidebarRef}
          tabIndex={0}
          onKeyDown={handleSidebarKeyDown}
          onFocus={() => setActiveZone("app-sidebar")}
          className={`w-[212px] shrink-0 flex flex-col outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-primary ${SIDEBAR.bg} border-r border-black/5`}
        >
          {/* Header */}
          <div className="h-12 flex items-center justify-between pl-4 pr-2 shrink-0">
            <h2 className="text-base font-semibold text-text-body">Messages</h2>
            <button
              onClick={() => setShowCreateChannel(true)}
              className="p-1 rounded bg-white border border-black/10 hover:border-black/20 text-text-secondary hover:text-text-body transition-colors focus-visible:ring-2 focus-visible:ring-brand-primary"
              title="New channel"
              aria-label="New channel"
            >
              <Icon icon={Plus} size={16} aria-hidden="true" />
            </button>
          </div>

          {/* Channels Section */}
        <div className="flex-1 overflow-y-auto">
          <div className="px-2">
            {/* Channels Header */}
            <div className="space-y-0.5">
              <div
                role="button"
                tabIndex={0}
                onClick={() => setShowChannelsExpanded(!showChannelsExpanded)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    setShowChannelsExpanded(!showChannelsExpanded);
                  }
                }}
                aria-expanded={showChannelsExpanded}
                className="flex items-center gap-1 px-2 py-1.5 text-xs font-medium text-text-tertiary cursor-pointer group"
              >
                <span>Channels</span>
                <ChevronRightIcon className={`w-3 h-3 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-all ${showChannelsExpanded ? 'rotate-90' : ''}`} aria-hidden="true" />
                <div className="flex-1" />
                <div className="relative">
                  <button
                    ref={channelsSectionMenuRef}
                    onClick={(e) => {
                      e.stopPropagation();
                      setShowChannelsSectionMenu(!showChannelsSectionMenu);
                    }}
                    className="p-1 rounded text-text-tertiary hover:text-text-body hover:bg-bg-gray-light transition-colors opacity-0 group-hover:opacity-100 group-focus-within:opacity-100 focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-brand-primary"
                    title="More options"
                    aria-label="Channels section options"
                  >
                    <EllipsisHorizontalIcon className="w-3.5 h-3.5" aria-hidden="true" />
                  </button>
                  <Dropdown
                    isOpen={showChannelsSectionMenu}
                    onClose={() => setShowChannelsSectionMenu(false)}
                    trigger={channelsSectionMenuRef}
                  >
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        setShowChannelsSectionMenu(false);
                        setShowCreateChannel(true);
                      }}
                      className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2 focus-visible:bg-bg-gray focus-visible:outline-none"
                    >
                      <PlusIcon className="w-3.5 h-3.5" aria-hidden="true" />
                      New Channel
                    </button>
                  </Dropdown>
                </div>
              </div>
            </div>

            {/* Loading state - only show if no cached channels at all */}
            {isLoadingChannels && safeChannels.length === 0 && (
              <div className="space-y-0.5">
                {[...Array(8)].map((_, i) => (
                  <div key={i} className="flex items-center gap-2 px-2 h-[32px]">
                    <div className="w-4 h-4 bg-gray-200 rounded animate-pulse" />
                    <div className="flex-1 h-3 bg-gray-200 rounded animate-pulse" />
                  </div>
                ))}
              </div>
            )}

            {/* Channels List - show cached channels immediately */}
            {showChannelsExpanded && (safeChannels.length > 0 || !isLoadingChannels) && (
              <div className="space-y-0.5">
                {filteredChannels.length === 0 ? (
                  <p className="text-xs text-text-tertiary px-2 py-1.5">
                    No channels yet. Create one to get started!
                  </p>
                ) : (
                  filteredChannels.map((channel) => {
                    const unreadCount = unreadCounts[channel.id] || 0;
                    return (
                      <div
                        key={channel.id}
                        onClick={() => navigateToChannel(channel.id)}
                        className={`w-full flex items-center gap-2 px-2 h-[32px] rounded-md text-sm transition-colors group cursor-pointer focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-primary ${
                          safeActiveChannelId === channel.id
                            ? SIDEBAR.selected
                            : unreadCount > 0
                              ? "text-black font-medium hover:bg-black/5"
                              : `${SIDEBAR.item} hover:bg-black/5`
                        }`}
                      >
                        {channel.is_private ? (
                          <LockClosedIcon className="w-4 h-4 flex-shrink-0" aria-hidden="true" />
                        ) : (
                          <HashtagIcon className="w-4 h-4 flex-shrink-0" aria-hidden="true" />
                        )}
                        <span className="flex-1 text-left truncate">
                          {channel.name}
                        </span>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleOpenChannelDetails("channel", channel.id);
                          }}
                          className="p-1 rounded text-text-tertiary hover:text-text-body hover:bg-black/10 transition-colors opacity-0 group-hover:opacity-100 group-focus-within:opacity-100 focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-brand-primary"
                          title="Channel settings"
                          aria-label={`Settings for ${channel.name}`}
                        >
                          <EllipsisHorizontalIcon className="w-4 h-4" aria-hidden="true" />
                        </button>
                        {unreadCount > 0 && (
                          <span className="bg-brand-primary text-white text-xs font-medium rounded-full w-[18px] h-[18px] flex items-center justify-center">
                            {unreadCount > 99 ? "99+" : unreadCount}
                          </span>
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            )}

            {/* Direct Messages Section */}
            <div className="mt-2">
              <div className="space-y-0.5">
                <div
                  role="button"
                  tabIndex={0}
                  onClick={() => setShowDMsExpanded(!showDMsExpanded)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      setShowDMsExpanded(!showDMsExpanded);
                    }
                  }}
                  aria-expanded={showDMsExpanded}
                  className="flex items-center gap-1 px-2 py-1.5 text-xs font-medium text-text-tertiary cursor-pointer group"
                >
                  <span>Direct Messages</span>
                  <ChevronRightIcon className={`w-3 h-3 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-all ${showDMsExpanded ? 'rotate-90' : ''}`} aria-hidden="true" />
                  <div className="flex-1" />
                  <div className="relative">
                    <button
                      ref={dmsSectionMenuRef}
                      onClick={(e) => {
                        e.stopPropagation();
                        setShowDMsSectionMenu(!showDMsSectionMenu);
                      }}
                      className="p-1 rounded text-text-tertiary hover:text-text-body hover:bg-bg-gray-light transition-colors opacity-0 group-hover:opacity-100 group-focus-within:opacity-100 focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-brand-primary"
                      title="More options"
                      aria-label="Direct messages section options"
                    >
                      <EllipsisHorizontalIcon className="w-3.5 h-3.5" aria-hidden="true" />
                    </button>
                    <Dropdown
                      isOpen={showDMsSectionMenu}
                      onClose={() => setShowDMsSectionMenu(false)}
                      trigger={dmsSectionMenuRef}
                    >
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setShowDMsSectionMenu(false);
                          handleOpenNewDM();
                        }}
                        className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2 focus-visible:bg-bg-gray focus-visible:outline-none"
                      >
                        <PlusIcon className="w-3.5 h-3.5" aria-hidden="true" />
                        New Message
                      </button>
                    </Dropdown>
                  </div>
                </div>
              </div>

              {/* Loading DMs - only show if no cached DMs at all */}
              {isLoadingDMs && safeDms.length === 0 && (
                <div className="space-y-0.5">
                  {[...Array(8)].map((_, i) => (
                    <div key={i} className="flex items-center gap-2 px-2 h-[32px]">
                      <div className="w-5 h-5 bg-gray-200 rounded-full animate-pulse" />
                      <div className="flex-1 h-3 bg-gray-200 rounded animate-pulse" />
                    </div>
                  ))}
                </div>
              )}

              {/* DMs List - show cached DMs immediately */}
              {showDMsExpanded && (safeDms.length > 0 || !isLoadingDMs) && (
                <div className="space-y-0.5">
                  {/* Existing DM conversations - sorted by most recent message */}
                  {[...safeDms].sort((a, b) =>
                    new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime()
                  ).map((dm) => {
                    // Find the other participant(s) - exclude current user
                    const otherParticipants =
                      dm.participants?.filter((p) => p.id !== user?.id) || [];
                    const isSelfDM = otherParticipants.length === 0;
                    const displayName = getDMDisplayName(dm);
                    const firstParticipant = isSelfDM
                      ? dm.participants?.find((p) => p.id === user?.id) || null
                      : otherParticipants[0];
                    const unreadCount = unreadCounts[dm.id] || 0;

                    return (
                      <button
                        key={dm.id}
                        onClick={() => navigateToChannel(dm.id)}
                        className={`w-full flex items-center gap-2 px-2 h-[32px] rounded-md text-sm transition-colors cursor-pointer focus:outline-none ${
                          safeActiveChannelId === dm.id
                            ? SIDEBAR.selected
                            : unreadCount > 0
                              ? "text-black font-medium hover:bg-black/5"
                              : `${SIDEBAR.item} hover:bg-black/5`
                        }`}
                      >
                        <div className="relative flex-shrink-0">
                          {firstParticipant?.avatar_url &&
                          !failedAvatarIds?.has(firstParticipant.id) ? (
                            <img
                              src={firstParticipant.avatar_url}
                              alt={displayName}
                              className="w-5 h-5 rounded-full object-cover"
                              onError={() =>
                                handleAvatarError(firstParticipant.id)
                              }
                            />
                          ) : (
                            <div
                              className="w-5 h-5 rounded-full flex items-center justify-center text-white font-semibold text-[8px]"
                              style={{
                                background: avatarGradient(firstParticipant?.name || firstParticipant?.email || "?"),
                              }}
                            >
                              {(firstParticipant?.name || firstParticipant?.email || "?").charAt(0).toUpperCase()}
                            </div>
                          )}
                          {firstParticipant &&
                            onlineUserIds.has(firstParticipant.id) && (
                              <span className="absolute -bottom-0.5 -right-0.5 w-2.5 h-2.5 bg-green-500 rounded-full border-2 border-white" />
                            )}
                        </div>
                        <span className="flex-1 text-left truncate">
                          {displayName}
                        </span>
                        {unreadCount > 0 && (
                          <span className="bg-brand-primary text-white text-xs font-medium rounded-full w-[18px] h-[18px] flex items-center justify-center">
                            {unreadCount > 99 ? "99+" : unreadCount}
                          </span>
                        )}
                      </button>
                    );
                  })}

                  {/* Workspace members without existing DMs - sorted by most recently added */}
                  {(() => {
                    // Get user IDs that already have DMs
                    const dmUserIds = new Set(
                      safeDms.flatMap((dm) =>
                        dm.participants?.filter((p) => p.id !== user?.id).map((p) => p.id) || []
                      )
                    );
                    // Filter workspace members to only those without existing DMs
                    // Then sort by joined_at descending (most recently added first)
                    const membersWithoutDM = safeWorkspaceMembers
                      .filter((m) => !dmUserIds.has(m.user_id))
                      .sort((a, b) =>
                        new Date(b.joined_at).getTime() - new Date(a.joined_at).getTime()
                      );

                    return membersWithoutDM.slice(0, 8).map((member) => {
                      const displayName = member.name || member.email?.split("@")[0] || "Unknown";
                      return (
                        <button
                          key={`member-${member.user_id}`}
                          onClick={() => handleStartDMWithUser(member.user_id)}
                          className={`w-full flex items-center gap-2 px-2 h-[32px] rounded-md text-sm transition-colors cursor-pointer focus:outline-none ${SIDEBAR.item} hover:bg-black/5`}
                        >
                          <div className="relative flex-shrink-0">
                            {member.avatar_url && !failedAvatarIds?.has(member.user_id) ? (
                              <img
                                src={member.avatar_url}
                                alt={displayName}
                                className="w-5 h-5 rounded-full object-cover"
                                onError={() => handleAvatarError(member.user_id)}
                              />
                            ) : (
                              <div
                                className="w-5 h-5 rounded-full flex items-center justify-center text-white font-semibold text-[8px]"
                                style={{
                                  background: avatarGradient(member.name || member.email || member.user_id),
                                }}
                              >
                                {(member.name || member.email || "?").charAt(0).toUpperCase()}
                              </div>
                            )}
                            {onlineUserIds.has(member.user_id) && (
                              <span className="absolute -bottom-0.5 -right-0.5 w-2.5 h-2.5 bg-green-500 rounded-full border-2 border-white" />
                            )}
                          </div>
                          <span className="flex-1 text-left truncate">
                            {displayName}
                          </span>
                        </button>
                      );
                    });
                  })()}

                  {/* Show message if no DMs and no other members */}
                  {safeDms.length === 0 && safeWorkspaceMembers.length === 0 && (
                    <p className="text-xs text-text-tertiary px-3 py-2">
                      No other members in this workspace
                    </p>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
        </div>

      {/* Main Content */}
      <div className="flex-1 flex min-w-0 overflow-hidden">
      {safeActiveChannelId && (currentChannel || currentDM) ? (
        <div className="flex-1 flex min-w-0 overflow-hidden bg-white rounded-r-lg">
          {/* Messages area */}
          <div
            className={`flex-1 min-w-0 flex flex-col ${activeThreadId ? "border-r border-border-gray" : ""}`}
          >
            {/* Header */}
            <div className="h-12 shrink-0 border-b border-border-gray pl-5 pr-3 flex items-center justify-between">
              <div className="flex items-center gap-2">
                {isInDM ? (
                  <>
                    {(() => {
                      const otherParticipant = currentDM?.participants?.find(
                        (p) => p.id !== user?.id,
                      ) || currentDM?.participants?.find((p) => p.id === user?.id);
                      return otherParticipant?.avatar_url &&
                        !failedAvatarIds?.has(otherParticipant.id) ? (
                        <img
                          src={otherParticipant.avatar_url}
                          alt={getDMDisplayName(currentDM!)}
                          className="w-7 h-7 rounded-full object-cover"
                          onError={() =>
                            otherParticipant &&
                            handleAvatarError(otherParticipant.id)
                          }
                        />
                      ) : (
                        <div
                          className="w-7 h-7 rounded-full flex items-center justify-center text-white font-semibold text-xs"
                          style={{
                            background: avatarGradient(otherParticipant?.name || otherParticipant?.email || getDMDisplayName(currentDM!)),
                          }}
                        >
                          {(otherParticipant?.name || otherParticipant?.email || getDMDisplayName(currentDM!)).charAt(0).toUpperCase()}
                        </div>
                      );
                    })()}
                    <h2 className="text-base font-semibold text-text-body">
                      {getDMDisplayName(currentDM!)}
                    </h2>
                  </>
                ) : (
                  <>
                    <button
                      onClick={() => handleOpenChannelDetails("channel")}
                      className="flex items-center gap-2 hover:bg-bg-gray-light rounded-md px-2 py-1 -ml-2 transition-colors"
                    >
                      {currentChannel!.is_private ? (
                        <LockClosedIcon className="w-4.5 h-4.5 text-text-body" />
                      ) : (
                        <HashtagIcon className="w-4.5 h-4.5 text-text-body" />
                      )}
                      <h2 className="text-base font-semibold text-text-body">
                        {currentChannel!.name}
                      </h2>
                    </button>
                    {currentChannel!.description && (
                      <span className="text-sm text-text-tertiary">
                        {currentChannel!.description}
                      </span>
                    )}
                  </>
                )}
              </div>
              <div className="flex items-center gap-2">
                <HeaderButtons
                  onSettingsClick={() => setShowSettingsDropdown(prev => !prev)}
                  settingsButtonRef={settingsButtonRef}
                />
              </div>
            </div>

            {/* Content area below header - relative for overlay panels */}
            <div className="flex-1 min-h-0 flex flex-col relative">
            {/* Messages - column-reverse anchors scroll to bottom; when inline edits expand, earlier messages push up */}
            <div
              ref={scrollableContainerRef}
              className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden flex flex-col-reverse"
            >
              {/* Single wrapper inside column-reverse so messages render in normal order */}
              <div>
                {/* Loading state - only show for active channel */}
                {isLoadingMessages && (
                  <div className="px-6 py-4 space-y-6">
                    {[...Array(8)].map((_, i) => (
                      <div key={i} className="flex gap-3">
                        <div className="w-10 h-10 bg-gray-200 rounded-lg animate-pulse shrink-0" />
                        <div className="flex-1 space-y-2">
                          <div className="flex items-center gap-2">
                            <div className="h-3.5 w-24 bg-gray-200 rounded animate-pulse" />
                            <div className="h-2.5 w-12 bg-gray-100 rounded animate-pulse" />
                          </div>
                          <div className="h-3 w-full bg-gray-200 rounded animate-pulse" />
                          <div className="h-3 w-3/4 bg-gray-200 rounded animate-pulse" />
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {/* Sentinel for infinite scroll - loads older messages */}
                {!isLoadingMessages && safeActiveChannelId && channelHasMore === true && (
                  <div ref={loadMoreSentinelRef} className="flex justify-center py-4">
                    {isLoadingOlderMessages && (
                      <div className="flex items-center gap-2 text-text-tertiary text-sm">
                        <div className="w-4 h-4 border-2 border-gray-300 border-t-gray-600 rounded-full animate-spin" />
                        Loading older messages...
                      </div>
                    )}
                  </div>
                )}

                {/* Channel creation banner - only show when all messages are loaded */}
                {!isLoadingMessages && currentChannel && !isInDM && channelHasMore === false && (
                  <div className="px-6 pt-6 pb-2">
                    <div className="flex items-center gap-3 mb-3">
                      <div className="w-10 h-10 rounded-lg bg-bg-gray flex items-center justify-center">
                        {currentChannel.is_private ? (
                          <LockClosedIcon className="w-5 h-5 text-text-secondary" />
                        ) : (
                          <HashtagIcon className="w-5 h-5 text-text-secondary" />
                        )}
                      </div>
                      <div>
                        <h3 className="text-lg font-semibold text-text-body">
                          {currentChannel.name}
                        </h3>
                        {currentChannel.description && (
                          <p className="text-sm text-text-secondary">
                            {currentChannel.description}
                          </p>
                        )}
                      </div>
                    </div>
                    <p className="text-sm text-text-tertiary">
                      <span className="font-medium text-text-secondary">
                        {currentChannel.created_by_user?.name ||
                          currentChannel.created_by_user?.email ||
                          "Someone"}
                      </span>{" "}
                      created this channel on{" "}
                      {new Date(currentChannel.created_at).toLocaleDateString(
                        "en-US",
                        {
                          month: "long",
                          day: "numeric",
                          year: "numeric",
                        },
                      )}
                    </p>
                    <div className="border-b border-border-gray mt-4" />
                  </div>
                )}

                {/* Render cached messages for visited channels - keeps images mounted */}
                {/* Include activeChannelId even if not in visitedChannelIds yet (initial load) */}
                {(() => {
                  const channelsToRender =
                    activeConversationId &&
                    !visitedChannelIds.includes(activeConversationId)
                      ? [activeConversationId, ...visitedChannelIds]
                      : visitedChannelIds;

                  return channelsToRender.map((channelId) => {
                    const isActive = channelId === activeConversationId;
                    const channelMessages = isActive
                      ? messages
                      : messagesCache[channelId] || [];
                    if (channelMessages.length === 0 && !isActive) return null;

                    return (
                      <CachedChannelMessages
                        key={channelId}
                        channelId={channelId}
                        isActive={isActive}
                        messages={channelMessages}
                        currentUserId={user?.id}
                        onEdit={handleStartEdit}
                        onDelete={handleDeleteMessage}
                        onReaction={(messageId, emoji) =>
                          addReaction(messageId, emoji)
                        }
                        onRemoveReaction={(messageId, emoji) =>
                          removeReaction(messageId, emoji)
                        }
                        onReply={(messageId) => setActiveThread(messageId)}
                        onShare={(message) => setForwardingMessage(message)}
                        onNavigateToChannel={navigateToChannel}
                        editingMessageId={editingMessageId}
                        editingContent={editingContent}
                        onSaveEdit={handleSaveEdit}
                        onCancelEdit={handleCancelEdit}
                        failedAvatarIds={failedAvatarIds}
                        onAvatarError={handleAvatarError}
                        onImageClick={setLightboxImageUrl}
                        getUserInfo={getUserInfo}
                      />
                    );
                  });
                })()}
              </div>
            </div>

            {/* Empty state for DMs - channels have the creation banner instead */}
            {!isLoadingMessages &&
              messages.length === 0 &&
              isInDM &&
              currentDM && (
                <div className="absolute inset-0 flex flex-col items-center justify-center text-text-tertiary pb-[80px] pointer-events-none">
                  <ChatBubbleOvalLeftIcon className="w-12 h-12 mb-4 opacity-50" />
                  <p className="text-lg">Start a conversation</p>
                  <p className="text-sm">
                    Send a message to {getDMDisplayName(currentDM)}
                  </p>
                </div>
              )}

            {/* Error display */}
            {visibleError && (
              <div className="shrink-0 px-4 py-2 bg-red-50 border-t border-red-200 text-red-600 text-sm">
                {visibleError}
              </div>
            )}

            {/* Input - in flex flow so growing editor pushes messages up */}
            <div className="shrink-0 relative px-4 pb-6 pt-2 bg-white">
              {/* Fade gradient above composer */}
              <div className="absolute -top-6 left-0 right-0 h-6 bg-gradient-to-t from-white to-transparent pointer-events-none" />

              {/* Hidden file input */}
              <input
                ref={fileInputRef}
                type="file"
                multiple
                className="hidden"
                onChange={handleFileInputChange}
              />

              {/* Typing indicator */}
              {(() => {
                const channelTypers = activeConversationId
                  ? (typingUsers[activeConversationId] || []).filter((t) => t.userId !== user?.id)
                  : [];
                if (channelTypers.length === 0) return null;
                const names = channelTypers.map((t) => t.userName);
                const text =
                  names.length === 1
                    ? `${names[0]} is typing...`
                    : `${names.join(", ")} are typing...`;
                return (
                  <div className="px-1 pb-1 text-xs text-text-tertiary animate-pulse">
                    {text}
                  </div>
                );
              })()}

              {forwardingMessage && (
                <ForwardPreviewBar
                  message={forwardingMessage}
                  onCancel={() => setForwardingMessage(null)}
                />
              )}
              <MessageComposer
                placeholder={
                  isInDM
                    ? `Message ${getDMDisplayName(currentDM!)}`
                    : `Message #${currentChannel!.name}`
                }
                onSend={(blocks) => {
                  handleSendMessage(blocks);
                  if (activeConversationId) stopTyping(activeConversationId);
                }}
                workspaceId={workspaceId || ""}
                channelId={activeConversationId || ""}
                pendingFiles={pendingFiles}
                onFileSelect={handleFileSelect}
                onRemovePendingFile={removePendingFile}
                isUploading={isUploading}
                onTyping={() =>
                  activeConversationId && broadcastTyping(activeConversationId)
                }
              />
            </div>
            </div>
          </div>

          {/* Thread Panel */}
          <AnimatePresence>
          {activeThreadId && threadParentMessage && (
            <motion.div
              initial={{ width: 0, opacity: 0 }}
              animate={{ width: 384, opacity: 1 }}
              exit={{ width: 0, opacity: 0 }}
              transition={{ duration: 0.2, ease: [0.4, 0, 0.2, 1] }}
              className="flex flex-col bg-bg-white overflow-hidden shrink-0 border-l border-border-gray"
            >
              {/* Thread Header */}
              <div className="h-12 border-b border-border-gray px-4 flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <ChatBubbleOvalLeftIcon className="w-4.5 h-4.5 text-text-secondary" />
                  <h3 className="font-medium text-text-body">Thread</h3>
                </div>
                <button
                  onClick={clearThread}
                  className="p-1.5 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded transition-colors"
                >
                  <XMarkIcon className="w-4.5 h-4.5" />
                </button>
              </div>

              {/* Thread content */}
              <div className="flex-1 overflow-y-auto">
                {/* Parent message */}
                <div className="border-b border-border-gray">
                  <Message
                    message={threadParentMessage}
                    currentUserId={user?.id}
                    onReaction={(emoji) =>
                      addReaction(threadParentMessage.id, emoji)
                    }
                    onRemoveReaction={(emoji) =>
                      removeReaction(threadParentMessage.id, emoji)
                    }
                    onNavigateToChannel={navigateToChannel}
                    isInThread
                    failedAvatarIds={failedAvatarIds}
                    onAvatarError={handleAvatarError}
                    onImageClick={setLightboxImageUrl}
                    getUserInfo={getUserInfo}
                  />
                </div>

                {/* Reply count */}
                <div className="px-4 py-2 text-xs text-text-tertiary border-b border-border-gray">
                  {threadReplies.length}{" "}
                  {threadReplies.length === 1 ? "reply" : "replies"}
                </div>

                {/* Thread replies */}
                {isLoadingThread ? (
                  <div className="py-4 space-y-4">
                    {[...Array(2)].map((_, i) => (
                      <div key={i} className="flex gap-3">
                        <div className="w-10 h-10 bg-gray-200 rounded-lg animate-pulse shrink-0" />
                        <div className="flex-1 space-y-2">
                          <div className="flex items-center gap-2">
                            <div className="h-3.5 w-20 bg-gray-200 rounded animate-pulse" />
                            <div className="h-2.5 w-10 bg-gray-100 rounded animate-pulse" />
                          </div>
                          <div className="h-3 w-full bg-gray-200 rounded animate-pulse" />
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="py-2">
                    {threadReplies.map((reply) => (
                      <Message
                        key={reply.id}
                        message={reply}
                        currentUserId={user?.id}
                        onEdit={() => handleStartEdit(reply)}
                        onDelete={() => handleDeleteMessage(reply.id)}
                        onReaction={(emoji) => addReaction(reply.id, emoji)}
                        onRemoveReaction={(emoji) =>
                          removeReaction(reply.id, emoji)
                        }
                        onShare={() => setForwardingMessage(reply)}
                        onNavigateToChannel={navigateToChannel}
                        isEditing={editingMessageId === reply.id}
                        editingContent={
                          editingMessageId === reply.id ? editingContent : ""
                        }
                        onSaveEdit={handleSaveEdit}
                        onCancelEdit={handleCancelEdit}
                        isInThread
                        failedAvatarIds={failedAvatarIds}
                        onAvatarError={handleAvatarError}
                        onImageClick={setLightboxImageUrl}
                        getUserInfo={getUserInfo}
                      />
                    ))}
                  </div>
                )}
              </div>

              {/* Thread input */}
              <div className="relative z-10 px-4 pt-2 pb-6 bg-bg-white">
                {forwardingMessage && (
                  <ForwardPreviewBar
                    message={forwardingMessage}
                    onCancel={() => setForwardingMessage(null)}
                  />
                )}
                <input
                  ref={threadFileInputRef}
                  type="file"
                  multiple
                  className="hidden"
                  onChange={handleThreadFileInputChange}
                />
                <MessageComposer
                  placeholder="Reply..."
                  onSend={(blocks) => handleSendThreadReply(blocks)}
                  workspaceId={workspaceId || ""}
                  channelId={activeThreadId || ""}
                  pendingFiles={threadPendingFiles}
                  onFileSelect={handleThreadFileSelect}
                  onRemovePendingFile={removeThreadPendingFile}
                  isUploading={isThreadUploading}
                />
              </div>
            </motion.div>
          )}
          </AnimatePresence>
        </div>
      ) : (
        <div className="flex-1 flex items-center justify-center text-text-tertiary bg-white rounded-r-lg relative">
          <p>Select a channel or start a conversation</p>
        </div>
      )}
      </div>

      {/* Create Channel Modal */}
      {showCreateChannel &&
        createPortal(
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="fixed inset-0 z-9999 flex items-center justify-center bg-black/30"
            onClick={(e) =>
              e.target === e.currentTarget && setShowCreateChannel(false)
            }
          >
            <motion.div
              initial={{ opacity: 0, scale: 0.95, y: 10 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 10 }}
              transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
              className="bg-white rounded-lg overflow-hidden"
              style={{
                width: "100%",
                maxWidth: 420,
                margin: "0 16px",
                boxShadow: "0 24px 48px -12px rgba(0, 0, 0, 0.15)",
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <div className="flex items-start justify-between px-6 pt-6 pb-0">
                <h2 className="text-lg font-medium text-gray-900">
                  Create Channel
                </h2>
                <button
                  onClick={() => setShowCreateChannel(false)}
                  className="p-1.5 -mr-1 text-gray-300 hover:text-gray-500 rounded-lg transition-colors"
                >
                  <XMarkIcon className="w-4.5 h-4.5 stroke-2" />
                </button>
              </div>

              <form onSubmit={handleCreateChannel} className="px-6 pb-6 pt-5 space-y-5">
                <div>
                  <label className="block text-[12px] font-medium text-gray-500 mb-2">
                    Name
                  </label>
                  <input
                    type="text"
                    value={newChannelName}
                    onChange={(e) => setNewChannelName(e.target.value)}
                    className="w-full px-3 py-2 text-[14px] text-gray-900 bg-transparent border border-gray-200 rounded-lg focus:outline-none focus:border-gray-300 placeholder:text-gray-400"
                    placeholder="e.g. project-updates"
                    autoFocus
                    required
                  />
                </div>

                <div>
                  <label className="block text-[12px] font-medium text-gray-500 mb-2">
                    Description{" "}
                    <span className="text-gray-400 font-normal">
                      (optional)
                    </span>
                  </label>
                  <input
                    type="text"
                    value={newChannelDescription}
                    onChange={(e) => setNewChannelDescription(e.target.value)}
                    className="w-full px-3 py-2 text-[14px] text-gray-900 bg-transparent border border-gray-200 rounded-lg focus:outline-none focus:border-gray-300 placeholder:text-gray-400"
                    placeholder="What's this channel about?"
                  />
                </div>

                <div>
                  <label className="block text-[12px] font-medium text-gray-500 mb-2">
                    Type
                  </label>
                  <div className="flex gap-2">
                    <button
                      type="button"
                      onClick={() => setNewChannelPrivate(false)}
                      className={`flex-1 flex items-center justify-center gap-2 py-2 rounded-lg text-[13px] font-medium transition-colors ${
                        !newChannelPrivate
                          ? "bg-gray-900 text-white"
                          : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                      }`}
                    >
                      <HashtagIcon className="w-3.5 h-3.5" />
                      Public
                    </button>
                    <button
                      type="button"
                      onClick={() => setNewChannelPrivate(true)}
                      className={`flex-1 flex items-center justify-center gap-2 py-2 rounded-lg text-[13px] font-medium transition-colors ${
                        newChannelPrivate
                          ? "bg-gray-900 text-white"
                          : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                      }`}
                    >
                      <LockClosedIcon className="w-3.5 h-3.5" />
                      Private
                    </button>
                  </div>
                </div>

                <div className="flex items-center justify-end gap-2 pt-4 border-t border-gray-100">
                  <button
                    type="button"
                    onClick={() => setShowCreateChannel(false)}
                    className="px-3 py-2 text-[12px] font-medium text-gray-500 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={!newChannelName.trim() || isCreatingChannel}
                    className="px-3 py-2 text-[12px] font-medium bg-gray-900 text-white rounded-lg hover:bg-gray-800 disabled:opacity-40 disabled:cursor-not-allowed transition-all"
                  >
                    {isCreatingChannel ? 'Creating...' : 'Create'}
                  </button>
                </div>
              </form>
            </motion.div>
          </motion.div>,
          document.body,
        )}

      {/* New DM Modal */}
      {showNewDM &&
        createPortal(
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="fixed inset-0 bg-black/40 flex items-center justify-center z-[9999]"
            onClick={(e) => e.target === e.currentTarget && setShowNewDM(false)}
          >
            <motion.div
              initial={{ opacity: 0, scale: 0.95, y: 10 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 10 }}
              transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
              className="bg-white rounded-lg shadow-xl w-[400px] max-w-[calc(100%-2rem)] max-h-[80vh] flex flex-col overflow-hidden"
              onClick={(e) => e.stopPropagation()}
            >
              <div className="flex items-center justify-between px-5 py-4">
                <h2 className="text-[15px] font-semibold text-gray-900">
                  New Message
                </h2>
                <button
                  onClick={() => setShowNewDM(false)}
                  className="p-1 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
                >
                  <XMarkIcon className="w-5 h-5" />
                </button>
              </div>

              <div className="px-2 py-2 overflow-y-auto flex-1">
                {isLoadingMembers ? (
                  <div className="space-y-0.5">
                    {[...Array(4)].map((_, i) => (
                      <div key={i} className="flex items-center gap-3 px-3 py-2">
                        <div className="w-8 h-8 bg-gray-200 rounded-full animate-pulse shrink-0" />
                        <div className="flex-1 h-3 bg-gray-200 rounded animate-pulse" />
                      </div>
                    ))}
                  </div>
                ) : safeWorkspaceMembers.length === 0 ? (
                  <div className="text-center py-8 text-text-tertiary">
                    <p className="text-[13px]">No other members in this workspace yet.</p>
                    <p className="text-[13px] mt-1">
                      Invite someone to start messaging!
                    </p>
                  </div>
                ) : (
                  <div className="space-y-0.5">
                    {safeWorkspaceMembers.map((member) => (
                      <button
                        key={member.user_id}
                        onClick={() => handleStartDMWithUser(member.user_id)}
                        className="w-full flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-gray-50 transition-colors"
                      >
                        {member.avatar_url &&
                        !failedAvatarIds?.has(member.user_id) ? (
                          <img
                            src={member.avatar_url}
                            alt={member.name || member.email || "User"}
                            className="w-8 h-8 rounded-full object-cover flex-shrink-0"
                            onError={() => handleAvatarError(member.user_id)}
                          />
                        ) : (
                          <div
                            className="w-8 h-8 rounded-full flex-shrink-0 bg-gradient-to-br from-[#5A7864] to-[#607E98]"
                          />
                        )}
                        <div className="flex-1 text-left min-w-0">
                          <p className="text-[13px] font-medium text-gray-900 truncate">
                            {member.name ||
                              member.email?.split("@")[0] ||
                              "Unknown"}
                            {member.user_id === user?.id && (
                              <span className="text-gray-400 font-normal"> (you)</span>
                            )}
                          </p>
                          {member.email && member.name && (
                            <p className="text-[12px] text-gray-500 truncate">
                              {member.email}
                            </p>
                          )}
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </motion.div>
          </motion.div>,
          document.body,
        )}

      {/* Settings Dropdown */}
      <MessagesSettingsDropdown
        isOpen={showSettingsDropdown}
        onClose={() => setShowSettingsDropdown(false)}
        trigger={settingsButtonRef}
        onRename={currentChannel ? () => handleOpenChannelDetails("channel") : undefined}
        onMembers={currentChannel ? () => handleOpenChannelDetails("channel") : undefined}
        memberCount={!isLoadingChannelMembers ? safeChannelMembers.length : undefined}
        onAppSettings={() => handleOpenChannelDetails("app")}
        onLeave={currentChannel ? handleLeaveChannel : undefined}
        onDelete={currentChannel ? () => handleDeleteChannel(currentChannel.id) : undefined}
      />

      {/* Channel Details Modal (Members & Settings) */}
      {showChannelDetails &&
        modalChannel &&
        createPortal(
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="fixed inset-0 bg-black/40 flex items-center justify-center z-[9999]"
            onClick={(e) => {
              if (e.target === e.currentTarget) {
                setShowChannelDetails(false);
                setModalChannelId(null);
                setShowAdvancedOptions(false);
              }
            }}
          >
            <motion.div
              initial={{ opacity: 0, scale: 0.95, y: 10 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 10 }}
              transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
              className="bg-white rounded-lg shadow-xl w-[520px] max-w-[calc(100%-2rem)] max-h-[80vh] flex flex-col overflow-hidden"
              style={{ height: 500 }}
              onClick={(e) => e.stopPropagation()}
            >
              {/* Modal Header */}
              <div className="flex items-center justify-between px-6 py-5">
                <div className="flex items-center gap-2">
                  <h2 className="text-lg font-semibold text-gray-900">
                    Messages
                  </h2>
                </div>
                <button
                  onClick={() => {
                    setShowChannelDetails(false);
                    setModalChannelId(null);
                    setShowAdvancedOptions(false);
                  }}
                  className="p-1 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
                >
                  <XMarkIcon className="w-5 h-5" />
                </button>
              </div>

              {/* Tabs */}
              <div className="flex gap-4 px-6 mb-2">
                <button
                  onClick={() => setChannelDetailsTab("channel")}
                  className={`pb-2 text-sm font-medium transition-colors relative ${
                    channelDetailsTab === "channel"
                      ? "text-gray-900"
                      : "text-gray-500 hover:text-gray-700"
                  }`}
                >
                  Channel Settings
                  {channelDetailsTab === "channel" && (
                    <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gray-900 rounded-full" />
                  )}
                </button>
                <button
                  onClick={() => setChannelDetailsTab("app")}
                  className={`pb-2 text-sm font-medium transition-colors relative ${
                    channelDetailsTab === "app"
                      ? "text-gray-900"
                      : "text-gray-500 hover:text-gray-700"
                  }`}
                >
                  App Settings
                  {channelDetailsTab === "app" && (
                    <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gray-900 rounded-full" />
                  )}
                </button>
              </div>

              {/* Tab Content */}
              {channelDetailsTab === "channel" ? (
                <>
                  <div ref={channelContentRef} className="overflow-y-auto flex-1 px-6 py-3 space-y-4">
                    {/* Channel Details Section */}
                    <div>
                      <h3 className="text-sm font-medium text-gray-900 pb-3">
                        Details
                      </h3>
                      {/* Channel Name */}
                      <div className="mb-3">
                        <label className="block text-xs text-text-secondary mb-2">
                          Name
                        </label>
                        <div className="relative">
                          <span className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 text-sm">
                            #
                          </span>
                          <input
                            type="text"
                            value={editingChannelName}
                            onChange={(e) => setEditingChannelName(e.target.value)}
                            placeholder="e.g. marketing"
                            className="w-full pl-7 pr-3 py-2.5 bg-white border border-border-gray rounded-lg text-xs outline-none focus:border-text-tertiary"
                          />
                        </div>
                      </div>

                      {/* Channel Description */}
                      <div>
                        <label className="block text-xs text-text-secondary mb-2">
                          Description{" "}
                          <span className="text-text-tertiary">(optional)</span>
                        </label>
                        <textarea
                          value={editingChannelDescription}
                          onChange={(e) => setEditingChannelDescription(e.target.value)}
                          placeholder="What's this channel about?"
                          rows={1}
                          className="w-full px-3 py-2.5 bg-white border border-border-gray rounded-lg text-xs outline-none focus:border-text-tertiary resize-none"
                        />
                      </div>
                    </div>

                    {/* Members Section */}
                    <div className="border-t border-border-light pt-3">
                      <h3 className="flex items-center gap-2 text-sm font-medium text-gray-900 pb-3">
                        <Icon icon={Users} size={20} />
                        Members{!isLoadingChannelMembers && ` (${safeChannelMembers.length})`}
                      </h3>
                      {channelMembersError && (
                        <div className="mb-3 p-3 bg-red-50 border border-red-200 rounded-lg">
                          <p className="text-[13px] text-red-700">
                            {channelMembersError}
                          </p>
                        </div>
                      )}
                      {isLoadingChannelMembers ? (
                        <div className="space-y-2 mb-2">
                          {[...Array(3)].map((_, i) => (
                            <div key={i} className="flex items-center gap-3 px-3 py-2">
                              <div className="w-8 h-8 rounded-full bg-gray-200 animate-pulse flex-shrink-0" />
                              <div className="flex-1 space-y-2">
                                <div className="h-3 bg-gray-200 rounded animate-pulse" />
                                <div className="h-2 bg-gray-200 rounded animate-pulse w-3/4" />
                              </div>
                              <div className="w-6 h-6 bg-gray-200 rounded animate-pulse flex-shrink-0" />
                            </div>
                          ))}
                        </div>
                      ) : safeChannelMembers.length === 0 ? (
                        <p className="text-[13px] text-gray-600">
                          No members in this channel
                        </p>
                      ) : (
                        <div className="space-y-0.5 mb-2 max-h-48 overflow-y-auto overflow-x-hidden">
                          {safeChannelMembers.map((member) => {
                            const currentUserRole = safeChannelMembers.find(
                              (m) => m.user_id === user?.id,
                            )?.role;
                            const isChannelOwnerOrMod =
                              currentUserRole === "owner" ||
                              currentUserRole === "moderator";
                            const canRemove =
                              isChannelOwnerOrMod &&
                              member.role !== "owner" &&
                              member.user_id !== user?.id;

                            return (
                              <div
                                key={member.user_id}
                                className="flex items-center justify-between px-3 py-2 hover:bg-gray-50 rounded-lg transition-colors -mx-3"
                              >
                                <div className="flex items-center gap-3 min-w-0">
                                  {member.avatar_url &&
                                  !failedAvatarIds?.has(member.user_id) ? (
                                    <img
                                      src={member.avatar_url}
                                      alt={member.name || member.user_id}
                                      className="w-8 h-8 rounded-full object-cover flex-shrink-0"
                                      onError={() =>
                                        handleAvatarError(member.user_id)
                                      }
                                    />
                                  ) : (
                                    <div
                                      className="w-8 h-8 rounded-full flex-shrink-0 bg-gradient-to-br from-[#5A7864] to-[#607E98]"
                                    />
                                  )}
                                  <div className="min-w-0">
                                    <p className="text-[13px] font-medium text-gray-900 truncate">
                                      {member.name || member.user_id}
                                    </p>
                                    <p className="text-[12px] text-gray-500 capitalize">
                                      {member.role}
                                    </p>
                                  </div>
                                </div>
                                {canRemove && (
                                  <button
                                    onClick={() =>
                                      handleRemoveChannelMember(member.user_id)
                                    }
                                    className="px-2 py-1 text-[12px] text-red-500 hover:text-red-600 hover:bg-red-50 rounded-md transition-colors flex-shrink-0"
                                  >
                                    Remove
                                  </button>
                                )}
                              </div>
                            );
                          })}
                        </div>
                      )}

                      {/* Add Member Section */}
                      {(() => {
                        const currentUserRole = safeChannelMembers.find(
                          (m) => m.user_id === user?.id,
                        )?.role;
                        const isChannelOwnerOrMod =
                          currentUserRole === "owner" ||
                          currentUserRole === "moderator";

                        if (!isChannelOwnerOrMod) return null;

                        // Get members not in channel
                        const channelMemberIds = new Set(
                          safeChannelMembers.map((m) => m.user_id),
                        );
                        const availableMembers = safeWorkspaceMembers.filter(
                          (m) => !channelMemberIds.has(m.user_id),
                        );

                        return (
                          <div className="pt-2">
                            {!showAddMember ? (
                              <button
                                onClick={() => setShowAddMember(true)}
                                className="w-full px-3 py-2 text-[13px] font-medium text-gray-700 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors"
                              >
                                + Add Member
                              </button>
                            ) : (
                              <div className="space-y-3">
                                <label className="block text-[13px] font-medium text-gray-700">
                                  Select member to add
                                </label>
                                {availableMembers.length === 0 ? (
                                  <p className="text-[13px] text-gray-500">
                                    All workspace members are already in this channel
                                  </p>
                                ) : (
                                  <>
                                    <select
                                      value={addMemberUserId}
                                      onChange={(e) =>
                                        setAddMemberUserId(e.target.value)
                                      }
                                      className="w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg text-[13px] text-gray-900 focus:outline-none focus:ring-2 focus:ring-gray-900/10 focus:border-gray-400"
                                    >
                                      <option value="">Choose a member...</option>
                                      {availableMembers.map((member) => (
                                        <option
                                          key={member.user_id}
                                          value={member.user_id}
                                        >
                                          {member.name || member.email}
                                        </option>
                                      ))}
                                    </select>
                                    <div className="flex gap-2">
                                      <button
                                        type="button"
                                        onClick={() => {
                                          setShowAddMember(false);
                                          setAddMemberUserId("");
                                        }}
                                        className="flex-1 px-3 py-1.5 text-[13px] font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-md transition-colors"
                                      >
                                        Cancel
                                      </button>
                                      <button
                                        onClick={() =>
                                          handleAddChannelMember(addMemberUserId)
                                        }
                                        disabled={!addMemberUserId}
                                        className="flex-1 px-3 py-1.5 bg-gray-900 text-white rounded-md text-[13px] font-medium hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                                      >
                                        Add
                                      </button>
                                    </div>
                                  </>
                                )}
                              </div>
                            )}
                          </div>
                        );
                      })()}
                    </div>

                    {/* Collapsible Advanced Options */}
                    <div className="border-t border-border-light pt-4">
                      <button
                        onClick={() => {
                          const newState = !showAdvancedOptions;
                          setShowAdvancedOptions(newState);
                          if (newState) {
                            setTimeout(() => {
                              channelContentRef.current?.scrollTo({
                                top: channelContentRef.current.scrollHeight,
                                behavior: 'smooth',
                              });
                            }, 0);
                          }
                        }}
                        className="flex items-center gap-2 text-xs text-text-tertiary hover:text-text-secondary transition-colors"
                      >
                        <ChevronDownIcon
                          className="w-3 h-3"
                          style={{
                            transform: showAdvancedOptions ? "rotate(0deg)" : "rotate(-90deg)",
                            transition: "transform 0.15s ease",
                          }}
                        />
                        Advanced options
                      </button>
                      {showAdvancedOptions && (
                        <div className="mt-3">
                          <p className="text-xs text-text-tertiary mb-3">
                            Deleting this channel will permanently remove all messages and data. This action cannot be undone.
                          </p>
                          <button
                            onClick={() => {
                              setShowChannelDetails(false);
                              setModalChannelId(null);
                              setShowAdvancedOptions(false);
                              if (modalChannelId) handleDeleteChannel(modalChannelId);
                            }}
                            className="px-3 py-1.5 text-xs text-red-600 border border-red-200 rounded-md hover:bg-red-50 transition-colors"
                          >
                            Delete channel
                          </button>
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Channel Settings Footer */}
                  <div className="flex justify-end gap-2 px-6 py-4 border-t border-border-light">
                    <button
                      onClick={() => {
                        setShowChannelDetails(false);
                        setModalChannelId(null);
                        setShowAdvancedOptions(false);
                      }}
                      className="px-4 py-2 text-sm text-text-secondary hover:bg-bg-gray rounded-lg"
                    >
                      Cancel
                    </button>
                    <button
                      onClick={handleSaveChannelSettings}
                      disabled={!editingChannelName.trim() || isSavingChannel}
                      className="px-4 py-2 text-sm bg-black text-white rounded-lg disabled:opacity-50"
                    >
                      {isSavingChannel ? "Saving..." : "Save"}
                    </button>
                  </div>
                </>
              ) : (
                <>
                  {/* App Settings Tab Content */}
                  <div className="overflow-y-auto flex-1 px-6 py-3 space-y-4">
                    <h3 className="text-sm font-medium text-gray-900">
                      Permissions
                    </h3>
                    <div className="space-y-1">
                      {/* Notifications Toggle */}
                      <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                        <div>
                          <p className="text-sm font-medium text-gray-900">
                            Notifications
                          </p>
                          <p className="text-[13px] text-gray-500">
                            Receive notifications from this app
                          </p>
                        </div>
                        <button
                          className="relative w-11 h-6 bg-gray-200 rounded-full transition-colors cursor-not-allowed"
                          disabled
                        >
                          <span className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full shadow transition-transform" />
                        </button>
                      </div>
                    </div>
                  </div>

                  {/* App Settings Footer */}
                  <div className="flex justify-end gap-2 px-6 py-4 border-t border-border-light">
                    <button
                      onClick={() => {
                        setShowChannelDetails(false);
                        setModalChannelId(null);
                        setShowAdvancedOptions(false);
                      }}
                      className="px-4 py-2 text-sm text-text-secondary hover:bg-bg-gray rounded-lg"
                    >
                      Cancel
                    </button>
                    <button
                      disabled
                      className="px-4 py-2 text-sm bg-black text-white rounded-lg disabled:opacity-50"
                    >
                      Save
                    </button>
                  </div>
                </>
              )}
            </motion.div>
          </motion.div>,
          document.body,
        )}

      {/* Image Lightbox */}
      {lightboxImageUrl &&
        createPortal(
          <div
            className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/80"
            onClick={() => setLightboxImageUrl(null)}
          >
            {/* Close button */}
            <button
              onClick={() => setLightboxImageUrl(null)}
              className="absolute top-4 right-4 p-2 text-white/80 hover:text-white transition-colors"
              aria-label="Close"
            >
              <XMarkIcon className="w-6 h-6" />
            </button>
            {/* Image */}
            <img
              src={lightboxImageUrl}
              alt="Full size"
              className="max-w-[90vw] max-h-[90vh] object-contain rounded-lg"
              onClick={(e) => e.stopPropagation()}
            />
          </div>,
          document.body,
        )}
      </div>
    </div>
  );
}
