import { useState, useEffect, useRef, useMemo, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { createPortal } from "react-dom";
import { useIsRestoring } from "@tanstack/react-query";
import {
  StarIcon,
  ArchiveBoxIcon,
  TrashIcon,
  ArrowUturnLeftIcon,
  ArrowPathIcon,
  PaperAirplaneIcon,
  PaperClipIcon,
  EllipsisHorizontalIcon,
  ChevronRightIcon,
  ChevronDownIcon,
  PencilIcon,
  EnvelopeOpenIcon,
  MagnifyingGlassIcon,
  XMarkIcon,
  CloudArrowDownIcon,
} from "@heroicons/react/24/outline";
import { motion, AnimatePresence } from "motion/react";
import { Pencil, ArrowUpLeft, ArrowUpRight } from "lucide-react";
import { Icon } from "../ui/Icon";
import { StarIcon as StarIconSolid } from "@heroicons/react/24/solid";
import { useInView } from "react-intersection-observer";
import { SIDEBAR } from "../../lib/sidebar";
import { useEmailStore, type EmailFolder } from "../../stores/emailStore";
import { useKeyboardNavigation } from "../../hooks/useKeyboardNavigation";
import type { Email, EmailWithAttachments } from "../../api/client";
import {
  useEmailFolder,
  useEmailCounts,
  useEmailDetail,
  useEmailThread,
  useEmailSearch,
  useMarkEmailsRead,
  useArchiveEmail,
  useDeleteEmail,
  useRestoreEmail,
  useDeleteDraft,
  useSyncEmails,
  usePrefetchFolder,
  usePrefetchEmailDetails,
  flattenEmailPages,
  type EmailFolder as RQEmailFolder,
} from "../../hooks/queries/useEmails";
import { ComposeEmail } from "./ComposeEmail";
import { InlineReplyComposer } from "./InlineReplyComposer";
import { AttachmentList } from "./AttachmentList";
import { HeaderButtons } from "../MiniAppHeader";
import EmailSettingsDropdown from "./EmailSettingsDropdown";
import { sanitizeEmailHtml } from "../../utils/sanitizeHtml";

// Get initials from name or email
function getInitials(name?: string, email?: string): string {
  if (name) {
    const parts = name.split(" ").filter(Boolean);
    if (parts.length >= 2) {
      return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    }
    return parts[0]?.[0]?.toUpperCase() || "U";
  }
  if (email) {
    return email[0]?.toUpperCase() || "U";
  }
  return "U"; // "Unknown" instead of "?"
}

// Generate consistent color from string
function getAvatarColor(str?: string): string {
  const colors = [
    "#5E5CE6", // purple
    "#FF375F", // red
    "#FF9F0A", // orange
    "#30D158", // green
    "#64D2FF", // blue
    "#BF5AF2", // violet
    "#FF6482", // pink
    "#40C8E0", // teal
  ];
  if (!str) return colors[0];
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  return colors[Math.abs(hash) % colors.length];
}

// Decode HTML entities in text (for email snippets that contain encoded characters)
function decodeHtmlEntities(text: string): string {
  const textarea = document.createElement("textarea");
  textarea.innerHTML = text;
  return textarea.value;
}

// Strip quoted reply content from email snippet for cleaner display
function stripQuotedFromSnippet(snippet: string): string {
  // Broad pattern: "On [anything with a year], [name/email] wrote:"
  const quotePattern =
    /\s*On\s+[\w,.\s\/]+\d{4}[\w,.\s:]*(?:<[^>]+>|\[[^\]]*\][>]?|[^<\[]*)\s*wrote:.*/is;
  const match = snippet.match(quotePattern);
  if (match && match.index !== undefined && match.index > 0) {
    return snippet.slice(0, match.index).trim();
  }

  return snippet;
}

// Format date Apple Mail style
function formatDate(dateString?: string): string {
  if (!dateString) return "";
  const date = new Date(dateString);
  if (isNaN(date.getTime())) return "";
  const now = new Date();

  if (date.toDateString() === now.toDateString()) {
    return date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  }

  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  if (date.toDateString() === yesterday.toDateString()) {
    return "Yesterday";
  }

  const diffMs = now.getTime() - date.getTime();
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
  if (diffDays < 7) {
    return date.toLocaleDateString([], { weekday: "short" });
  }

  if (date.getFullYear() === now.getFullYear()) {
    return date.toLocaleDateString([], { month: "short", day: "numeric" });
  }

  return date.toLocaleDateString([], {
    month: "short",
    day: "numeric",
    year: "2-digit",
  });
}

// Split email HTML into main content and quoted content (for replies only, not forwards)
function splitQuotedContent(html: string): {
  mainContent: string;
  quotedContent: string | null;
} {
  // Skip splitting for forwarded messages - the forwarded content IS the main content
  const forwardPattern = /-{5,}\s*Forwarded message\s*-{5,}/i;
  if (forwardPattern.test(html)) {
    return { mainContent: html, quotedContent: null };
  }

  // Patterns that indicate quoted reply content (HTML elements)
  const htmlPatterns = [
    /<div\s+class=["']gmail_quote["'][^>]*>[\s\S]*$/i,
    /<blockquote[^>]*>[\s\S]*$/i,
  ];

  for (const pattern of htmlPatterns) {
    const match = html.match(pattern);
    if (match && match.index !== undefined) {
      const mainContent = html.slice(0, match.index).trim();
      const quotedContent = match[0];
      // Only split if there's meaningful main content
      if (mainContent && mainContent !== "<p></p>" && mainContent !== "<br>") {
        return { mainContent, quotedContent };
      }
    }
  }

  // Text-based pattern: "On [date], [name] wrote:" (common email reply quote format)
  // Matches variations like:
  // - "On 2/2/2026, 10:38:58 AM, Jane Doe <jane@example.com> wrote:"
  // - "On Mon, Feb 2, 2026 at 10:38 AM John wrote:"
  // - "On Wed, February 18, 2026 3:45 PM, Name [email@example.com]> wrote:"
  const textQuotePattern =
    /On\s+[\w,.\s\/]+\d{4}[\w,.\s:]*(?:&lt;[^&]*&gt;|<[^>]*>|\[[^\]]*\][>]?)?\s*wrote:/i;
  const textMatch = html.match(textQuotePattern);
  if (textMatch && textMatch.index !== undefined) {
    const mainContent = html.slice(0, textMatch.index).trim();
    const quotedContent = html.slice(textMatch.index);
    // Only split if there's meaningful main content
    if (mainContent && mainContent !== "<p></p>" && mainContent !== "<br>") {
      return { mainContent, quotedContent };
    }
  }

  return { mainContent: html, quotedContent: null };
}

// Convert plain text email body to HTML with proper formatting
// Handles > quoted lines, "On ... wrote:" headers, and preserves line breaks
function plainTextToHtml(text: string): string {
  const escaped = text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

  const lines = escaped.split("\n");
  const parts: string[] = [];
  let inQuote = false;
  const quoteBuffer: string[] = [];

  const flushQuote = () => {
    if (quoteBuffer.length > 0) {
      parts.push(`<blockquote>${quoteBuffer.join("<br>")}</blockquote>`);
      quoteBuffer.length = 0;
    }
    inQuote = false;
  };

  for (const line of lines) {
    // Lines starting with > (after our HTML escaping, > becomes &gt;)
    if (line.startsWith("&gt; ") || line === "&gt;") {
      if (!inQuote) inQuote = true;
      quoteBuffer.push(line.replace(/^&gt;\s?/, ""));
    } else {
      if (inQuote) flushQuote();

      if (line.trim() === "") {
        parts.push("<br>");
      } else {
        parts.push(`<p>${line}</p>`);
      }
    }
  }
  if (inQuote) flushQuote();

  return parts.join("\n");
}

// Thread info type - extends Email with thread metadata
interface EmailWithThreadInfo extends Email {
  threadCount: number;
  threadEmails: Email[]; // All emails in this thread, sorted oldest first
}

// Group emails by thread and return the most recent email from each thread with thread info
function groupEmailsByThread(emails: Email[]): EmailWithThreadInfo[] {
  // Group by thread_id
  const threadMap = new Map<string, Email[]>();

  emails.forEach((email) => {
    const threadId = email.thread_id || email.id; // Fallback to email id if no thread
    const existing = threadMap.get(threadId) || [];
    threadMap.set(threadId, [...existing, email]);
  });

  // For each thread, return the most recent email with thread info
  const result: EmailWithThreadInfo[] = [];

  threadMap.forEach((threadEmails) => {
    // Sort by date, newest first
    const sorted = [...threadEmails].sort(
      (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
    );

    const mostRecent = sorted[0];

    // Check if any email in thread is unread
    const hasUnread = threadEmails.some((e) => !e.is_read);

    // Use API-provided message_count (accurate, includes all thread messages)
    // with fallback to local count
    const apiThreadCount = Math.max(...threadEmails.map(e => e.message_count || 0));
    const threadCount = apiThreadCount > threadEmails.length ? apiThreadCount : threadEmails.length;

    result.push({
      ...mostRecent,
      is_read: !hasUnread, // Thread is "unread" if any email is unread
      threadCount,
      threadEmails: sorted.reverse(), // Oldest first for display
    });
  });

  // Sort threads by most recent email date
  return result.sort(
    (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
  );
}

// Group emails by date
function groupEmailsByDate(
  emails: EmailWithThreadInfo[],
): { label: string; emails: EmailWithThreadInfo[] }[] {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  const thisWeekStart = new Date(today);
  thisWeekStart.setDate(thisWeekStart.getDate() - thisWeekStart.getDay());
  const lastWeekStart = new Date(thisWeekStart);
  lastWeekStart.setDate(lastWeekStart.getDate() - 7);
  const thisMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);

  const groups: Record<string, EmailWithThreadInfo[]> = {};

  emails.forEach((email) => {
    if (!email.date) {
      const existing = groups["Other"] || [];
      groups["Other"] = [...existing, email];
      return;
    }
    const date = new Date(email.date);
    if (isNaN(date.getTime())) {
      const existing = groups["Other"] || [];
      groups["Other"] = [...existing, email];
      return;
    }
    const emailDate = new Date(
      date.getFullYear(),
      date.getMonth(),
      date.getDate(),
    );

    let label: string;
    if (emailDate >= today) {
      label = "Today";
    } else if (emailDate >= yesterday) {
      label = "Yesterday";
    } else if (emailDate >= thisWeekStart) {
      label = "This Week";
    } else if (emailDate >= lastWeekStart) {
      label = "Last Week";
    } else if (emailDate >= thisMonthStart) {
      label = "This Month";
    } else if (date.getFullYear() === now.getFullYear()) {
      label = date.toLocaleDateString([], { month: "long" });
    } else {
      label = date.toLocaleDateString([], { month: "long", year: "numeric" });
    }

    if (!groups[label]) groups[label] = [];
    groups[label].push(email);
  });

  const order = ["Today", "Yesterday", "This Week", "Last Week", "This Month"];
  return Object.entries(groups)
    .sort(([a], [b]) => {
      const aIdx = order.indexOf(a);
      const bIdx = order.indexOf(b);
      if (aIdx !== -1 && bIdx !== -1) return aIdx - bIdx;
      if (aIdx !== -1) return -1;
      if (bIdx !== -1) return 1;
      return 0;
    })
    .map(([label, emails]) => ({ label, emails }));
}

const FOLDER_LIST: {
  id: EmailFolder;
  name: string;
  icon: React.ComponentType<{ className?: string }>;
}[] = [
  { id: "INBOX", name: "Inbox", icon: EnvelopeOpenIcon },
  { id: "STARRED", name: "Flagged", icon: StarIcon },
  { id: "SENT", name: "Sent", icon: PaperAirplaneIcon },
  { id: "DRAFT", name: "Drafts", icon: PencilIcon },
  { id: "TRASH", name: "Trash", icon: TrashIcon },
];

// TTL for read overrides (must match emailStore.ts and useEmails.ts)
const READ_OVERRIDE_TTL_MS = 5 * 60 * 1000;

export default function EmailView() {
  // Store state
  const {
    activeFolder,
    selectedEmailId,
    accountsStatus,
    selectedAccountIds,
    setActiveFolder,
    setSelectedEmailId,
    setSelectedAccounts,
    setAccountsStatus,
    openCompose,
    openComposeWithDraft,
    openInlineReply,
    openInlineForward,
    inlineReply,
    fetchingRemoteId,
    fetchRemoteAndOpen,
    // For thread email body loading
    emailDetailsCache,
    fetchEmailDetails,
    loadingDetailsId,
    // Legacy compatibility (empty, React Query provides data)
    accountFolders,
    getAccountKey,
    prefetchAllAccounts,
    // Read overrides to prevent stale unread state on reload
    readOverrides,
    // Optimistic reply to show immediately while sending
    optimisticReply,
  } = useEmailStore();

  // Handle deep-link: /email?open=<emailId>&thread=<threadId> (from chat email cards, etc.)
  const [searchParams, setSearchParams] = useSearchParams();
  const openEmailId = searchParams.get("open");
  const openThreadId = searchParams.get("thread");
  const deepLinkThreadRef = useRef<string | null>(null);
  useEffect(() => {
    if (!openEmailId && !openThreadId) return;
    if (openThreadId) deepLinkThreadRef.current = openThreadId;
    setSelectedEmailId(openEmailId ?? openThreadId ?? null);
    setSearchParams({}, { replace: true });
  }, [openEmailId, openThreadId, setSelectedEmailId, setSearchParams]);

  // Derive account key
  const accountKey = getAccountKey();

  // Check if React Query cache is still being restored from localStorage
  // During restoration, we should not show a spinner - data will appear shortly
  const isRestoring = useIsRestoring();

  // React Query hooks for data fetching
  const {
    data: emailsData,
    isLoading,
    isFetching,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
  } = useEmailFolder(activeFolder as RQEmailFolder, selectedAccountIds);

  const { data: counts } = useEmailCounts(selectedAccountIds);

  // Search state
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState("");
  const [isSearchActive, setIsSearchActive] = useState(false);

  // Category filter state (for inbox only)
  type CategoryFilter = "personal" | "promotions" | "social";
  const [categoryFilter, setCategoryFilter] =
    useState<CategoryFilter>("personal");
  const { data: searchData, isFetching: isSearching } = useEmailSearch(
    debouncedSearchQuery,
    selectedAccountIds,
    isSearchActive && debouncedSearchQuery.length > 0,
  );
  const searchResults = searchData?.emails ?? null;
  const searchMeta = searchData
    ? {
        local_count: searchData.local_count,
        remote_count: searchData.remote_count,
        provider_errors: searchData.provider_errors,
        has_provider_errors: searchData.has_provider_errors,
      }
    : null;

  // Mutations
  const markReadMutation = useMarkEmailsRead(
    activeFolder as RQEmailFolder,
    selectedAccountIds,
  );
  const archiveMutation = useArchiveEmail(
    activeFolder as RQEmailFolder,
    selectedAccountIds,
  );
  const deleteMutation = useDeleteEmail(
    activeFolder as RQEmailFolder,
    selectedAccountIds,
  );
  const restoreMutation = useRestoreEmail(
    activeFolder as RQEmailFolder,
    selectedAccountIds,
  );
  const deleteDraftMutation = useDeleteDraft(selectedAccountIds);
  const syncMutation = useSyncEmails();

  // Prefetch helper for email details (body content)
  const prefetchEmailDetails = usePrefetchEmailDetails();

  // Hover prefetch helper
  const prefetchFolder = usePrefetchFolder();

  // Sync accountsStatus from React Query to Zustand store for use in openInlineReply
  useEffect(() => {
    const apiAccountsStatus = emailsData?.pages?.[0]?.accountsStatus;
    if (apiAccountsStatus && apiAccountsStatus.length > 0) {
      setAccountsStatus(apiAccountsStatus);
    }
  }, [emailsData?.pages, setAccountsStatus]);

  // Get current folders for this account (legacy - mostly empty, React Query has data)
  const currentFolders = accountFolders[accountKey];

  // Flatten paginated emails from React Query
  const rqEmails = flattenEmailPages(emailsData);

  // Use store's persisted emails as fallback for instant initial load
  const storeEmails = currentFolders?.[activeFolder]?.emails ?? [];

  // Compute the ID of the email we need detail for (latest in thread, or selected if no thread)
  // This must be computed before useEmailDetail so we fetch the right email's body
  const emailIdForDetail = useMemo(() => {
    if (!selectedEmailId) return null;

    const allEmails = rqEmails.length > 0 ? rqEmails : storeEmails;
    const selectedEmail = allEmails.find((e) => e.id === selectedEmailId);
    if (!selectedEmail) return null;

    if (!selectedEmail.thread_id) return selectedEmail.id;

    // Find the latest email in this thread
    const threadEmails = allEmails.filter(
      (e) => e.thread_id === selectedEmail.thread_id,
    );
    if (threadEmails.length <= 1) return selectedEmail.id;

    // Sort by date and return the latest
    threadEmails.sort(
      (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
    );
    return threadEmails[0]?.id ?? selectedEmail.id;
  }, [selectedEmailId, rqEmails, storeEmails]);

  // Fetch email detail (body content) via React Query - for the latest thread email
  const { data: emailDetailData } = useEmailDetail(emailIdForDetail);

  // Use React Query emails (already has readOverrides applied in the hook)
  // Fall back to store cache for instant initial load before React Query hydrates
  const emails = useMemo(() => {
    // Prefer React Query data (has readOverrides applied)
    if (rqEmails.length > 0) {
      return rqEmails;
    }
    // Fall back to store cache, apply readOverrides manually for store data
    if (storeEmails.length > 0 && Object.keys(readOverrides).length > 0) {
      const now = Date.now();
      return storeEmails.map((email) => {
        const overrideAt = readOverrides[email.id];
        if (
          overrideAt &&
          now - overrideAt <= READ_OVERRIDE_TTL_MS &&
          !email.is_read
        ) {
          return { ...email, is_read: true };
        }
        return email;
      });
    }
    return storeEmails;
  }, [rqEmails, storeEmails, readOverrides]);

  // Infinite scroll trigger - defined early but effect runs after filteredEmails is computed
  const { ref: loadMoreRef, inView } = useInView();

  // Derive loading states
  const isLoadingMore = isFetchingNextPage;
  const isSyncing = syncMutation.isPending;
  const error = null; // Errors now handled per-query

  // Clear stale selection when the selected email no longer exists in the loaded list.
  // If exact id match fails, try to find a thread match (handles chat deep-links
  // where the message id may differ from the inbox's latest thread message id).
  useEffect(() => {
    if (!selectedEmailId || isLoading || isFetching) return;
    const allEmails = rqEmails.length > 0 ? rqEmails : storeEmails;
    if (allEmails.some((email) => email.id === selectedEmailId)) return;

    const threadId = deepLinkThreadRef.current;
    if (threadId) {
      const byThread = allEmails.find((email) => email.thread_id === threadId);
      if (byThread) {
        deepLinkThreadRef.current = null;
        setSelectedEmailId(byThread.id);
        return;
      }
    }
    setSelectedEmailId(null);
  }, [
    selectedEmailId,
    isLoading,
    isFetching,
    rqEmails,
    storeEmails,
    setSelectedEmailId,
  ]);

  // Get selected email with detail merged from React Query cache
  const selectedEmail = useMemo((): Email | null => {
    if (!selectedEmailId) return null;
    const email = emails.find((e) => e.id === selectedEmailId);
    if (!email) return null;
    // Merge with detail data from React Query (includes body_html, body_text)
    if (emailDetailData) {
      return { ...email, ...emailDetailData };
    }
    // Fallback to store cache for backwards compatibility
    const cachedDetail = emailDetailsCache[selectedEmailId];
    if (cachedDetail) {
      return { ...email, ...cachedDetail };
    }
    return email;
  }, [selectedEmailId, emails, emailDetailData, emailDetailsCache]);

  // Fetch thread emails from API when viewing a thread
  const selectedThreadId = selectedEmail?.thread_id ?? null;
  const { data: threadData } = useEmailThread(selectedThreadId);

  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const preloadedIdsRef = useRef<Set<string>>(new Set());
  const replyComposerRef = useRef<HTMLDivElement>(null);

  // Track which accounts are expanded to show folders
  const [expandedAccounts, setExpandedAccounts] = useState<Set<string>>(
    new Set(["all"]),
  );
  const [expandedThreadEmails, setExpandedThreadEmails] = useState<Set<string>>(
    new Set(),
  );

  // Search UI state
  const [isSearchOpen, setIsSearchOpen] = useState(false);
  const searchInputRef = useRef<HTMLInputElement>(null);

  const handleSearchChange = useCallback((value: string) => {
    setSearchQuery(value);
    setIsSearchActive(value.trim().length > 0);
  }, []);

  const clearSearch = useCallback(() => {
    setSearchQuery("");
    setDebouncedSearchQuery("");
    setIsSearchActive(false);
  }, []);

  // Debounce provider search requests
  useEffect(() => {
    if (!isSearchActive || !searchQuery.trim()) {
      setDebouncedSearchQuery("");
      return;
    }
    const timer = setTimeout(() => {
      setDebouncedSearchQuery(searchQuery.trim());
    }, 300);
    return () => clearTimeout(timer);
  }, [searchQuery, isSearchActive]);

  // Thread attachments map
  const [threadAttachmentsMap, setThreadAttachmentsMap] = useState<
    Record<string, EmailWithAttachments[]>
  >({});

  // Track whether to show quoted content in emails
  const [showQuotedContent, setShowQuotedContent] = useState(false);

  // Settings dropdown
  const [showSettingsDropdown, setShowSettingsDropdown] = useState(false);
  const settingsButtonRef = useRef<HTMLButtonElement>(null);

  // Lightbox for image attachments
  const [lightboxImageUrl, setLightboxImageUrl] = useState<string | null>(null);

  // Reset quoted content visibility when selecting a different email
  useEffect(() => {
    setShowQuotedContent(false);
  }, [selectedEmailId]);

  // Scroll to reply composer when it opens
  useEffect(() => {
    if (inlineReply.isOpen && replyComposerRef.current) {
      // Wait for TipTap editor to fully initialize before scrolling
      setTimeout(() => {
        replyComposerRef.current?.scrollIntoView({
          behavior: "smooth",
          block: "end",
        });
      }, 200);
    }
  }, [inlineReply.isOpen]);

  // React Query automatically re-fetches when selectedAccountIds changes (part of query key)

  // Prefetch all accounts once we know what accounts exist
  useEffect(() => {
    if (accountsStatus.length > 0) {
      prefetchAllAccounts();
    }
  }, [accountsStatus.length, prefetchAllAccounts]);

  // Clear search when folder changes (React Query handles data fetching via query key)
  useEffect(() => {
    preloadedIdsRef.current.clear();
    if (isSearchActive || searchQuery.trim()) {
      clearSearch();
      setIsSearchOpen(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeFolder]);

  const handleSync = async () => {
    await syncMutation.mutateAsync();
  };

  const resolveDraftDeleteId = (email: Email): string => {
    if (email.gmail_draft_id) return email.gmail_draft_id;

    const rawItem = email.raw_item;
    if (rawItem && typeof rawItem === "object") {
      if (
        typeof rawItem.gmail_draft_id === "string" &&
        rawItem.gmail_draft_id
      ) {
        return rawItem.gmail_draft_id;
      }
      if (rawItem.message && typeof rawItem.id === "string" && rawItem.id) {
        return rawItem.id;
      }
    }
    return email.id;
  };

  const handleEmailClick = async (email: EmailWithThreadInfo | Email) => {
    // Handle remote-only search results — fetch from provider first
    if ("source" in email && email.source === "remote") {
      fetchRemoteAndOpen(email);
      return;
    }

    const emailId = email.id;
    const isDraft =
      email.label_ids?.some((l) => l.toUpperCase() === "DRAFT") ||
      activeFolder === "DRAFT";

    // Drafts should open in compose, not read-only detail
    if (isDraft) {
      await openComposeWithDraft(email as Email);
      return;
    }

    // Set selected immediately for responsive UI
    setSelectedEmailId(emailId);

    // Prefetch adjacent emails for faster keyboard navigation
    const currentIndex = flatEmailList.findIndex((e) => e.id === emailId);
    if (currentIndex !== -1) {
      const adjacentIds: string[] = [];
      // Prefetch next/previous email only to avoid request bursts
      for (let i = -1; i <= 1; i++) {
        if (i === 0) continue; // Skip current email
        const adjacentEmail = flatEmailList[currentIndex + i];
        if (adjacentEmail) {
          adjacentIds.push(adjacentEmail.id);
        }
      }
      if (adjacentIds.length > 0) {
        prefetchEmailDetails(adjacentIds);
      }
    }

    // Mark as read optimistically (clicked email + unread siblings in loaded thread)
    const unreadIds: string[] = [];
    const seenIds = new Set<string>();

    if (!email.is_read) {
      unreadIds.push(emailId);
      seenIds.add(emailId);
    }

    if (email.thread_id) {
      const loadedEmails = rqEmails.length > 0 ? rqEmails : storeEmails;
      loadedEmails.forEach((loadedEmail) => {
        if (
          loadedEmail.thread_id === email.thread_id &&
          !loadedEmail.is_read &&
          !seenIds.has(loadedEmail.id)
        ) {
          seenIds.add(loadedEmail.id);
          unreadIds.push(loadedEmail.id);
        }
      });
    }

    if (unreadIds.length > 0) {
      markReadMutation.mutate(unreadIds);
    }

    // Email details are prefetched on hover/initial load, accessed from emailDetailsCache

    // Keep focus on the list container for keyboard navigation
    setTimeout(() => {
      emailListRef.current?.focus();
    }, 0);
  };

  const handleArchive = async (email: Email) => {
    await archiveMutation.mutateAsync(email.id);
  };

  const handleRestore = async (email: Email) => {
    await restoreMutation.mutateAsync(email.id);
    setSelectedEmailId(null);
  };

  const handleDelete = async (email: Email) => {
    const isDraft =
      email.label_ids?.some((l) => l.toUpperCase() === "DRAFT") ||
      activeFolder === "DRAFT";
    if (isDraft) {
      await deleteDraftMutation.mutateAsync(resolveDraftDeleteId(email));
      if (selectedEmailId === email.id) {
        setSelectedEmailId(null);
      }
      return;
    }
    await deleteMutation.mutateAsync(email.id);
  };

  // Filter emails by search query — prefer server results, fall back to client-side
  // Then apply category filter for inbox
  const filteredEmails = useMemo(() => {
    let result = emails;

    // Apply search filter
    if (searchQuery.trim()) {
      if (searchResults !== null) {
        result = searchResults;
      } else {
        // Client-side filter while server is loading
        const q = searchQuery.toLowerCase();
        result = result.filter(
          (email) =>
            email.subject?.toLowerCase().includes(q) ||
            email.from_name?.toLowerCase().includes(q) ||
            email.from_email?.toLowerCase().includes(q) ||
            email.snippet?.toLowerCase().includes(q),
        );
      }
    }

    // Apply category filter (only for inbox)
    if (activeFolder === "INBOX" && !searchQuery.trim()) {
      result = result.filter((email) => {
        const labels = email.label_ids || [];
        const hasCategory = (cat: string) =>
          labels.some(
            (l) => l.toUpperCase() === `CATEGORY_${cat.toUpperCase()}`,
          );

        if (categoryFilter === "personal") {
          // Personal = everything EXCEPT promotions and social
          // This includes: PRIMARY, PERSONAL, UPDATES, FORUMS, or no category
          return !hasCategory("PROMOTIONS") && !hasCategory("SOCIAL");
        } else if (categoryFilter === "promotions") {
          return hasCategory("PROMOTIONS");
        } else if (categoryFilter === "social") {
          return hasCategory("SOCIAL");
        }
        return true;
      });
    }

    return result;
  }, [emails, searchQuery, searchResults, activeFolder, categoryFilter]);

  // Infinite scroll effect - only fetch more when:
  // 1. User scrolls to bottom (inView)
  // 2. There are more pages to fetch
  // 3. Not already fetching
  // 4. There are filtered emails visible (prevents fetching when category shows 0 results)
  useEffect(() => {
    if (
      inView &&
      hasNextPage &&
      !isFetchingNextPage &&
      filteredEmails.length > 0
    ) {
      fetchNextPage();
    }
  }, [
    inView,
    hasNextPage,
    isFetchingNextPage,
    filteredEmails.length,
    fetchNextPage,
  ]);

  // First group emails by thread, then by date
  // Skip threading for DRAFT and SENT folders - show each email individually
  const threadedEmails = useMemo(() => {
    if (activeFolder === "DRAFT" || activeFolder === "SENT") {
      // Don't group drafts/sent - each is its own item
      return filteredEmails.map((email) => ({
        ...email,
        threadCount: 1,
        threadEmails: [email],
      }));
    }
    return groupEmailsByThread(filteredEmails);
  }, [filteredEmails, activeFolder]);
  const groupedEmails = useMemo(
    () => groupEmailsByDate(threadedEmails),
    [threadedEmails],
  );

  // Flat list of all emails for keyboard navigation
  const flatEmailList = useMemo(() => {
    return groupedEmails.flatMap((group) => group.emails);
  }, [groupedEmails]);

  // Get thread emails - prefer API data, fall back to local search
  // Aggressively deduplicate to prevent showing draft-like duplicates
  const selectedThreadEmails = useMemo(() => {
    if (!selectedEmail || !selectedEmail.thread_id)
      return [selectedEmail].filter(Boolean) as Email[];

    // Build a map of local emails for merging missing fields
    const localEmailMap = new Map<string, Email>();
    rqEmails.forEach((email) => localEmailMap.set(email.id, email));
    storeEmails.forEach((email) => {
      if (!localEmailMap.has(email.id)) {
        localEmailMap.set(email.id, email);
      }
    });

    const processEmails = (emails: Email[]) => {
      // Deduplicate by ID only - aggressive content-based deduplication was eating legitimate emails
      const seenIds = new Set<string>();
      const deduplicated = emails.filter((email) => {
        if (seenIds.has(email.id)) return false;
        seenIds.add(email.id);
        return true;
      });

      // Enrich with local data if API data is missing fields
      return deduplicated
        .map((email) => {
          const localEmail = localEmailMap.get(email.id);
          if (localEmail && (!email.from_email || !email.from_name)) {
            return {
              ...email,
              from_email: email.from_email || localEmail.from_email,
              from_name: email.from_name || localEmail.from_name,
            };
          }
          return email;
        })
        .sort(
          (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime(),
        );
    };

    // If we have thread data from API, use it - with aggressive deduplication
    let result: Email[];
    if (threadData?.emails && threadData.emails.length > 0) {
      result = processEmails([...threadData.emails]);
    } else {
      // Fall back to searching local data (React Query + Zustand store)
      const threadEmails: Email[] = [];
      localEmailMap.forEach((email) => {
        if (email.thread_id === selectedEmail.thread_id) {
          threadEmails.push(email);
        }
      });
      result = processEmails(threadEmails);
    }

    // Append optimistic reply if it matches this thread
    if (
      optimisticReply &&
      optimisticReply.thread_id === selectedEmail.thread_id
    ) {
      result = [...result, optimisticReply];
    }

    return result;
  }, [selectedEmail, threadData, rqEmails, storeEmails, optimisticReply]);

  // Get the latest (most recent) email in the thread for display at bottom
  const latestThreadEmail = useMemo(() => {
    if (selectedThreadEmails.length === 0) return selectedEmail;
    const latest = selectedThreadEmails[selectedThreadEmails.length - 1];
    // Merge with React Query detail data if this is the email we fetched
    if (latest.id === emailIdForDetail && emailDetailData) {
      return { ...latest, ...emailDetailData };
    }
    // Fall back to store cache for other thread emails
    return emailDetailsCache[latest.id] || latest;
  }, [
    selectedThreadEmails,
    selectedEmail,
    emailIdForDetail,
    emailDetailData,
    emailDetailsCache,
  ]);

  // Split latest email content into main and quoted parts
  // For plain-text-only emails, convert to HTML first so they get the same treatment
  const {
    mainContent: latestEmailMainContent,
    quotedContent: latestEmailQuotedContent,
  } = useMemo(() => {
    if (latestThreadEmail?.body_html) {
      return splitQuotedContent(sanitizeEmailHtml(latestThreadEmail.body_html));
    }
    const textBody = latestThreadEmail?.body_text || latestThreadEmail?.snippet;
    if (textBody) {
      return splitQuotedContent(plainTextToHtml(textBody));
    }
    return { mainContent: null, quotedContent: null };
  }, [
    latestThreadEmail?.body_html,
    latestThreadEmail?.body_text,
    latestThreadEmail?.snippet,
  ]);

  // Generate iframe srcDoc for the latest thread email's HTML content (main content only)
  const latestEmailHtmlContent = useMemo(() => {
    if (!latestEmailMainContent) return null;
    const iframeStyles = `
      body {
        font-family: 'Helvetica Neue', Helvetica, system-ui, -apple-system, sans-serif;
        font-size: 14px;
        line-height: 1.5;
        color: #000000;
        margin: 0;
        padding: 8px 24px;
        background: #fff;
        overflow: hidden;
      }
      /* Center email content - many HTML emails use tables */
      body > table,
      body > div,
      body > center {
        margin-left: auto;
        margin-right: auto;
      }
      img { max-width: 100%; height: auto; }
      a { color: #2486FF; }
      blockquote {
        margin: 0;
        padding-left: 12px;
        border-left: 3px solid #E7E7E6;
        color: #000000a8;
      }
      pre, code {
        font-family: 'Chivo Mono', monospace;
        font-size: 13px;
        background: #F4F3F1;
        border-radius: 4px;
      }
      pre { padding: 12px; overflow-x: auto; }
      code { padding: 2px 4px; }
      table { border-collapse: collapse; }
      td, th { padding: 8px; }
    `;
    return `
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <base target="_blank">
        <style>${iframeStyles}</style>
      </head>
      <body>${latestEmailMainContent}</body>
      </html>
    `;
  }, [latestEmailMainContent]);

  // Generate iframe srcDoc for quoted content
  const quotedContentHtml = useMemo(() => {
    if (!latestEmailQuotedContent) return null;
    const iframeStyles = `
      body {
        font-family: 'Helvetica Neue', Helvetica, system-ui, -apple-system, sans-serif;
        font-size: 14px;
        line-height: 1.5;
        color: #000000;
        margin: 0;
        padding: 8px 24px;
        background: #fff;
        overflow: hidden;
      }
      /* Center email content - many HTML emails use tables */
      body > table,
      body > div,
      body > center {
        margin-left: auto;
        margin-right: auto;
      }
      img { max-width: 100%; height: auto; }
      a { color: #2486FF; }
      blockquote {
        margin: 0;
        padding-left: 12px;
        border-left: 3px solid #E7E7E6;
        color: #000000a8;
      }
      pre, code {
        font-family: 'Chivo Mono', monospace;
        font-size: 13px;
        background: #F4F3F1;
        border-radius: 4px;
      }
      pre { padding: 12px; overflow-x: auto; }
      code { padding: 2px 4px; }
      table { border-collapse: collapse; }
      td, th { padding: 8px; }
    `;
    return `
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <base target="_blank">
        <style>${iframeStyles}</style>
      </head>
      <body>${latestEmailQuotedContent}</body>
      </html>
    `;
  }, [latestEmailQuotedContent]);

  // Preload a single email in the background using React Query
  const preloadEmail = useCallback(
    (emailId: string) => {
      if (preloadedIdsRef.current.has(emailId)) return;
      preloadedIdsRef.current.add(emailId);
      // Use React Query prefetch for better caching and deduplication
      prefetchEmailDetails([emailId]);
    },
    [prefetchEmailDetails],
  );

  // Preload adjacent emails around a given index
  const preloadAdjacent = useCallback(
    (currentIndex: number, range: number = 1) => {
      if (flatEmailList.length === 0) return;

      const start = Math.max(0, currentIndex - range);
      const end = Math.min(flatEmailList.length - 1, currentIndex + range);

      for (let i = start; i <= end; i++) {
        const email = flatEmailList[i];
        if (
          email &&
          !email.body_html &&
          !email.body_text &&
          !("source" in email && email.source === "remote")
        ) {
          preloadEmail(email.id);
        }
      }
    },
    [flatEmailList, preloadEmail],
  );

  // Preload first N emails on initial load
  useEffect(() => {
    if (flatEmailList.length > 0) {
      const preloadCount = Math.min(3, flatEmailList.length);
      for (let i = 0; i < preloadCount; i++) {
        const email = flatEmailList[i];
        if (
          email &&
          !email.body_html &&
          !email.body_text &&
          !("source" in email && email.source === "remote")
        ) {
          preloadEmail(email.id);
        }
      }
    }
  }, [flatEmailList, preloadEmail]);

  // Preload adjacent emails when selection changes
  useEffect(() => {
    if (selectedEmailId && flatEmailList.length > 0) {
      const currentIndex = flatEmailList.findIndex(
        (e) => e.id === selectedEmailId,
      );
      if (currentIndex !== -1) {
        preloadAdjacent(currentIndex);
      }
    }
  }, [selectedEmailId, flatEmailList, preloadAdjacent]);

  // Reuse thread query payload for attachment rendering to avoid duplicate thread API calls
  useEffect(() => {
    if (!selectedThreadId || !threadData?.emails?.length) return;
    setThreadAttachmentsMap((prev) => {
      if (prev[selectedThreadId]) return prev;
      return {
        ...prev,
        [selectedThreadId]: threadData.emails as EmailWithAttachments[],
      };
    });
  }, [selectedThreadId, threadData]);

  // Keyboard navigation
  const navigateEmails = useCallback(
    (direction: "up" | "down") => {
      if (flatEmailList.length === 0) return;

      const currentIndex = selectedEmailId
        ? flatEmailList.findIndex((e) => e.id === selectedEmailId)
        : -1;

      let newIndex: number;
      if (direction === "down") {
        newIndex =
          currentIndex < flatEmailList.length - 1
            ? currentIndex + 1
            : currentIndex;
      } else {
        newIndex = currentIndex > 0 ? currentIndex - 1 : 0;
      }

      if (newIndex !== currentIndex && flatEmailList[newIndex]) {
        const email = flatEmailList[newIndex];
        handleEmailClick(email);

        setTimeout(() => {
          const emailElement = document.querySelector(
            `[data-email-id="${email.id}"]`,
          );
          emailElement?.scrollIntoView({
            behavior: "instant",
            block: "nearest",
          });
        }, 0);
      }
    },
    [flatEmailList, selectedEmailId],
  );

  // Zone-based keyboard navigation
  // app-sidebar = mailboxes sidebar (Inbox, Flagged, etc.)
  // content = email list
  const { activeZone, setActiveZone } = useKeyboardNavigation();
  const mailboxesSidebarRef = useRef<HTMLDivElement>(null);
  const emailListRef = useRef<HTMLDivElement>(null);

  // Build list of navigable folder IDs for keyboard navigation (only "All Accounts" folders for simplicity)
  const folderIds = useMemo(() => FOLDER_LIST.map((f) => f.id), []);

  // Find current folder index
  const currentFolderIndex = useMemo(() => {
    // Only consider "All Accounts" selection (when selectedAccountIds is empty)
    if (selectedAccountIds.length > 0) return 0;
    return folderIds.indexOf(activeFolder);
  }, [folderIds, activeFolder, selectedAccountIds]);

  // Get compose/reply state to avoid stealing focus when modal is open
  const composeIsOpen = useEmailStore((s) => s.compose.isOpen);
  const inlineReplyIsOpen = useEmailStore((s) => s.inlineReply.isOpen);

  // Focus appropriate element when zone changes
  useEffect(() => {
    // Don't steal focus when search is open - user is typing in the search input
    if (isSearchOpen) return;
    // Don't steal focus when compose modal is open - user is typing an email
    if (composeIsOpen) return;
    // Don't steal focus when inline reply is open - user is typing a reply
    if (inlineReplyIsOpen) return;

    if (activeZone === "app-sidebar") {
      setTimeout(() => {
        mailboxesSidebarRef.current?.focus();
      }, 0);
    } else if (activeZone === "content") {
      setTimeout(() => {
        emailListRef.current?.focus();
      }, 0);
      // Auto-select first email if none selected
      if (!selectedEmailId && flatEmailList.length > 0) {
        setSelectedEmailId(flatEmailList[0].id);
      }
    }
  }, [
    activeZone,
    selectedEmailId,
    flatEmailList,
    setSelectedEmailId,
    isSearchOpen,
    composeIsOpen,
    inlineReplyIsOpen,
  ]);

  // Keyboard handler for mailboxes sidebar (Inbox, Flagged, etc.)
  const handleMailboxesKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "ArrowLeft") {
        e.preventDefault();
        setActiveZone("main-sidebar");
        return;
      }

      if (e.key === "ArrowRight") {
        e.preventDefault();
        setActiveZone("content");
        return;
      }

      if (activeZone !== "app-sidebar") return;

      if (e.key === "ArrowDown") {
        e.preventDefault();
        const nextIndex =
          currentFolderIndex < folderIds.length - 1
            ? currentFolderIndex + 1
            : 0;
        setActiveFolder(folderIds[nextIndex]);
        setSelectedAccounts([]); // Ensure we're in "All Accounts" mode
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        const prevIndex =
          currentFolderIndex > 0
            ? currentFolderIndex - 1
            : folderIds.length - 1;
        setActiveFolder(folderIds[prevIndex]);
        setSelectedAccounts([]); // Ensure we're in "All Accounts" mode
      }
    },
    [
      activeZone,
      setActiveZone,
      currentFolderIndex,
      folderIds,
      setActiveFolder,
      setSelectedAccounts,
    ],
  );

  // Keyboard handler for email list
  const handleEmailListKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "ArrowLeft") {
        e.preventDefault();
        setActiveZone("app-sidebar");
        return;
      }

      if (activeZone !== "content") return;

      if (e.key === "ArrowDown" || e.key === "j") {
        e.preventDefault();
        navigateEmails("down");
      } else if (e.key === "ArrowUp" || e.key === "k") {
        e.preventDefault();
        navigateEmails("up");
      }
    },
    [navigateEmails, setActiveZone, activeZone],
  );

  // Get folder counts for display (for "All Accounts" view)
  const getFolderCount = (folderId: EmailFolder): number | undefined => {
    if (folderId === "INBOX") return counts?.inbox_unread;
    if (folderId === "DRAFT") return counts?.drafts_count;
    return undefined;
  };

  // Get folder counts for a specific account
  const getAccountFolderCount = (
    accountId: string,
    folderId: EmailFolder,
  ): number | undefined => {
    if (folderId !== "DRAFT") return undefined;
    const accountStatus = accountsStatus.find(
      (a) => a.connectionId === accountId,
    );
    const accountCount = counts?.per_account?.find(
      (account) =>
        account.id === accountId || account.email === accountStatus?.email,
    );
    return accountCount?.drafts_count;
  };

  return (
    <div className="flex-1 flex h-full overflow-hidden">
      {/* Main content container - light bg with rounded corners */}
      <div className="flex-1 flex overflow-hidden bg-bg-mini-app">
        {/* Mailboxes sidebar */}
        <div
          ref={mailboxesSidebarRef}
          tabIndex={0}
          onKeyDown={handleMailboxesKeyDown}
          onFocus={() => setActiveZone("app-sidebar")}
          className={`w-[212px] shrink-0 flex flex-col overflow-hidden outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-primary ${SIDEBAR.bg} border-r border-black/5`}
        >
          {/* Header */}
          <div className="h-12 flex items-center justify-between pl-4 pr-2 shrink-0">
            <h2 className="text-base font-semibold text-text-body">Email</h2>
            <button
              onClick={openCompose}
              className="p-1 rounded bg-white border border-black/10 hover:border-black/20 text-text-secondary hover:text-text-body transition-colors focus-visible:ring-2 focus-visible:ring-brand-primary"
              title="Compose email"
              aria-label="Compose email"
            >
              <Icon
                icon={Pencil}
                size={16}
                aria-hidden="true"
              />
            </button>
          </div>

          <div className="flex-1 overflow-y-auto px-2 pt-0">
            {/* All Accounts section */}
            {(() => {
              const isAllExpanded = expandedAccounts.has("all");
              const toggleAllExpand = () => {
                setExpandedAccounts((prev) => {
                  const next = new Set(prev);
                  if (next.has("all")) {
                    next.delete("all");
                  } else {
                    next.add("all");
                  }
                  return next;
                });
              };

              return (
                <div className="space-y-0.5">
                  {/* All Accounts header - styled like folder row */}
                  <button
                    onClick={toggleAllExpand}
                    aria-expanded={isAllExpanded}
                    className="w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors group cursor-pointer text-text-secondary hover:text-text-body hover:bg-black/5 focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-primary"
                  >
                    {isAllExpanded ? (
                      <ChevronDownIcon
                        className="w-3.5 h-3.5 flex-shrink-0"
                        aria-hidden="true"
                      />
                    ) : (
                      <ChevronRightIcon
                        className="w-3.5 h-3.5 flex-shrink-0"
                        aria-hidden="true"
                      />
                    )}
                    <span className="flex-1 text-left">All Accounts</span>
                  </button>

                  {/* Folders under All Accounts */}
                  {isAllExpanded && (
                    <div className="space-y-0.5">
                      {FOLDER_LIST.map((folder) => {
                        const count = getFolderCount(folder.id);
                        const isActive =
                          activeFolder === folder.id &&
                          selectedAccountIds.length === 0;
                        const FolderIcon = folder.icon;

                        return (
                          <button
                            key={folder.id}
                            onClick={() => {
                              setActiveFolder(folder.id);
                              setSelectedAccounts([]);
                            }}
                            onMouseEnter={() =>
                              prefetchFolder(folder.id as RQEmailFolder, [])
                            }
                            className={`w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors cursor-pointer focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-primary ${
                              isActive
                                ? SIDEBAR.selected
                                : `${SIDEBAR.item} hover:bg-black/5`
                            }`}
                          >
                            <FolderIcon
                              className="w-4 h-4 shrink-0"
                              aria-hidden="true"
                            />
                            <span className="flex-1 text-left">
                              {folder.name}
                            </span>
                            {count !== undefined && count > 0 && (
                              <span className="text-xs font-bold text-text-body">
                                {count}
                              </span>
                            )}
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })()}

            {/* Individual account sections */}
            {accountsStatus.map((account) => {
              const isExpanded = expandedAccounts.has(account.connectionId);
              const toggleExpand = () => {
                setExpandedAccounts((prev) => {
                  const next = new Set(prev);
                  if (next.has(account.connectionId)) {
                    next.delete(account.connectionId);
                  } else {
                    next.add(account.connectionId);
                  }
                  return next;
                });
              };

              return (
                <div key={account.connectionId} className="mt-2 space-y-0.5">
                  {/* Account header - styled like folder row */}
                  <button
                    onClick={toggleExpand}
                    aria-expanded={isExpanded}
                    className="w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors group cursor-pointer text-text-secondary hover:text-text-body hover:bg-black/5 focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-primary"
                  >
                    {isExpanded ? (
                      <ChevronDownIcon
                        className="w-3.5 h-3.5 flex-shrink-0"
                        aria-hidden="true"
                      />
                    ) : (
                      <ChevronRightIcon
                        className="w-3.5 h-3.5 flex-shrink-0"
                        aria-hidden="true"
                      />
                    )}
                    <span className="flex-1 text-left truncate">
                      {account.email}
                    </span>
                  </button>

                  {/* Folders under this account */}
                  {isExpanded && (
                    <div className="space-y-0.5">
                      {FOLDER_LIST.map((folder) => {
                        const isActive =
                          activeFolder === folder.id &&
                          selectedAccountIds.length === 1 &&
                          selectedAccountIds[0] === account.connectionId;
                        const FolderIcon = folder.icon;
                        const folderCount = getAccountFolderCount(
                          account.connectionId,
                          folder.id,
                        );

                        return (
                          <button
                            key={folder.id}
                            onClick={() => {
                              setActiveFolder(folder.id);
                              setSelectedAccounts([account.connectionId]);
                            }}
                            onMouseEnter={() =>
                              prefetchFolder(folder.id as RQEmailFolder, [
                                account.connectionId,
                              ])
                            }
                            className={`w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors cursor-pointer focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-primary ${
                              isActive
                                ? SIDEBAR.selected
                                : `${SIDEBAR.item} hover:bg-black/5`
                            }`}
                          >
                            <FolderIcon
                              className="w-4 h-4 shrink-0"
                              aria-hidden="true"
                            />
                            <span className="flex-1 text-left">
                              {folder.name}
                            </span>
                            {folderCount !== undefined && folderCount > 0 && (
                              <span className="text-xs font-bold text-text-body">
                                {folderCount}
                              </span>
                            )}
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          <div className="p-2">
            <button
              onClick={handleSync}
              disabled={isSyncing}
              className="w-full flex items-center justify-center gap-2 px-3 py-1.5 text-[13px] text-text-secondary hover:text-text-body transition-colors rounded-lg hover:bg-bg-gray-dark/50"
            >
              <ArrowPathIcon
                className={`w-3.5 h-3.5 ${isSyncing ? "animate-spin" : ""}`}
              />
              {isSyncing ? "Syncing..." : "Sync Mail"}
            </button>
          </div>
        </div>

        {/* Email list + detail container */}
        <div className="flex-1 flex flex-col overflow-hidden">
          <div className="flex flex-1 bg-white overflow-hidden">
            {/* Email list */}
            <div
              ref={emailListRef}
              tabIndex={0}
              onKeyDown={handleEmailListKeyDown}
              onFocus={() => setActiveZone("content")}
              className="w-80 shrink-0 flex flex-col outline-none border-r border-border-gray"
            >
              <div className="h-12 shrink-0 flex items-center pl-4 pr-2 border-b border-border-gray">
                <AnimatePresence mode="wait">
                  {isSearchOpen ? (
                    <motion.div
                      key="search"
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      exit={{ opacity: 0 }}
                      transition={{ duration: 0.08 }}
                      className="flex-1 flex items-center gap-2"
                    >
                      <motion.div
                        className="relative flex-1"
                        initial={{ x: 8 }}
                        animate={{ x: 0 }}
                        exit={{ x: 8 }}
                        transition={{ duration: 0.08 }}
                      >
                        <MagnifyingGlassIcon className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-text-tertiary" />
                        <input
                          ref={searchInputRef}
                          type="text"
                          placeholder="Search emails..."
                          value={searchQuery}
                          onChange={(e) => handleSearchChange(e.target.value)}
                          onKeyDown={(e) => {
                            e.stopPropagation();
                            if (e.key === "Escape") {
                              setSearchQuery("");
                              clearSearch();
                              setIsSearchOpen(false);
                            }
                          }}
                          className="w-full pl-9 pr-3 py-1.5 text-sm bg-bg-mini-app rounded-lg focus:outline-none"
                          autoFocus
                        />
                      </motion.div>
                      {isSearching && (
                        <ArrowPathIcon className="w-3.5 h-3.5 text-text-tertiary animate-spin shrink-0" />
                      )}
                      <button
                        onClick={() => {
                          setSearchQuery("");
                          clearSearch();
                          setIsSearchOpen(false);
                        }}
                        className="p-2 text-text-body hover:bg-bg-gray-dark/50 rounded-lg transition-colors shrink-0"
                        title="Close search"
                      >
                        <XMarkIcon className="w-4.5 h-4.5" />
                      </button>
                    </motion.div>
                  ) : (
                    <motion.div
                      key="header"
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      exit={{ opacity: 0 }}
                      transition={{ duration: 0.05 }}
                      className="flex-1"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <h2 className="text-base font-semibold text-text-body">
                            {
                              FOLDER_LIST.find((f) => f.id === activeFolder)
                                ?.name
                            }
                            <span className="text-sm font-normal text-text-secondary ml-1.5">
                              - {filteredEmails.length}{" "}
                              {filteredEmails.length === 1
                                ? "message"
                                : "messages"}
                            </span>
                          </h2>
                        </div>
                        <button
                          onClick={() => setIsSearchOpen(true)}
                          className="p-2 text-text-body hover:bg-bg-gray-dark/50 rounded-lg transition-colors"
                          title="Search"
                        >
                          <MagnifyingGlassIcon className="w-4.5 h-4.5" />
                        </button>
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>

              {isSearchOpen &&
                searchQuery.trim() &&
                searchMeta &&
                !isSearching && (
                  <div className="px-4 py-1 text-[11px] text-text-tertiary border-b border-border-gray">
                    {searchMeta.local_count} local · {searchMeta.remote_count}{" "}
                    from provider
                    {searchMeta.has_provider_errors && (
                      <span className="text-yellow-500 ml-1">(partial)</span>
                    )}
                  </div>
                )}

              {/* Category filter buttons - only show for inbox when not searching */}
              {activeFolder === "INBOX" && !isSearchOpen && (
                <div className="px-4 py-2 flex gap-2">
                  {(["personal", "promotions", "social"] as const).map(
                    (category) => (
                      <button
                        key={category}
                        onClick={() => setCategoryFilter(category)}
                        className={`px-3 py-1 text-[13px] rounded-lg transition-colors ${
                          categoryFilter === category
                            ? "bg-black/8 text-text-body font-medium"
                            : "text-text-secondary hover:text-text-body hover:bg-black/5"
                        }`}
                      >
                        {category.charAt(0).toUpperCase() + category.slice(1)}
                      </button>
                    ),
                  )}
                </div>
              )}

              <div
                ref={scrollContainerRef}
                className="flex-1 overflow-y-auto [&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-track]:rounded-full [&::-webkit-scrollbar-thumb]:bg-black/20 [&::-webkit-scrollbar-thumb]:rounded-full"
              >
                {/* Only show skeleton if actually loading AND not restoring from cache AND no emails */}
                {isLoading && !isRestoring && emails.length === 0 ? (
                  <div className="animate-pulse">
                    {/* Skeleton email items */}
                    {[...Array(8)].map((_, i) => (
                      <div key={i} className="px-2 py-0.5">
                        <div className="px-3 py-2.5">
                          <div className="flex gap-2">
                            {/* Unread indicator placeholder */}
                            <div className="w-2 shrink-0" />
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between gap-2 mb-1">
                                <div className="h-4 bg-gray-200 rounded w-32" />
                                <div className="h-3 bg-gray-100 rounded w-12" />
                              </div>
                              <div className="h-4 bg-gray-200 rounded w-48 mb-1.5" />
                              <div className="h-3.5 bg-gray-100 rounded w-full" />
                            </div>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : error && emails.length === 0 ? (
                  <div className="p-4 text-center text-red-400">{error}</div>
                ) : emails.length === 0 ? (
                  <div className="p-8 text-center text-text-secondary">
                    <p className="text-sm">No messages</p>
                  </div>
                ) : filteredEmails.length === 0 && activeFolder === "INBOX" ? (
                  <div className="p-8 text-center text-text-secondary">
                    <p className="text-sm">No {categoryFilter} emails</p>
                  </div>
                ) : (
                  groupedEmails.map((group) => (
                    <div key={group.label}>
                      <div className="sticky top-0 z-10 bg-white px-4 py-1.5 text-[10px] font-label font-medium text-text-tertiary uppercase tracking-wide">
                        {group.label}
                      </div>
                      {group.emails.map((email) => (
                        <div key={email.id} className="px-2 py-0.5">
                          <button
                            data-email-id={email.id}
                            onClick={() => handleEmailClick(email)}
                            onMouseEnter={() => {
                              // Prefetch email details on hover for instant navigation
                              if (
                                !(
                                  "source" in email && email.source === "remote"
                                )
                              ) {
                                preloadEmail(email.id);
                              }
                            }}
                            className={`w-full text-left px-3 py-2.5 rounded-lg transition-all duration-150 outline-none focus:outline-none ${
                              selectedEmailId === email.id
                                ? activeZone === "content"
                                  ? "bg-brand-primary/5"
                                  : "bg-black/[0.03]"
                                : "hover:bg-black/[0.02]"
                            }`}
                          >
                            <div className="flex gap-2">
                              {/* Unread indicator */}
                              <div className="w-2 shrink-0 flex items-start pt-1.5">
                                {!email.is_read && (
                                  <div className="w-2 h-2 rounded-full bg-blue-600" />
                                )}
                              </div>

                              <div className="flex-1 min-w-0">
                                <div className="flex items-center justify-between gap-2 mb-0.5">
                                  <div className="flex items-center gap-1.5 min-w-0">
                                    <span
                                      className={`text-[14px] truncate ${
                                        !email.is_read
                                          ? "text-text-body font-bold"
                                          : "text-text-body font-medium"
                                      }`}
                                    >
                                      {email.from_name ||
                                        email.from_email?.split("@")[0] ||
                                        "Unknown"}
                                    </span>
                                    {email.threadCount > 1 && (
                                      <span className="text-[12px] text-text-secondary font-medium shrink-0">
                                        {email.threadCount}
                                      </span>
                                    )}
                                  </div>
                                  <div className="flex items-center gap-1.5 shrink-0">
                                    {"source" in email &&
                                      email.source === "remote" && (
                                        <CloudArrowDownIcon
                                          className="w-3 h-3 text-text-tertiary"
                                          title="From provider"
                                        />
                                      )}
                                    {email.has_attachments && (
                                      <PaperClipIcon className="w-3 h-3 text-text-tertiary" />
                                    )}
                                    <span className="text-[11px] text-text-tertiary">
                                      {formatDate(email.date)}
                                    </span>
                                  </div>
                                </div>

                                <p className="text-[13px] text-text-body line-clamp-1">
                                  {email.subject || "(No Subject)"}
                                </p>
                                <p className="text-[13px] mt-0.5 text-text-body opacity-60 line-clamp-1">
                                  {email.snippet
                                    ? decodeHtmlEntities(
                                        email.snippet
                                          .replace(/\s+/g, " ")
                                          .trim(),
                                      )
                                    : "\u00A0"}
                                </p>
                              </div>
                            </div>
                          </button>
                        </div>
                      ))}
                    </div>
                  ))
                )}
                {/* Infinite scroll trigger */}
                <div ref={loadMoreRef} className="h-1" />
                {/* Loading more indicator */}
                {isLoadingMore && (
                  <div className="py-4 text-center">
                    <ArrowPathIcon className="w-4.5 h-4.5 mx-auto animate-spin text-text-tertiary" />
                  </div>
                )}
              </div>
            </div>

            {/* Email detail */}
            <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
              {selectedEmail ? (
                <>
                  {/* Toolbar */}
                  <div className="h-12 flex items-center justify-between px-4 border-b border-border-gray">
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => openInlineReply(selectedEmail, "reply")}
                        className="p-2 text-text-body hover:bg-bg-gray-dark/50 rounded-lg transition-colors"
                        title="Reply"
                      >
                        <Icon icon={ArrowUpLeft} size={18} />
                      </button>
                      {(selectedEmail.to_emails?.length > 1 ||
                        (selectedEmail.cc_emails?.length ?? 0) > 0) && (
                        <button
                          onClick={() =>
                            openInlineReply(selectedEmail, "reply-all")
                          }
                          className="p-2 text-text-body hover:bg-bg-gray-dark/50 rounded-lg transition-colors"
                          title="Reply All"
                        >
                          <Icon icon={ArrowUpLeft} size={18} />
                        </button>
                      )}
                      <button
                        onClick={() => openInlineForward(selectedEmail)}
                        className="p-2 text-text-body hover:bg-bg-gray-dark/50 rounded-lg transition-colors"
                        title="Forward"
                      >
                        <Icon icon={ArrowUpRight} size={18} />
                      </button>
                      <div className="w-px h-5 bg-border-gray mx-1" />
                      {activeFolder === "TRASH" ? (
                        <button
                          onClick={() => handleRestore(selectedEmail)}
                          className="p-2 text-text-body hover:bg-bg-gray-dark/50 rounded-lg transition-colors"
                          title="Restore"
                        >
                          <ArrowUturnLeftIcon className="w-4.5 h-4.5" />
                        </button>
                      ) : (
                        <>
                          <button
                            onClick={() => handleArchive(selectedEmail)}
                            className="p-2 text-text-body hover:bg-bg-gray-dark/50 rounded-lg transition-colors"
                            title="Archive"
                          >
                            <ArchiveBoxIcon className="w-4.5 h-4.5" />
                          </button>
                          <button
                            onClick={() => handleDelete(selectedEmail)}
                            className="p-2 text-red-400 hover:bg-bg-gray-dark/50 rounded-lg transition-colors"
                            title="Delete"
                          >
                            <TrashIcon className="w-4.5 h-4.5" />
                          </button>
                        </>
                      )}
                    </div>
                    <div className="flex items-center gap-0.5">
                      <HeaderButtons
                        onSettingsClick={() =>
                          setShowSettingsDropdown((prev) => !prev)
                        }
                        settingsButtonRef={settingsButtonRef}
                      />
                    </div>
                  </div>

                  {/* Content area - relative for panels */}
                  <div className="flex-1 overflow-hidden relative">
                    {/* Subject header */}
                    <div className="h-full overflow-y-auto">
                      <div className="px-6 py-4">
                        <h1 className="text-xl font-semibold text-text-body">
                          {selectedEmail.subject || "(No Subject)"}
                        </h1>
                      </div>
                      <div className="mx-6 border-b border-border-gray" />

                      {/* Email thread + Inline Reply container */}
                      <div className="pb-32">
                        {/* Fetching remote email overlay */}
                        {fetchingRemoteId &&
                          fetchingRemoteId === selectedEmailId && (
                            <div className="absolute inset-0 z-20 flex items-center justify-center bg-white/80">
                              <div className="flex items-center gap-2 text-text-secondary">
                                <ArrowPathIcon className="w-5 h-5 animate-spin" />
                                <span className="text-sm">
                                  Fetching from provider...
                                </span>
                              </div>
                            </div>
                          )}
                        {/* Previous emails in thread (collapsed) */}
                        {selectedThreadEmails.length > 1 &&
                          selectedThreadEmails
                            .slice(0, -1)
                            .map((threadEmail) => {
                              const isExpanded = expandedThreadEmails.has(
                                threadEmail.id,
                              );
                              // Use cached details if available
                              const cachedEmail =
                                emailDetailsCache[threadEmail.id];
                              const emailToRender = cachedEmail || threadEmail;
                              return (
                                <div
                                  key={threadEmail.id}
                                  className="border-b border-border-gray"
                                >
                                  {/* Collapsed header - always visible */}
                                  <button
                                    onClick={() => {
                                      setExpandedThreadEmails((prev) => {
                                        const next = new Set(prev);
                                        if (next.has(threadEmail.id)) {
                                          next.delete(threadEmail.id);
                                        } else {
                                          next.add(threadEmail.id);
                                          // Fetch details if not cached
                                          if (
                                            !emailDetailsCache[threadEmail.id]
                                              ?.body_html &&
                                            !emailDetailsCache[threadEmail.id]
                                              ?.body_text
                                          ) {
                                            fetchEmailDetails(threadEmail.id);
                                          }
                                        }
                                        return next;
                                      });
                                    }}
                                    className="w-full text-left px-6 py-3 hover:bg-bg-gray transition-colors flex items-center gap-3"
                                  >
                                    <div
                                      className="w-8 h-8 rounded-full flex items-center justify-center text-white text-xs font-medium shrink-0"
                                      style={{
                                        backgroundColor: getAvatarColor(
                                          threadEmail.from_email,
                                        ),
                                      }}
                                    >
                                      {getInitials(
                                        threadEmail.from_name,
                                        threadEmail.from_email,
                                      )}
                                    </div>
                                    <div className="flex-1 min-w-0">
                                      <div className="flex items-center gap-2">
                                        <span className="text-[14px] font-medium text-text-body truncate">
                                          {threadEmail.from_name ||
                                            threadEmail.from_email ||
                                            "Unknown"}
                                        </span>
                                        <span className="text-[12px] text-text-tertiary shrink-0">
                                          {formatDate(threadEmail.date)}
                                        </span>
                                      </div>
                                      {!isExpanded && (
                                        <p className="text-[13px] text-text-secondary truncate mt-0.5">
                                          {threadEmail.snippet
                                            ? decodeHtmlEntities(
                                                stripQuotedFromSnippet(
                                                  threadEmail.snippet,
                                                ),
                                              )
                                            : ""}
                                        </p>
                                      )}
                                    </div>
                                    <ChevronDownIcon
                                      className={`w-4 h-4 text-text-tertiary shrink-0 transition-transform ${isExpanded ? "rotate-180" : ""}`}
                                    />
                                  </button>

                                  {/* Expanded content */}
                                  {isExpanded && (
                                    <div className="px-6 pb-4">
                                      <div className="text-[13px] text-text-secondary mb-3">
                                        To:{" "}
                                        {emailToRender.to_emails?.join(", ") ||
                                          accountsStatus
                                            .map((a) => a.email)
                                            .join(", ") ||
                                          "me"}
                                      </div>
                                      {loadingDetailsId === threadEmail.id ? (
                                        <div className="flex items-center justify-center py-4">
                                          <ArrowPathIcon className="w-5 h-5 text-text-tertiary animate-spin" />
                                        </div>
                                      ) : emailToRender.body_html ? (
                                        <div
                                          className="prose prose-sm max-w-none text-[14px] text-text-body"
                                          dangerouslySetInnerHTML={{
                                            __html: sanitizeEmailHtml(
                                              emailToRender.body_html,
                                            ),
                                          }}
                                        />
                                      ) : emailToRender.body_text ||
                                        emailToRender.snippet ? (
                                        <div
                                          className="prose prose-sm max-w-none text-[14px] text-text-body"
                                          dangerouslySetInnerHTML={{
                                            __html: plainTextToHtml(
                                              emailToRender.body_text ||
                                                emailToRender.snippet ||
                                                "",
                                            ),
                                          }}
                                        />
                                      ) : (
                                        <div className="text-[14px] text-text-secondary">
                                          No content
                                        </div>
                                      )}
                                      {(() => {
                                        const threadEmails =
                                          threadAttachmentsMap[
                                            selectedEmail?.thread_id || ""
                                          ];
                                        const emailWithAttachments =
                                          threadEmails?.find(
                                            (e) => e.id === threadEmail.id,
                                          );
                                        if (
                                          emailWithAttachments?.attachments
                                            ?.length
                                        ) {
                                          return (
                                            <AttachmentList
                                              emailId={threadEmail.id}
                                              attachments={
                                                emailWithAttachments.attachments
                                              }
                                              compact
                                              onImageClick={setLightboxImageUrl}
                                            />
                                          );
                                        }
                                        return null;
                                      })()}
                                    </div>
                                  )}
                                </div>
                              );
                            })}

                        {/* Current/Latest email - header + body */}
                        {latestThreadEmail && (
                          <div>
                            {/* Latest email header */}
                            <div className="px-6 py-4 flex items-start gap-3">
                              <div
                                className="w-8 h-8 rounded-full flex items-center justify-center text-white text-xs font-medium shrink-0"
                                style={{
                                  backgroundColor: getAvatarColor(
                                    latestThreadEmail.from_email,
                                  ),
                                }}
                              >
                                {getInitials(
                                  latestThreadEmail.from_name,
                                  latestThreadEmail.from_email,
                                )}
                              </div>
                              <div className="flex-1 min-w-0">
                                <div className="flex items-baseline gap-2">
                                  <span className="text-[15px] font-medium text-text-body">
                                    {latestThreadEmail.from_name ||
                                      latestThreadEmail.from_email}
                                  </span>
                                  {latestThreadEmail.from_name && (
                                    <span className="text-[13px] text-text-secondary">
                                      &lt;{latestThreadEmail.from_email}&gt;
                                    </span>
                                  )}
                                </div>
                                <div className="flex items-center gap-2 text-[13px] text-text-secondary mt-0.5">
                                  <span>
                                    To:{" "}
                                    {latestThreadEmail.to_emails?.join(", ") ||
                                      accountsStatus
                                        .map((a) => a.email)
                                        .join(", ") ||
                                      "me"}
                                  </span>
                                  <span>•</span>
                                  <span>
                                    {new Date(
                                      latestThreadEmail.date,
                                    ).toLocaleString([], {
                                      weekday: "short",
                                      month: "short",
                                      day: "numeric",
                                      year: "numeric",
                                      hour: "numeric",
                                      minute: "2-digit",
                                    })}
                                  </span>
                                </div>
                              </div>
                              {latestThreadEmail.is_starred && (
                                <StarIconSolid className="w-5 h-5 text-yellow-400 shrink-0" />
                              )}
                            </div>

                            {/* Latest email body */}
                            {loadingDetailsId === latestThreadEmail.id ? (
                              <div className="flex items-center justify-center py-12">
                                <ArrowPathIcon className="w-6 h-6 text-text-tertiary animate-spin" />
                              </div>
                            ) : latestEmailHtmlContent ? (
                              <>
                                <iframe
                                  srcDoc={latestEmailHtmlContent}
                                  className="w-full border-0"
                                  sandbox="allow-same-origin allow-popups"
                                  title="Email content"
                                  style={{ height: "100px" }}
                                  onLoad={(e) => {
                                    // Auto-resize iframe to content height
                                    const iframe =
                                      e.target as HTMLIFrameElement;
                                    if (iframe.contentDocument) {
                                      const contentHeight =
                                        iframe.contentDocument.body
                                          .scrollHeight;
                                      iframe.style.height =
                                        Math.max(contentHeight, 50) + "px";
                                    }
                                  }}
                                />
                                {/* Ellipsis button to toggle quoted content */}
                                {latestEmailQuotedContent && (
                                  <div className="px-6 py-2">
                                    <button
                                      onClick={() =>
                                        setShowQuotedContent(!showQuotedContent)
                                      }
                                      className="p-1.5 rounded-md border border-border-gray hover:bg-bg-gray-light transition-colors"
                                      title={
                                        showQuotedContent
                                          ? "Hide quoted content"
                                          : "Show quoted content"
                                      }
                                    >
                                      <EllipsisHorizontalIcon className="w-4 h-4 stroke-2 text-text-secondary" />
                                    </button>
                                  </div>
                                )}
                                {/* Quoted content iframe */}
                                {latestEmailQuotedContent &&
                                  showQuotedContent &&
                                  quotedContentHtml && (
                                    <iframe
                                      srcDoc={quotedContentHtml}
                                      className="w-full border-0"
                                      sandbox="allow-same-origin allow-popups"
                                      title="Quoted content"
                                      style={{ height: "100px" }}
                                      onLoad={(e) => {
                                        const iframe =
                                          e.target as HTMLIFrameElement;
                                        if (iframe.contentDocument) {
                                          const contentHeight =
                                            iframe.contentDocument.body
                                              .scrollHeight;
                                          iframe.style.height =
                                            Math.max(contentHeight, 50) + "px";
                                        }
                                      }}
                                    />
                                  )}
                              </>
                            ) : (
                              <div className="px-6 py-4 text-[14px] text-text-secondary">
                                No content
                              </div>
                            )}
                            {(() => {
                              const threadEmails =
                                threadAttachmentsMap[
                                  selectedEmail?.thread_id || ""
                                ];
                              const emailWithAttachments = threadEmails?.find(
                                (e) => e.id === latestThreadEmail?.id,
                              );
                              if (emailWithAttachments?.attachments?.length) {
                                return (
                                  <div className="px-6 py-4">
                                    <AttachmentList
                                      emailId={latestThreadEmail.id}
                                      attachments={
                                        emailWithAttachments.attachments
                                      }
                                      onImageClick={setLightboxImageUrl}
                                    />
                                  </div>
                                );
                              }
                              return null;
                            })()}
                          </div>
                        )}

                        {/* Inline Reply Composer - appears below latest email like Gmail */}
                        {inlineReply.isOpen &&
                          inlineReply.draft?.replyToEmailId ===
                            selectedEmailId && (
                            <div ref={replyComposerRef} className="px-6 py-4">
                              <InlineReplyComposer />
                            </div>
                          )}
                      </div>
                    </div>
                  </div>
                </>
              ) : (
                <>
                  {/* Header with buttons */}
                  <div className="h-12 flex items-center justify-end pl-6 pr-3 border-b border-border-gray">
                    <HeaderButtons
                      onSettingsClick={() =>
                        setShowSettingsDropdown((prev) => !prev)
                      }
                      settingsButtonRef={settingsButtonRef}
                    />
                  </div>
                  {/* Content area - relative for panels */}
                  <div className="flex-1 overflow-hidden relative">
                    <div className="h-full flex items-center justify-center bg-white">
                      <div className="text-center">
                        <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-bg-gray-dark flex items-center justify-center">
                          <PaperAirplaneIcon className="w-8 h-8 text-text-secondary" />
                        </div>
                        <p className="text-lg font-medium text-text-body">
                          No Message Selected
                        </p>
                        <p className="text-sm mt-1 text-text-secondary">
                          Select a message to read
                        </p>
                      </div>
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Settings Dropdown */}
        <EmailSettingsDropdown
          isOpen={showSettingsDropdown}
          onClose={() => setShowSettingsDropdown(false)}
          trigger={settingsButtonRef}
          onArchive={
            selectedEmail && activeFolder !== "TRASH"
              ? () => handleArchive(selectedEmail)
              : undefined
          }
          onDelete={
            selectedEmail && activeFolder !== "TRASH"
              ? () => handleDelete(selectedEmail)
              : undefined
          }
          onRestore={
            selectedEmail && activeFolder === "TRASH"
              ? () => handleRestore(selectedEmail)
              : undefined
          }
        />

        {/* Compose Email Modal */}
        <ComposeEmail />

        {/* Image Lightbox */}
        {lightboxImageUrl &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/80"
              onClick={() => setLightboxImageUrl(null)}
            >
              <button
                onClick={() => setLightboxImageUrl(null)}
                className="absolute top-4 right-4 p-2 text-white/80 hover:text-white transition-colors"
                aria-label="Close"
              >
                <XMarkIcon className="w-6 h-6" />
              </button>
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
