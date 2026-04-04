import { useState, memo } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { motion } from 'motion/react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import {
  DocumentDuplicateIcon,
  DocumentTextIcon,
  CheckIcon,
  CalendarIcon,
  EnvelopeIcon,
  CheckCircleIcon,
  ClockIcon,
  ChevronDownIcon,
  ChevronRightIcon,
  PlusIcon,
  XMarkIcon,
  ArrowTopRightOnSquareIcon,
} from '@heroicons/react/24/outline';
import { useNavigate } from 'react-router-dom';
import type { Components } from 'react-markdown';
import type { ContentPart, Source } from '../../api/client';
import { executeAction } from '../../api/client';
import ChatAttachmentImage from './ChatAttachmentImage';
import StreamingText from './StreamingText';
import { getAccountPalette } from '../../utils/accountColors';
import { toast } from 'sonner';
import { useCalendarStore } from '../../stores/calendarStore';
import { useFilesStore } from '../../stores/filesStore';
import { projectKeys } from '../../hooks/queries/keys';

// ============================================================================
// Code Block (shared with ChatMessage)
// ============================================================================

function CodeBlock({ language, children }: { language: string | undefined; children: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(children);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group my-4 rounded-xl overflow-hidden bg-[#1e1e1e]">
      <div className="flex items-center justify-between px-4 py-2 bg-[#2d2d2d] text-text-tertiary text-xs">
        <span className="font-mono">{language || 'text'}</span>
        <button
          onClick={handleCopy}
          className="flex items-center gap-1.5 hover:text-text-light transition-colors"
        >
          {copied ? (
            <>
              <CheckIcon className="w-3.5 h-3.5 stroke-2" />
              <span>Copied!</span>
            </>
          ) : (
            <>
              <DocumentDuplicateIcon className="w-3.5 h-3.5" />
              <span>Copy code</span>
            </>
          )}
        </button>
      </div>
      <SyntaxHighlighter
        language={language || 'text'}
        style={oneDark}
        customStyle={{
          margin: 0,
          padding: '16px',
          background: '#1e1e1e',
          fontSize: '13px',
          lineHeight: '1.5',
        }}
        codeTagProps={{ style: { fontFamily: 'var(--font-mono)' } }}
      >
        {children}
      </SyntaxHighlighter>
    </div>
  );
}

// ============================================================================
// Markdown renderer config (shared)
// ============================================================================

const markdownComponents: Components = {
  code({ className, children, ...props }) {
    const match = /language-(\w+)/.exec(className || '');
    const codeString = String(children).replace(/\n$/, '');
    const isInline = !match && !codeString.includes('\n');

    if (isInline) {
      return (
        <code className="bg-[#EAEAEA] px-1.5 py-0.5 rounded text-sm font-mono text-text-body" {...props}>
          {children}
        </code>
      );
    }

    return <CodeBlock language={match?.[1]}>{codeString}</CodeBlock>;
  },
  p({ children }) {
    return <p className="mb-4 last:mb-0 leading-7">{children}</p>;
  },
  ul({ children }) {
    return <ul className="mb-4 last:mb-0 pl-6 list-disc space-y-2">{children}</ul>;
  },
  ol({ children }) {
    return <ol className="mb-4 last:mb-0 pl-6 list-decimal space-y-2">{children}</ol>;
  },
  li({ children }) {
    return <li className="leading-7">{children}</li>;
  },
  h1({ children }) {
    return <h1 className="text-2xl font-semibold mb-4 mt-6 first:mt-0">{children}</h1>;
  },
  h2({ children }) {
    return <h2 className="text-xl font-semibold mb-3 mt-5 first:mt-0">{children}</h2>;
  },
  h3({ children }) {
    return <h3 className="text-lg font-semibold mb-2 mt-4 first:mt-0">{children}</h3>;
  },
  blockquote({ children }) {
    return (
      <blockquote className="border-l-4 border-border-gray pl-4 my-4 text-text-secondary italic">
        {children}
      </blockquote>
    );
  },
  a({ href, children }) {
    return (
      <a href={href} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
        {children}
      </a>
    );
  },
  table({ children }) {
    return (
      <div className="overflow-x-auto my-4">
        <table className="min-w-full border-collapse border border-border-gray">{children}</table>
      </div>
    );
  },
  th({ children }) {
    return <th className="border border-border-gray bg-bg-gray px-4 py-2 text-left font-semibold">{children}</th>;
  },
  td({ children }) {
    return <td className="border border-border-gray px-4 py-2">{children}</td>;
  },
  hr() {
    return <hr className="my-6 border-border-light" />;
  },
};

// ============================================================================
// Display Card Helpers
// ============================================================================

const AVATAR_COLORS = [
  'bg-blue-600', 'bg-violet-600', 'bg-emerald-600', 'bg-amber-600',
  'bg-rose-600', 'bg-cyan-600', 'bg-indigo-600', 'bg-teal-600',
  'bg-pink-600', 'bg-orange-600',
];

function getInitials(name: string): string {
  if (!name) return '?';
  const cleaned = name.replace(/[<>@.]/g, ' ').trim();
  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  return (parts[0]?.[0] || '?').toUpperCase();
}

function getAvatarColor(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash);
  return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length];
}

function cleanFromName(raw: string): string {
  const match = raw.match(/^(.+?)\s*<[^>]+>$/);
  return match ? match[1].trim() : raw;
}

// ============================================================================
// Display Card Components
// ============================================================================

function CalendarEventCard({ item }: { item: Record<string, unknown> }) {
  const title = (item.title || item.summary || 'Untitled') as string;
  const startTime = item.start_time as string | undefined;
  const endTime = item.end_time as string | undefined;
  const location = item.location as string | undefined;
  const isAllDay = item.is_all_day as boolean | undefined;

  const formatTime = (iso: string) => {
    try {
      return new Date(iso).toLocaleString(undefined, {
        weekday: 'short', month: 'short', day: 'numeric',
        hour: 'numeric', minute: '2-digit',
      });
    } catch { return iso; }
  };

  const formatTimeOnly = (iso: string) => {
    try {
      return new Date(iso).toLocaleTimeString(undefined, {
        hour: 'numeric', minute: '2-digit',
      });
    } catch { return iso; }
  };

  const timeDisplay = isAllDay
    ? 'All day'
    : startTime
      ? endTime
        ? `${formatTime(startTime)} – ${formatTimeOnly(endTime)}`
        : formatTime(startTime)
      : undefined;

  return (
    <div className="w-[340px] max-w-full flex items-center gap-3 px-3.5 py-2.5 rounded-xl bg-white border border-border-light shadow-[0_1px_3px_rgba(0,0,0,0.04)] hover:shadow-[0_2px_8px_rgba(0,0,0,0.06)] transition-shadow">
      <div className="w-1 self-stretch rounded-full bg-blue-500 shrink-0" />
      <div className="min-w-0 flex-1">
        <p className="text-[13px] font-medium text-text-body leading-snug truncate">{title}</p>
        {timeDisplay && (
          <p className="text-[11px] text-text-secondary mt-0.5">{timeDisplay}</p>
        )}
        {location && <p className="text-[11px] text-text-tertiary mt-0.5 truncate">{location}</p>}
      </div>
    </div>
  );
}

function EmailCard({ item }: { item: Record<string, unknown> }) {
  const navigate = useNavigate();
  const subject = (item.subject || 'No subject') as string;
  const rawFrom = (item.from_name || item.from_email || item.from || '') as string;
  const from = cleanFromName(rawFrom);
  const snippet = (item.snippet || '') as string;
  const emailId = (item.id as string) || null;
  const threadId = (item.thread_id as string) || null;

  const handleClick = () => {
    const params = new URLSearchParams();
    if (emailId) params.set('open', emailId);
    if (threadId) params.set('thread', threadId);
    navigate(params.toString() ? `/email?${params}` : '/email');
  };

  return (
    <div onClick={handleClick} className="w-[340px] max-w-full flex items-center gap-3 p-3 rounded-xl bg-white border border-border-light shadow-sm hover:shadow-md transition-shadow cursor-pointer">
      <div className={`size-8 rounded-full ${getAvatarColor(from)} flex items-center justify-center shrink-0`}>
        <span className="text-[11px] font-semibold text-white leading-none">{getInitials(from)}</span>
      </div>
      <div className="grow min-w-0 text-left">
        <div className="text-sm font-medium text-text-body truncate">{subject}</div>
        <div className="text-xs text-text-secondary truncate mt-0.5">
          {from}{from && snippet ? ` · ${snippet}` : snippet || ''}
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// Part Renderers
// ============================================================================

function TextPartRenderer({ content, isStreaming }: { content: string; isStreaming?: boolean }) {
  return <StreamingText content={content} isStreaming={isStreaming ?? false} />;
}

function DisplayPartRenderer({ part }: { part: ContentPart }) {
  const displayType = part.data.display_type as string;
  const items = (part.data.items || []) as Record<string, unknown>[];
  const totalCount = part.data.total_count as number | undefined;

  if (!items.length) return null;

  const renderCard = (item: Record<string, unknown>, i: number) => {
    switch (displayType) {
      case 'calendar_events': return <CalendarEventCard key={i} item={item} />;
      case 'emails': return <EmailCard key={i} item={item} />;
      default: return null;
    }
  };

  const label: Record<string, string> = {
    calendar_events: 'Events',
    emails: 'Emails',
  };

  return (
    <div className="mt-6 mb-6">
      <div className="space-y-2">
        {items.map((item, i) => (
          <motion.div
            key={i}
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{
              duration: 0.35,
              delay: i * 0.1,
              ease: [0.25, 0.1, 0.25, 1],
            }}
          >
            {renderCard(item, i)}
          </motion.div>
        ))}
      </div>
      {totalCount != null && totalCount > items.length && (
        <motion.p
          className="text-xs text-text-tertiary mt-2 pl-1"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.3, delay: items.length * 0.1 + 0.1 }}
        >
          +{totalCount - items.length} more {label[displayType] || 'items'}
        </motion.p>
      )}
    </div>
  );
}

function ActionPartRenderer({ part, messageId }: { part: ContentPart; messageId?: string }) {
  const queryClient = useQueryClient();
  const action = part.data.action as string;
  const initialStatus = part.data.status as string;
  const description = part.data.description as string;
  const actionData = (part.data.data || part.data) as Record<string, unknown>;
  const [status, setStatus] = useState(initialStatus);
  const [isExecuting, setIsExecuting] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [resultData, setResultData] = useState<Record<string, unknown> | null>(
    (part.data.result as Record<string, unknown>) || null
  );

  const isExecuted = status === 'executed' || status === 'confirmed';
  const isError = status === 'error';

  const handleConfirm = async () => {
    if (!messageId || isExecuted || isExecuting) return;
    setIsExecuting(true);
    setErrorMsg(null);
    try {
      const res = await executeAction(messageId, part.id);
      setStatus('executed');
      if (res.result) setResultData(res.result);
      // Refresh relevant stores after successful execution
      if (action === 'create_calendar_event' || action === 'update_calendar_event' || action === 'delete_calendar_event') {
        useCalendarStore.getState().fetchEvents();
      }
      if (action === 'create_document') {
        useFilesStore.getState().fetchDocuments();
      }
      if (action === 'create_project_issue') {
        await queryClient.invalidateQueries({ queryKey: projectKeys.all });
      }
      if (action === 'create_calendar_event') {
        const title = (actionData.summary || description || 'Event') as string;
        if (res.result?.sync_error) {
          toast.warning(`"${title}" saved locally but not synced to your calendar provider`);
        } else {
          toast.success(`"${title}" added to calendar`);
        }
      }
    } catch (err) {
      console.error('Failed to execute action:', err);
      setStatus('error');
      setErrorMsg(err instanceof Error ? err.message : 'Action failed');
    } finally {
      setIsExecuting(false);
    }
  };

  // Calendar event action card
  if (action === 'create_calendar_event') {
    const summary = (actionData.summary || '') as string;
    const startTime = actionData.start_time as string | undefined;
    const endTime = actionData.end_time as string | undefined;
    const eventDescription = actionData.description as string | undefined;

    const formatTime = (iso: string) => {
      try {
        return new Date(iso).toLocaleString(undefined, {
          weekday: 'short', month: 'short', day: 'numeric',
          hour: 'numeric', minute: '2-digit',
        });
      } catch { return iso; }
    };

    const accountEmail = (resultData?.account_email || actionData.account_email) as string | undefined;
    const palette = getAccountPalette(accountEmail);

    return (
      <div
        className="my-3 w-[340px] max-w-full rounded-xl border overflow-hidden transition-colors duration-300"
        style={{
          backgroundColor: isExecuted ? palette.bg : 'white',
          borderColor: isExecuted ? palette.accent + '30' : 'var(--color-border-light)',
          boxShadow: isExecuted ? 'none' : '0 1px 3px rgba(0,0,0,0.04)',
        }}
      >
        <div className="flex items-center gap-3 px-3.5 py-2.5">
          <div
            className="w-1 self-stretch rounded-full shrink-0 transition-colors duration-300"
            style={{ backgroundColor: isExecuted ? palette.accent : '#d1d5db' }}
          />
          <div className="min-w-0 flex-1">
            <p
              className="text-[13px] font-medium leading-snug transition-colors duration-300"
              style={{ color: isExecuted ? palette.title : undefined }}
            >
              {summary || description}
            </p>
            {startTime && (
              <p
                className="text-[11px] mt-0.5 transition-colors duration-300"
                style={{ color: isExecuted ? palette.time : undefined }}
              >
                {formatTime(startTime)}
                {endTime && ` – ${new Date(endTime).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })}`}
              </p>
            )}
            {eventDescription && (
              <p
                className="text-[11px] mt-0.5 line-clamp-2 transition-colors duration-300"
                style={{ color: isExecuted ? palette.time : undefined }}
              >
                {eventDescription}
              </p>
            )}
            {errorMsg && <p className="text-[11px] text-red-500 mt-1">{errorMsg}</p>}
          </div>
        </div>
        <div
          className="grid transition-[grid-template-rows] duration-300 ease-in-out"
          style={{ gridTemplateRows: isExecuted ? '0fr' : '1fr' }}
        >
          <div className="overflow-hidden">
            <div className="mx-3.5 border-t border-border-light" />
            <div className="px-3.5 py-2">
              {isError ? (
                <button
                  onClick={handleConfirm}
                  disabled={isExecuting}
                  className="text-xs font-medium text-red-500 hover:text-red-600 transition-colors disabled:opacity-50"
                >
                  Retry
                </button>
              ) : (
                <button
                  onClick={handleConfirm}
                  disabled={isExecuting}
                  className="flex items-center gap-1 text-xs font-medium text-blue-600 hover:text-blue-700 active:scale-[0.98] transition-all disabled:opacity-50"
                >
                  {isExecuting ? (
                    <span className="w-3 h-3 border-2 border-blue-300 border-t-blue-600 rounded-full animate-spin" />
                  ) : (
                    <PlusIcon className="w-3.5 h-3.5 stroke-[2.5]" />
                  )}
                  Add to calendar
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Email action card
  if (action === 'send_email') {
    const to = (actionData.to || '') as string;
    const subject = (actionData.subject || '') as string;
    const body = (actionData.body || '') as string;

    return (
      <div className="my-3 rounded-xl border border-border-light overflow-hidden">
        <div className="flex items-start gap-3 px-4 py-3 bg-[#FCFCFC]">
          <EnvelopeIcon className="w-5 h-5 mt-0.5 text-text-tertiary shrink-0" />
          <div className="min-w-0 flex-1">
            {to && <p className="text-xs text-text-secondary">To: {to}</p>}
            <p className="text-sm font-medium text-text-body mt-0.5">{subject || description}</p>
            {body && <p className="text-xs text-text-tertiary mt-1 line-clamp-3 italic">{body}</p>}
          </div>
          {isExecuted ? (
            <span className="flex items-center gap-1 text-xs text-green-600 font-medium shrink-0">
              <CheckCircleIcon className="w-4 h-4" />
              Sent
            </span>
          ) : isError ? (
            <button
              onClick={handleConfirm}
              disabled={isExecuting}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-red-500 text-white text-xs font-medium hover:bg-red-600 transition-colors disabled:opacity-50 shrink-0"
            >
              Retry
            </button>
          ) : (
            <button
              onClick={handleConfirm}
              disabled={isExecuting}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-text-body text-white text-xs font-medium hover:bg-black/70 transition-colors disabled:opacity-50 shrink-0"
            >
              {isExecuting ? (
                <span className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              ) : null}
              Send
            </button>
          )}
        </div>
        {errorMsg && <p className="px-4 pb-2 text-xs text-red-500">{errorMsg}</p>}
      </div>
    );
  }

  // Document action card
  if (action === 'create_document') {
    const title = (actionData.title || '') as string;
    const content = (actionData.content || '') as string;
    const docId = resultData?.document_id as string | undefined;
    const wsId = (resultData?.workspace_id || actionData.workspace_id) as string | undefined;

    return (
      <div className="my-3 rounded-xl border border-border-light overflow-hidden">
        <div className="flex items-start gap-3 px-4 py-3 bg-[#F9F5FF]/40">
          <DocumentTextIcon className="w-5 h-5 mt-0.5 text-violet-500 shrink-0" />
          <div className="min-w-0 flex-1">
            <p className="text-sm font-medium text-text-body">{title || description}</p>
            {content && !isExecuted && (
              <div className="mt-2 max-h-[200px] overflow-y-auto rounded-lg bg-white/60 border border-border-light px-3 py-2">
                <div className="prose prose-sm prose-gray max-w-none text-xs text-text-secondary">
                  <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
                    {content}
                  </ReactMarkdown>
                </div>
              </div>
            )}
            {isExecuted && docId && wsId && (
              <a
                href={`/workspace/${wsId}/files/${docId}`}
                className="inline-flex items-center gap-1 mt-1.5 text-xs text-violet-600 hover:text-violet-700 hover:underline transition-colors"
              >
                Open document
                <ArrowTopRightOnSquareIcon className="w-3 h-3" />
              </a>
            )}
          </div>
          {isExecuted ? (
            <span className="flex items-center gap-1 text-xs text-green-600 font-medium shrink-0">
              <CheckCircleIcon className="w-4 h-4" />
              Created
            </span>
          ) : isError ? (
            <button
              onClick={handleConfirm}
              disabled={isExecuting}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-red-500 text-white text-xs font-medium hover:bg-red-600 transition-colors disabled:opacity-50 shrink-0"
            >
              Retry
            </button>
          ) : (
            <button
              onClick={handleConfirm}
              disabled={isExecuting}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-violet-500 text-white text-xs font-medium hover:bg-violet-600 transition-colors disabled:opacity-50 shrink-0"
            >
              {isExecuting ? (
                <span className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              ) : (
                <PlusIcon className="w-3.5 h-3.5 stroke-2" />
              )}
              Create
            </button>
          )}
        </div>
        {errorMsg && <p className="px-4 pb-2 text-xs text-red-500">{errorMsg}</p>}
      </div>
    );
  }

  // Fallback for unknown action types
  return (
    <div className="my-3 flex items-center gap-2.5 px-3 py-2.5 rounded-lg bg-bg-gray/60 border border-border-light">
      {isExecuted ? (
        <CheckCircleIcon className="w-4 h-4 text-green-600 shrink-0" />
      ) : (
        <ClockIcon className="w-4 h-4 text-blue-500 shrink-0" />
      )}
      <div className="min-w-0 flex-1">
        <p className="text-sm text-text-body">{description || action}</p>
      </div>
      {!isExecuted && (
        <button
          onClick={handleConfirm}
          disabled={isExecuting}
          className="text-xs text-blue-600 font-medium hover:underline shrink-0 disabled:opacity-50"
        >
          {isExecuting ? 'Confirming...' : 'Confirm'}
        </button>
      )}
      {isExecuted && (
        <span className="text-[10px] font-medium px-1.5 py-0.5 rounded-full bg-green-100 text-green-700 shrink-0">
          Done
        </span>
      )}
    </div>
  );
}

// Human-readable tool labels
const TOOL_LABELS: Record<string, string> = {
  semantic_search: 'Semantic search',
  search_messages: 'Searched messages',
  list_project_boards: 'Checked project boards',
  get_project_board: 'Checked project board',
  get_project_issue: 'Checked project issue',
  get_email_thread: 'Fetched email thread',
  get_calendar_events: 'Checked calendar',
  get_emails: 'Checked emails',
  create_calendar_event: 'Creating event',
  send_email: 'Drafting email',
  create_document: 'Creating document',
  create_project_issue: 'Drafting project issue',
  update_document: 'Updating document',
  web_search: 'Searched the web',
};

function getSmartSearchTypesLabel(args?: Record<string, unknown>): string {
  const types = String(args?.types || 'emails,calendar,documents');
  const typeLabels: Record<string, string> = {
    emails: 'emails',
    calendar: 'calendar',
    documents: 'notes',
  };
  return types.split(',').map(t => typeLabels[t.trim()] || t.trim()).join(', ');
}

function getToolLabel(name: string, phase: string, args?: Record<string, unknown>): string {
  // Smart search: show what types are being searched
  if (name === 'smart_search') {
    const typesLabel = getSmartSearchTypesLabel(args);
    return phase === 'running' ? `Searching ${typesLabel}` : `Searched ${typesLabel}`;
  }

  if (phase === 'running') {
    // Use present tense for running
    const runningLabels: Record<string, string> = {
      semantic_search: 'Semantic search',
      search_messages: 'Searching messages',
      list_project_boards: 'Checking project boards',
      get_project_board: 'Checking project board',
      get_project_issue: 'Checking project issue',
      get_email_thread: 'Fetching email thread',
      get_calendar_events: 'Checking calendar',
      get_emails: 'Checking emails',
      web_search: 'Searching the web',
    };
    return runningLabels[name] || name.replace(/_/g, ' ').replace(/^\w/, c => c.toUpperCase()) + '...';
  }
  return TOOL_LABELS[name] || name.replace(/_/g, ' ').replace(/^\w/, c => c.toUpperCase());
}

function ToolCallPartRenderer({ part }: { part: ContentPart }) {
  const name = part.data.name as string;
  const phase = (part.data.phase || 'done') as string;
  const status = part.data.status as string | undefined;
  const durationMs = part.data.duration_ms as number | undefined;
  const args = part.data.args as Record<string, unknown> | undefined;

  // For smart_search, the types are already shown in the label — skip the raw query
  const query = name === 'smart_search' ? undefined : (args?.query || args?.search);

  return (
    <div className="flex items-center gap-2 py-1.5 my-0.5">
      {/* Status indicator */}
      {phase === 'running' ? (
        <span className="w-3.5 h-3.5 flex items-center justify-center shrink-0">
          <span className="w-2 h-2 rounded-full bg-text-body animate-thinking-dot" />
        </span>
      ) : status === 'error' ? (
        <XMarkIcon className="w-3.5 h-3.5 text-red-500 shrink-0 stroke-2" />
      ) : (
        <CheckIcon className="w-3.5 h-3.5 text-text-tertiary shrink-0 stroke-2" />
      )}

      {/* Tool label */}
      <span className={`text-sm ${phase === 'running' ? 'text-text-body' : 'text-text-tertiary'}`}>
        {getToolLabel(name, phase, args)}
      </span>

      {/* Query inline */}
      {typeof query === 'string' && query && (
        <span className="text-sm text-text-tertiary truncate max-w-[200px]">
          &ldquo;{query.slice(0, 40)}&rdquo;
        </span>
      )}

      {/* Duration */}
      {phase === 'done' && durationMs != null && (
        <span className="text-xs text-text-tertiary shrink-0">
          {durationMs < 1000 ? `${durationMs}ms` : `${(durationMs / 1000).toFixed(1)}s`}
        </span>
      )}
    </div>
  );
}

function SourcesPartRenderer({ part }: { part: ContentPart }) {
  const sources = (part.data.sources || []) as Source[];
  if (!sources.length) return null;

  return (
    <div className="my-3 flex flex-wrap gap-2">
      {sources.map((source, i) => {
        const domain = source.domain || (() => { try { return new URL(source.url).hostname; } catch { return ''; } })();
        const favicon = source.favicon || `https://www.google.com/s2/favicons?domain=${domain}&sz=16`;
        return (
          <a
            key={i}
            href={source.url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-full bg-bg-gray/60 border border-border-light text-xs text-text-secondary hover:bg-bg-gray hover:text-text-body transition-colors"
          >
            <img src={favicon} alt="" className="w-3.5 h-3.5 rounded-sm" onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }} />
            <span className="max-w-[120px] truncate">{source.title || domain}</span>
          </a>
        );
      })}
    </div>
  );
}

function SourceRefRenderer({ part, sources }: { part: ContentPart; sources: Source[] }) {
  const sourceIndex = (part.data.source_index as number) || 0;
  const source = sources[sourceIndex - 1]; // 1-based index

  if (!source) {
    return <sup className="text-xs text-text-tertiary">[{sourceIndex}]</sup>;
  }

  return (
    <a
      href={source.url}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center justify-center w-4 h-4 rounded-full bg-bg-gray text-[10px] font-medium text-text-secondary hover:bg-bg-gray-dark transition-colors align-super ml-0.5"
      title={source.title || source.url}
    >
      {sourceIndex}
    </a>
  );
}

interface EmailItem {
  id?: string;
  thread_id?: string;
  subject?: string;
  from?: string;
  from_name?: string;
  from_email?: string;
  snippet?: string;
}

interface CalendarItem {
  id?: string;
  title?: string;
  summary?: string;
  start_time?: string;
  end_time?: string;
  location?: string;
  is_all_day?: boolean;
}

function EmailRefRenderer({ part, emails }: { part: ContentPart; emails: EmailItem[] }) {
  const navigate = useNavigate();
  const emailIndex = (part.data.email_index as number) || 0;
  const email = emails[emailIndex - 1]; // 1-based index

  if (!email) return null;

  const subject = email.subject || 'Email';
  const from = email.from_name || email.from_email || email.from || '';
  const emailId = email.id;
  const threadId = email.thread_id;

  const handleClick = () => {
    const params = new URLSearchParams();
    if (emailId) params.set('open', emailId);
    if (threadId) params.set('thread', threadId);
    navigate(params.toString() ? `/email?${params}` : '/email');
  };

  const displayFrom = cleanFromName(from);

  return (
    <button type="button" onClick={handleClick} className="group/eref block w-[340px] max-w-full mb-1.5 no-underline text-left">
      <div className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg bg-white border border-border-light shadow-[0_1px_2px_rgba(0,0,0,0.04)] group-hover/eref:shadow-[0_2px_6px_rgba(0,0,0,0.06)] transition-all">
        <div className={`w-5 h-5 rounded-full ${getAvatarColor(displayFrom)} flex items-center justify-center shrink-0`}>
          <span className="text-[8px] font-semibold text-white leading-none">{getInitials(displayFrom)}</span>
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-xs font-medium text-text-body truncate">{subject}</p>
          {displayFrom && <p className="text-[10px] text-text-tertiary truncate">{displayFrom}</p>}
        </div>
      </div>
    </button>
  );
}

function CalRefRenderer({ part, events }: { part: ContentPart; events: CalendarItem[] }) {
  const calIndex = (part.data.cal_index as number) || 0;
  const event = events[calIndex - 1]; // 1-based index

  if (!event) return null;

  const title = event.title || event.summary || 'Event';
  const startTime = event.start_time;

  const formatTime = (iso: string) => {
    try {
      return new Date(iso).toLocaleString(undefined, {
        weekday: 'short', month: 'short', day: 'numeric',
        hour: 'numeric', minute: '2-digit',
      });
    } catch { return iso; }
  };

  return (
    <span className="inline-block mr-1.5 mb-1.5">
      <div className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg bg-white border border-border-light shadow-[0_1px_2px_rgba(0,0,0,0.04)]">
        <div className="w-5 h-5 rounded-md bg-[#E9F2FF] flex items-center justify-center shrink-0">
          <CalendarIcon className="w-3 h-3 text-blue-600" />
        </div>
        <div className="min-w-0">
          <p className="text-xs font-medium text-text-body truncate max-w-[220px]">{title}</p>
          {startTime && <p className="text-[10px] text-text-tertiary">{formatTime(startTime)}</p>}
        </div>
      </div>
    </span>
  );
}

function AttachmentPartRenderer({ part }: { part: ContentPart }) {
  const attachmentId = part.data.attachment_id as string;
  const width = part.data.width as number | undefined;
  const height = part.data.height as number | undefined;

  if (!attachmentId) return null;

  return (
    <div className="my-2">
      <ChatAttachmentImage attachmentId={attachmentId} width={width} height={height} />
    </div>
  );
}

function ReasoningPartRenderer({ part }: { part: ContentPart }) {
  const [isOpen, setIsOpen] = useState(false);
  const content = (part.data.content || '') as string;

  if (!content) return null;

  return (
    <div className="my-3 border border-border-light rounded-lg overflow-hidden">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center gap-2 px-3 py-2 text-xs text-text-tertiary hover:bg-bg-gray/50 transition-colors"
      >
        {isOpen ? <ChevronDownIcon className="w-3 h-3" /> : <ChevronRightIcon className="w-3 h-3" />}
        <span>Reasoning</span>
      </button>
      {isOpen && (
        <div className="px-3 pb-3 text-sm text-text-secondary">
          <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
            {content}
          </ReactMarkdown>
        </div>
      )}
    </div>
  );
}

function ToolResultPartRenderer({ part }: { part: ContentPart }) {
  // tool_result parts are similar to display parts
  return <DisplayPartRenderer part={{ ...part, type: 'display' }} />;
}

// ============================================================================
// Main Renderer
// ============================================================================

interface ContentPartsRendererProps {
  parts: ContentPart[];
  messageId?: string;
  isStreaming?: boolean;
}

function ContentPartsRenderer({ parts, messageId, isStreaming }: ContentPartsRendererProps) {
  // Collect all sources for source_ref linking
  const allSources: Source[] = [];
  for (const part of parts) {
    if (part.type === 'sources' && Array.isArray(part.data.sources)) {
      allSources.push(...(part.data.sources as Source[]));
    }
  }

  // Collect all email items from display/tool_result parts for email_ref linking
  const allEmails: EmailItem[] = [];
  for (const part of parts) {
    if ((part.type === 'display' || part.type === 'tool_result') &&
        part.data.display_type === 'emails' &&
        Array.isArray(part.data.items)) {
      allEmails.push(...(part.data.items as EmailItem[]));
    }
  }

  // Collect all calendar items from display/tool_result parts for cal_ref linking
  const allCalEvents: CalendarItem[] = [];
  for (const part of parts) {
    if ((part.type === 'display' || part.type === 'tool_result') &&
        part.data.display_type === 'calendar_events' &&
        Array.isArray(part.data.items)) {
      allCalEvents.push(...(part.data.items as CalendarItem[]));
    }
  }

  // If we have inline ref parts, hide the corresponding display cards (they're redundant)
  const hasEmailRefs = parts.some(p => p.type === 'email_ref');
  const hasCalRefs = parts.some(p => p.type === 'cal_ref');

  // Find the last text part index for streaming animation
  const lastTextPartIndex = parts.reduce((lastIdx, part, idx) =>
    part.type === 'text' ? idx : lastIdx, -1);

  return (
    <>
      {parts.map((part, index) => {
        switch (part.type) {
          case 'text':
            // Only animate the last text part during streaming
            const isLastTextPart = index === lastTextPartIndex;
            return (
              <TextPartRenderer
                key={part.id}
                content={(part.data.content as string) || ''}
                isStreaming={isStreaming && isLastTextPart}
              />
            );
          case 'display': {
            // Skip display cards when inline refs handle the linking
            const dt = part.data.display_type as string;
            if (hasEmailRefs && dt === 'emails') return null;
            if (hasCalRefs && dt === 'calendar_events') return null;
            return <DisplayPartRenderer key={part.id} part={part} />;
          }
          case 'tool_result': {
            const dt2 = part.data.display_type as string;
            if (hasEmailRefs && dt2 === 'emails') return null;
            if (hasCalRefs && dt2 === 'calendar_events') return null;
            return <ToolResultPartRenderer key={part.id} part={part} />;
          }
          case 'action':
            return <ActionPartRenderer key={part.id} part={part} messageId={messageId} />;
          case 'tool_call':
            return <ToolCallPartRenderer key={part.id} part={part} />;
          case 'sources':
            return <SourcesPartRenderer key={part.id} part={part} />;
          case 'source_ref':
            return <SourceRefRenderer key={part.id} part={part} sources={allSources} />;
          case 'email_ref':
            return <EmailRefRenderer key={part.id} part={part} emails={allEmails} />;
          case 'cal_ref':
            return <CalRefRenderer key={part.id} part={part} events={allCalEvents} />;
          case 'attachment':
            return <AttachmentPartRenderer key={part.id} part={part} />;
          case 'reasoning':
            return <ReasoningPartRenderer key={part.id} part={part} />;
          default:
            return null;
        }
      })}
    </>
  );
}

export default memo(ContentPartsRenderer);

// Export shared markdown components for reuse
export { markdownComponents, CodeBlock };
