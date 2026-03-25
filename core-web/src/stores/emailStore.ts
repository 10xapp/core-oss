import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import {
  sendEmail as sendEmailApi,
  sendDraft as sendDraftApi,
  saveDraft as saveDraftApi,
  updateDraft as updateDraftApi,
  deleteDraft as deleteDraftApi,
  searchEmails as searchEmailsApi,
  fetchRemoteEmail as fetchRemoteEmailApi,
  getEmailDetails,
  getThreadDetails,
  type Email,
  type EmailAccountStatus,
  type EmailWithAttachments,
  type EmailAttachmentUpload,
} from '../api/client';

export type EmailFolder = 'INBOX' | 'SENT' | 'DRAFT' | 'TRASH' | 'STARRED';

// Helper to derive account key from selectedAccountIds
const getAccountKeyFromIds = (selectedAccountIds: string[]): string => {
  if (selectedAccountIds.length === 0) return 'all';
  if (selectedAccountIds.length === 1) return selectedAccountIds[0];
  return selectedAccountIds.slice().sort().join(',');
};

const resolveGmailDraftId = (email: Email): string | undefined => {
  if (email.gmail_draft_id) return email.gmail_draft_id;

  const rawItem = email.raw_item;
  if (!rawItem || typeof rawItem !== 'object') return undefined;
  if (typeof rawItem.gmail_draft_id === 'string' && rawItem.gmail_draft_id) {
    return rawItem.gmail_draft_id;
  }
  // raw_item.id is only a draft ID when payload is a draft wrapper.
  if (rawItem.message && typeof rawItem.id === 'string' && rawItem.id) {
    return rawItem.id;
  }
  return undefined;
};

export interface ComposeDraft {
  id?: string;  // Gmail draft ID (or legacy message ID resolvable by backend)
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string;
  bodyHtml: string;
  bodyText: string;
}

interface ComposeState {
  isOpen: boolean;
  isMinimized: boolean;
  draft: ComposeDraft;
  isSending: boolean;
  isSavingDraft: boolean;
  sendError: string | null;
}

const createEmptyComposeDraft = (): ComposeDraft => ({
  to: [],
  cc: [],
  bcc: [],
  subject: '',
  bodyHtml: '',
  bodyText: ''
});

const createEmptyComposeState = (): ComposeState => ({
  isOpen: false,
  isMinimized: false,
  draft: createEmptyComposeDraft(),
  isSending: false,
  isSavingDraft: false,
  sendError: null
});

// Inline Reply types
export interface InlineReplyDraft {
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string;
  bodyHtml: string;
  bodyText: string;
  replyToEmailId: string;
  threadId?: string;
  accountId?: string;
  accountEmail?: string;  // The user's email address for this account
  replyType: 'reply' | 'reply-all' | 'forward';
}

interface InlineReplyState {
  isOpen: boolean;
  draft: InlineReplyDraft | null;
  isSending: boolean;
  sendError: string | null;
}

const createEmptyInlineReplyState = (): InlineReplyState => ({
  isOpen: false,
  draft: null,
  isSending: false,
  sendError: null
});

interface EmailState {
  // UI state
  activeFolder: EmailFolder;
  selectedEmailId: string | null;

  // Account filtering (Apple Mail style)
  accountsStatus: EmailAccountStatus[];
  selectedAccountIds: string[];  // Empty = all accounts (unified view)

  // Compose state
  compose: ComposeState;

  // Inline reply state
  inlineReply: InlineReplyState;

  // Local read overrides to prevent stale server data from flipping read status
  readOverrides: Record<string, number>;

  // Search
  searchResults: Email[] | null;
  isSearching: boolean;
  searchMeta: { local_count: number; remote_count: number; provider_errors?: Record<string, string>; has_provider_errors: boolean } | null;
  fetchingRemoteId: string | null;

  // Optimistic reply shown in thread while sending
  optimisticReply: Email | null;

  // Email detail cache (for thread view - loads body content for multiple emails)
  emailDetailsCache: Record<string, Email>;
  loadingDetailsId: string | null;
  threadAttachmentsCache: Record<string, EmailWithAttachments[]>;

  // Legacy compatibility (empty, data comes from React Query)
  accountFolders: Record<string, Record<EmailFolder, { emails: Email[]; hasMore: boolean }>>;

  // UI Actions
  setActiveFolder: (folder: EmailFolder) => void;
  setSelectedEmailId: (id: string | null) => void;
  setSelectedAccounts: (accountIds: string[]) => void;
  toggleAccountFilter: (accountId: string) => void;
  setAccountsStatus: (accounts: EmailAccountStatus[]) => void;

  // Email detail actions (for thread view)
  fetchEmailDetails: (emailId: string) => Promise<Email | null>;
  fetchThreadAttachments: (threadId: string) => Promise<EmailWithAttachments[] | null>;

  // Helpers
  getAccountKey: () => string;
  prefetchAllAccounts: () => void;

  // Compose actions
  openCompose: () => void;
  openComposeWithDraft: (email: Email) => Promise<void>;
  closeCompose: () => Promise<void>;
  toggleComposeMinimize: () => void;
  updateComposeDraft: (field: keyof ComposeDraft, value: string | string[]) => void;
  updateComposeBody: (bodyHtml: string, bodyText: string) => void;
  sendComposedEmail: (attachments?: EmailAttachmentUpload[]) => Promise<boolean>;
  discardCompose: () => void;

  // Inline reply/forward actions
  openInlineReply: (email: Email, type: 'reply' | 'reply-all') => void;
  openInlineForward: (email: Email) => void;
  closeInlineReply: () => void;
  updateInlineReplyDraft: (field: keyof InlineReplyDraft, value: string | string[]) => void;
  updateInlineReplyBody: (bodyHtml: string, bodyText: string) => void;
  sendInlineReply: (attachments?: EmailAttachmentUpload[]) => Promise<boolean>;
  discardInlineReply: () => void;

  // Search actions
  searchEmailsServer: (query: string) => Promise<void>;
  fetchRemoteAndOpen: (email: Email) => Promise<void>;
  clearSearch: () => void;

  // Optimistic reply actions
  setOptimisticReply: (email: Email) => void;
  clearOptimisticReply: () => void;

  // Read override actions (for React Query optimistic updates)
  addReadOverrides: (emailIds: string[]) => void;
  removeReadOverrides: (emailIds: string[]) => void;
}

const MAX_CACHE_SIZE = 10;

export const useEmailStore = create<EmailState>()(
  persist(
    (set, get) => ({
      activeFolder: 'INBOX',
      selectedEmailId: null,
      accountsStatus: [],
      selectedAccountIds: [],  // Empty = all accounts (unified view)
      compose: createEmptyComposeState(),
      inlineReply: createEmptyInlineReplyState(),
      readOverrides: {},
      searchResults: null,
      isSearching: false,
      searchMeta: null,
      fetchingRemoteId: null,
      optimisticReply: null,
      emailDetailsCache: {},
      loadingDetailsId: null,
      threadAttachmentsCache: {},
      accountFolders: {},  // Legacy - kept for compatibility, data comes from React Query

      getAccountKey: () => getAccountKeyFromIds(get().selectedAccountIds),

      prefetchAllAccounts: () => {
        // No-op - React Query handles prefetching now
      },

      fetchEmailDetails: async (emailId: string) => {
        const { emailDetailsCache } = get();

        // Return cached if available
        if (
          emailDetailsCache[emailId]?.body_html ||
          emailDetailsCache[emailId]?.body_text ||
          emailDetailsCache[emailId]?.gmail_draft_id
        ) {
          return emailDetailsCache[emailId];
        }

        set({ loadingDetailsId: emailId });

        try {
          const { email: fullEmail } = await getEmailDetails(emailId);

          // Evict oldest if cache is full
          const cache = { ...get().emailDetailsCache };
          const cacheKeys = Object.keys(cache);
          if (cacheKeys.length >= MAX_CACHE_SIZE) {
            delete cache[cacheKeys[0]];
          }
          cache[emailId] = fullEmail;

          set((state) => ({
            emailDetailsCache: cache,
            loadingDetailsId: state.loadingDetailsId === emailId ? null : state.loadingDetailsId
          }));

          return fullEmail;
        } catch (err) {
          console.error('Failed to load email details:', err);
          set((state) => ({
            loadingDetailsId: state.loadingDetailsId === emailId ? null : state.loadingDetailsId
          }));
          return null;
        }
      },

      fetchThreadAttachments: async (threadId: string) => {
        const { threadAttachmentsCache } = get();

        // Return cached if available
        if (threadAttachmentsCache[threadId]) {
          return threadAttachmentsCache[threadId];
        }

        try {
          const threadData = await getThreadDetails(threadId);

          // Cache the thread emails with attachments
          const cache = { ...get().threadAttachmentsCache };
          cache[threadId] = threadData.emails;

          set({ threadAttachmentsCache: cache });

          return threadData.emails;
        } catch (err) {
          console.error('Failed to load thread attachments:', err);
          return null;
        }
      },

      setActiveFolder: (folder: EmailFolder) => {
        set({ activeFolder: folder, selectedEmailId: null });
      },

      setSelectedEmailId: (id: string | null) => {
        set({ selectedEmailId: id });
      },

      setSelectedAccounts: (accountIds: string[]) => {
        set({ selectedAccountIds: accountIds, selectedEmailId: null });
      },

      toggleAccountFilter: (accountId: string) => {
        const { selectedAccountIds } = get();
        const isSelected = selectedAccountIds.includes(accountId);
        const newSelection = isSelected
          ? selectedAccountIds.filter(id => id !== accountId)
          : [...selectedAccountIds, accountId];
        set({ selectedAccountIds: newSelection, selectedEmailId: null });
      },

      setAccountsStatus: (accounts: EmailAccountStatus[]) => {
        set({ accountsStatus: accounts });
      },

      // Compose actions
      openCompose: () => {
        set({
          compose: {
            ...createEmptyComposeState(),
            isOpen: true
          }
        });
      },

      openComposeWithDraft: async (email: Email) => {
        // Always fetch details for drafts to get reliable draft identifiers
        const fetched = await get().fetchEmailDetails(email.id);
        const details = fetched || email;
        const draftIdentifier =
          resolveGmailDraftId(details) ||
          resolveGmailDraftId(email) ||
          details.id ||
          email.id;

        set({
          selectedEmailId: null,
          compose: {
            isOpen: true,
            isMinimized: false,
            isSending: false,
            isSavingDraft: false,
            sendError: null,
            draft: {
              id: draftIdentifier,
              to: details.to_emails || [],
              cc: details.cc_emails || [],
              bcc: [],
              subject: details.subject || '',
              bodyHtml: details.body_html || '',
              bodyText: details.body_text || '',
            },
          },
        });
      },

      closeCompose: async () => {
        const { compose } = get();
        const { draft } = compose;

        // Check if there's content worth saving as draft
        const hasContent = draft.to.length > 0 || draft.subject.trim() || draft.bodyText.trim();

        if (hasContent) {
          // Auto-save as draft
          set(state => ({
            compose: { ...state.compose, isSavingDraft: true }
          }));

          try {
            if (draft.id) {
              // Update existing draft
              await updateDraftApi(draft.id, {
                to: draft.to.length > 0 ? draft.to : undefined,
                cc: draft.cc.length > 0 ? draft.cc : undefined,
                bcc: draft.bcc.length > 0 ? draft.bcc : undefined,
                subject: draft.subject || undefined,
                body_html: draft.bodyHtml || undefined,
                body_text: draft.bodyText || undefined
              });
            } else {
              // Create new draft
              await saveDraftApi({
                to: draft.to.length > 0 ? draft.to : undefined,
                cc: draft.cc.length > 0 ? draft.cc : undefined,
                bcc: draft.bcc.length > 0 ? draft.bcc : undefined,
                subject: draft.subject || undefined,
                body_html: draft.bodyHtml || undefined,
                body_text: draft.bodyText || undefined
              });
            }
            // React Query will handle cache invalidation via queryClient
          } catch (err) {
            console.error('Failed to save draft:', err);
          }
        }

        // Close the compose window
        set({ compose: createEmptyComposeState() });
      },

      toggleComposeMinimize: () => {
        set(state => ({
          compose: {
            ...state.compose,
            isMinimized: !state.compose.isMinimized
          }
        }));
      },

      updateComposeDraft: (field: keyof ComposeDraft, value: string | string[]) => {
        set(state => ({
          compose: {
            ...state.compose,
            draft: {
              ...state.compose.draft,
              [field]: value
            }
          }
        }));
      },

      // Batch update for body fields to avoid double re-renders from editor
      updateComposeBody: (bodyHtml: string, bodyText: string) => {
        set(state => ({
          compose: {
            ...state.compose,
            draft: {
              ...state.compose.draft,
              bodyHtml,
              bodyText
            }
          }
        }));
      },

      sendComposedEmail: async (attachments) => {
        const { compose } = get();
        const { draft } = compose;

        // Validate
        if (draft.to.length === 0) {
          throw new Error('Please add at least one recipient');
        }

        set(state => ({
          compose: {
            ...state.compose,
            isSending: true,
            sendError: null,
          },
        }));

        try {
          if (draft.id) {
            // Send via draft endpoint so provider draft cleanup is handled server-side
            await sendDraftApi(draft.id);
          } else {
            await sendEmailApi({
              to: draft.to,
              cc: draft.cc.length > 0 ? draft.cc : undefined,
              bcc: draft.bcc.length > 0 ? draft.bcc : undefined,
              subject: draft.subject || '(No Subject)',
              body: draft.bodyText || '',
              body_html: draft.bodyHtml || undefined,
              attachments: attachments?.length ? attachments : undefined,
            });
          }

          set({ compose: createEmptyComposeState() });
          return true;
        } catch (err) {
          const message = err instanceof Error ? err.message : 'Failed to send email';
          set(state => ({
            compose: {
              ...state.compose,
              isSending: false,
              sendError: message,
            },
          }));
          throw err;
        }
      },

      discardCompose: () => {
        const { compose } = get();
        const { draft } = compose;

        // If there was a saved draft, delete it
        if (draft.id) {
          deleteDraftApi(draft.id).catch(err => {
            console.error('Failed to delete draft:', err);
          });
        }

        // Reset compose state
        set({ compose: createEmptyComposeState() });
      },

      // Inline Reply Actions
      openInlineReply: (email: Email, type: 'reply' | 'reply-all') => {
        const { accountsStatus } = get();

        // Find the user's email for this reply with fallback logic:
        // 1. Use email.connection_id directly (most reliable - this is the ext_connection_id)
        // 2. Try email.account_email (the account this email belongs to)
        // 3. If not set, find which user email is in to_emails/cc_emails (we received this email)
        // 4. Fall back to first connected account
        let userEmail = email.account_email;
        let accountId: string | undefined = email.connection_id;

        // If we have connection_id, use it directly and find matching account for userEmail
        if (accountId) {
          const account = accountsStatus.find(a => a.connectionId === accountId);
          if (account) {
            userEmail = account.email;
          }
        } else if (userEmail) {
          const account = accountsStatus.find(a => a.email.toLowerCase() === userEmail?.toLowerCase());
          accountId = account?.connectionId;
        } else {
          // Check which of user's accounts received this email
          const userEmails = new Set(accountsStatus.map(a => a.email.toLowerCase()));
          const allRecipients = [
            ...(email.to_emails || []),
            ...(email.cc_emails || [])
          ].map(e => e.toLowerCase());

          for (const recipient of allRecipients) {
            if (userEmails.has(recipient)) {
              const account = accountsStatus.find(a => a.email.toLowerCase() === recipient);
              if (account) {
                userEmail = account.email;
                accountId = account.connectionId;
                break;
              }
            }
          }

          // Final fallback: use first connected account
          if (!userEmail && accountsStatus.length > 0) {
            userEmail = accountsStatus[0].email;
            accountId = accountsStatus[0].connectionId;
          }
        }

        // Determine recipients
        const toRecipients = type === 'reply'
          ? [email.from_email]
          : [email.from_email, ...(email.to_emails || [])].filter(
              (e, i, arr) => arr.indexOf(e) === i // dedupe
            );

        const ccRecipients = type === 'reply-all'
          ? (email.cc_emails || [])
          : [];

        // Generate subject with "Re:" prefix (avoid "Re: Re: Re:")
        const subject = email.subject?.startsWith('Re:')
          ? email.subject
          : `Re: ${email.subject || ''}`;

        const draft: InlineReplyDraft = {
          to: toRecipients,
          cc: ccRecipients,
          bcc: [],
          subject,
          bodyHtml: '',
          bodyText: '',
          replyToEmailId: email.id,
          threadId: email.thread_id,
          accountId,
          accountEmail: userEmail,  // Store for optimistic UI
          replyType: type,
        };

        set({
          inlineReply: {
            isOpen: true,
            draft,
            isSending: false,
            sendError: null,
          }
        });
      },

      openInlineForward: (email: Email) => {
        const { accountsStatus } = get();

        // Find the user's email for this forward with fallback logic:
        // 1. Use email.connection_id directly (most reliable - this is the ext_connection_id)
        // 2. Try email.account_email (the account this email belongs to)
        // 3. If not set, find which user email is in to_emails/cc_emails (we received this email)
        // 4. Fall back to first connected account
        let userEmail = email.account_email;
        let accountId: string | undefined = email.connection_id;

        // If we have connection_id, use it directly and find matching account for userEmail
        if (accountId) {
          const account = accountsStatus.find(a => a.connectionId === accountId);
          if (account) {
            userEmail = account.email;
          }
        } else if (userEmail) {
          const account = accountsStatus.find(a => a.email.toLowerCase() === userEmail?.toLowerCase());
          accountId = account?.connectionId;
        } else {
          // Check which of user's accounts received this email
          const userEmails = new Set(accountsStatus.map(a => a.email.toLowerCase()));
          const allRecipients = [
            ...(email.to_emails || []),
            ...(email.cc_emails || [])
          ].map(e => e.toLowerCase());

          for (const recipient of allRecipients) {
            if (userEmails.has(recipient)) {
              const account = accountsStatus.find(a => a.email.toLowerCase() === recipient);
              if (account) {
                userEmail = account.email;
                accountId = account.connectionId;
                break;
              }
            }
          }

          // Final fallback: use first connected account
          if (!userEmail && accountsStatus.length > 0) {
            userEmail = accountsStatus[0].email;
            accountId = accountsStatus[0].connectionId;
          }
        }

        // Generate subject with "Fwd:" prefix (avoid "Fwd: Fwd: Fwd:")
        const subject = email.subject?.startsWith('Fwd:')
          ? email.subject
          : `Fwd: ${email.subject || ''}`;

        // Generate forwarded content
        const date = email.date ? new Date(email.date).toLocaleString() : '';
        const from = email.from_name
          ? `${email.from_name} &lt;${email.from_email}&gt;`
          : email.from_email;
        const to = email.to_emails?.join(', ') || '';

        const forwardedContent = `
          <div class="gmail_forward">
            <br>
            <div style="color: #666; font-size: 12px; border-top: 1px solid #ccc; padding-top: 12px; margin-top: 12px;">
              ---------- Forwarded message ---------<br>
              From: ${from}<br>
              Date: ${date}<br>
              Subject: ${email.subject || ''}<br>
              To: ${to}
            </div>
            <br>
            <div>
              ${email.body_html || email.body_text || ''}
            </div>
          </div>
        `;

        const draft: InlineReplyDraft = {
          to: [], // Forward starts with empty recipients
          cc: [],
          bcc: [],
          subject,
          bodyHtml: `<p></p>${forwardedContent}`,
          bodyText: '',
          replyToEmailId: email.id,
          threadId: email.thread_id,
          accountId,
          accountEmail: userEmail,  // Store for optimistic UI
          replyType: 'forward',
        };

        set({
          inlineReply: {
            isOpen: true,
            draft,
            isSending: false,
            sendError: null,
          }
        });
      },

      closeInlineReply: () => {
        set({ inlineReply: createEmptyInlineReplyState() });
      },

      updateInlineReplyDraft: (field: keyof InlineReplyDraft, value: string | string[]) => {
        set(state => ({
          inlineReply: {
            ...state.inlineReply,
            draft: state.inlineReply.draft
              ? { ...state.inlineReply.draft, [field]: value }
              : null,
          }
        }));
      },

      // Batch update for body fields to avoid double re-renders from editor
      updateInlineReplyBody: (bodyHtml: string, bodyText: string) => {
        set(state => ({
          inlineReply: {
            ...state.inlineReply,
            draft: state.inlineReply.draft
              ? { ...state.inlineReply.draft, bodyHtml, bodyText }
              : null,
          }
        }));
      },

      sendInlineReply: async (attachments) => {
        const { inlineReply } = get();
        const { draft, isSending } = inlineReply;

        if (isSending) {
          return false;
        }

        if (!draft || draft.to.length === 0) {
          set(state => ({
            inlineReply: {
              ...state.inlineReply,
              sendError: 'Please add at least one recipient'
            }
          }));
          return false;
        }

        // Use the stored account email from the draft, with fallback to first account
        const { accountsStatus } = get();
        const senderEmail = draft.accountEmail || accountsStatus[0]?.email || '';

        // Create optimistic reply to show immediately in thread
        const optimisticEmail: Email = {
          id: `optimistic-${Date.now()}`,
          thread_id: draft.threadId || '',
          account_email: senderEmail,
          from_name: senderEmail,
          from_email: senderEmail,
          to_emails: draft.to,
          cc_emails: draft.cc.length > 0 ? draft.cc : undefined,
          subject: draft.subject,
          snippet: draft.bodyText?.slice(0, 100) || '',
          body_html: draft.bodyHtml || undefined,
          body_text: draft.bodyText || undefined,
          date: new Date().toISOString(),
          is_read: true,
          is_starred: false,
          label_ids: ['SENT'],
          has_attachments: !!attachments?.length,
        };

        // Keep reply open while sending; close only on success to avoid data loss on failure.
        set(state => ({
          inlineReply: {
            ...state.inlineReply,
            isSending: true,
            sendError: null,
          },
          optimisticReply: optimisticEmail,
        }));

        try {
          await sendEmailApi({
            to: draft.to,
            cc: draft.cc.length > 0 ? draft.cc : undefined,
            bcc: draft.bcc.length > 0 ? draft.bcc : undefined,
            subject: draft.subject,
            body: draft.bodyText || '',
            body_html: draft.bodyHtml || undefined,
            thread_id: draft.threadId,
            account_id: draft.accountId,
            attachments: attachments?.length ? attachments : undefined,
          });

          set({ inlineReply: createEmptyInlineReplyState() });

          // Don't clear optimisticReply here - let the component clear it
          // after the query refetch completes to avoid a flash

          return true;
        } catch (err) {
          const message = err instanceof Error ? err.message : 'Failed to send reply';
          set(state => ({
            inlineReply: {
              ...state.inlineReply,
              isSending: false,
              sendError: message,
            },
            optimisticReply: null,
          }));
          return false;
        }
      },

      discardInlineReply: () => {
        set({ inlineReply: createEmptyInlineReplyState() });
      },

      // Search actions
      searchEmailsServer: async (query: string) => {
        const { selectedAccountIds } = get();
        set({ isSearching: true });

        try {
          const result = await searchEmailsApi({
            query,
            account_ids: selectedAccountIds.length > 0 ? selectedAccountIds : undefined,
            provider_search: true,
            max_results: 25,
          });

          // Stale check: only update if no newer search has started
          if (!get().isSearching) return;

          set({
            searchResults: result.emails,
            isSearching: false,
            searchMeta: {
              local_count: result.local_count,
              remote_count: result.remote_count,
              provider_errors: result.provider_errors,
              has_provider_errors: result.has_provider_errors,
            },
          });
        } catch (err) {
          console.error('Search failed:', err);
          set({ isSearching: false });
        }
      },

      fetchRemoteAndOpen: async (email: Email) => {
        if (email.source !== 'remote' || !email.connection_id) {
          get().setSelectedEmailId(email.id);
          return;
        }

        set({ fetchingRemoteId: email.id, selectedEmailId: email.id });

        try {
          const result = await fetchRemoteEmailApi({
            external_id: email.id,
            connection_id: email.connection_id,
          });

          if (result.success && result.email) {
            const fullEmail = result.email;

            // Update search results in-place
            set((state) => {
              const updatedResults = state.searchResults?.map(e =>
                e.id === email.id ? { ...fullEmail, source: 'local' as const } : e
              ) ?? null;

              return {
                searchResults: updatedResults,
                fetchingRemoteId: null,
              };
            });
          } else {
            console.error('Failed to fetch remote email:', result.error);
            set({ fetchingRemoteId: null });
          }
        } catch (err) {
          console.error('Failed to fetch remote email:', err);
          set({ fetchingRemoteId: null });
        }
      },

      clearSearch: () => {
        set({
          searchResults: null,
          isSearching: false,
          searchMeta: null,
          fetchingRemoteId: null,
        });
      },

      setOptimisticReply: (email: Email) => {
        set({ optimisticReply: email });
      },

      clearOptimisticReply: () => {
        set({ optimisticReply: null });
      },

      addReadOverrides: (emailIds: string[]) => {
        const now = Date.now();
        set(state => {
          const nextOverrides = { ...state.readOverrides };
          emailIds.forEach(id => {
            nextOverrides[id] = now;
          });
          return { readOverrides: nextOverrides };
        });
      },

      removeReadOverrides: (emailIds: string[]) => {
        set(state => {
          const nextOverrides = { ...state.readOverrides };
          emailIds.forEach(id => {
            delete nextOverrides[id];
          });
          return { readOverrides: nextOverrides };
        });
      },
    }),
    {
      name: 'core-email-storage-v5',  // Bumped version for slimmed down store
      partialize: (state) => ({
        // Only persist UI preferences
        activeFolder: state.activeFolder,
        selectedAccountIds: state.selectedAccountIds,
        readOverrides: state.readOverrides,
      })
    }
  )
);
