import { useEffect, useCallback, useState, useRef } from "react";
import { motion, AnimatePresence } from "motion/react";
import { useEditor, EditorContent } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import Link from "@tiptap/extension-link";
import Underline from "@tiptap/extension-underline";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";
import {
  XMarkIcon,
  PaperAirplaneIcon,
  PaperClipIcon,
  LinkIcon,
  ListBulletIcon,
  ChevronDownIcon,
  ChevronUpIcon,
} from "@heroicons/react/24/outline";
import { useEmailStore } from "../../../stores/emailStore";
import { emailKeys } from "../../../hooks/queries/keys";
import { type EmailAttachmentUpload } from "../../../api/client";
import ChipInput, { type ChipInputRef } from "../ComposeEmail/ChipInput";

export default function InlineReplyComposer() {
  const queryClient = useQueryClient();
  const {
    inlineReply,
    updateInlineReplyDraft,
    updateInlineReplyBody,
    sendInlineReply,
    discardInlineReply,
    clearOptimisticReply,
  } = useEmailStore();

  const { isOpen, draft, isSending, sendError } = inlineReply;
  const [showQuoted, setShowQuoted] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const ccInputRef = useRef<ChipInputRef>(null);
  const bccInputRef = useRef<ChipInputRef>(null);
  const [showCc, setShowCc] = useState(draft?.cc ? draft.cc.length > 0 : false);
  const [showBcc, setShowBcc] = useState(draft?.bcc ? draft.bcc.length > 0 : false);

  // Attachment state
  const [attachments, setAttachments] = useState<File[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const addFilesRef = useRef<(files: FileList | File[]) => void>(() => {});

  // Track if the editor itself triggered the update to avoid sync loops
  const isEditorUpdateRef = useRef(false);

  // Initialize Tiptap editor
  const editor = useEditor({
    extensions: [
      StarterKit.configure({
        heading: false,
        codeBlock: false,
        code: false,
        link: false,
        underline: false,
        blockquote: {
          HTMLAttributes: {
            class: "border-l-2 border-gray-300 pl-3 text-gray-600",
          },
        },
      }),
      Link.configure({
        openOnClick: false,
        HTMLAttributes: {
          class: "text-blue-600 underline",
        },
      }),
      Underline,
    ],
    content: "",
    editorProps: {
      attributes: {
        class: "prose prose-sm max-w-none focus:outline-none min-h-[100px] px-4 py-2 text-sm",
      },
      handleDrop: (_view, event) => {
        if (event.dataTransfer?.files?.length) {
          event.preventDefault();
          addFilesRef.current(event.dataTransfer.files);
          return true;
        }
        return false;
      },
      handlePaste: (_view, event) => {
        if (event.clipboardData?.files?.length) {
          event.preventDefault();
          addFilesRef.current(event.clipboardData.files);
          return true;
        }
        return false;
      },
    },
    onUpdate: ({ editor }) => {
      isEditorUpdateRef.current = true;
      updateInlineReplyBody(editor.getHTML(), editor.getText());
    },
  });

  // Set initial content when draft changes
  useEffect(() => {
    // Skip if this was triggered by the editor itself
    if (isEditorUpdateRef.current) {
      isEditorUpdateRef.current = false;
      return;
    }
    if (editor && draft?.bodyHtml && editor.isEmpty) {
      editor.commands.setContent(draft.bodyHtml);
      // Focus at start (before quoted content)
      editor.commands.focus("start");
    }
  }, [draft?.bodyHtml, editor]);

  // Auto-show CC/BCC when draft has values (e.g., reply-all populates CC)
  useEffect(() => {
    if (draft?.cc && draft.cc.length > 0) setShowCc(true);
    if (draft?.bcc && draft.bcc.length > 0) setShowBcc(true);
  }, [draft?.replyToEmailId]);

  // Focus editor when opened
  useEffect(() => {
    if (isOpen && editor) {
      setTimeout(() => {
        editor.commands.focus("start");
      }, 100);
    }
  }, [isOpen, editor]);

  const addFiles = useCallback((files: FileList | File[]) => {
    const MAX_SIZE = 25 * 1024 * 1024;
    const newFiles = Array.from(files).filter((file) => {
      if (file.size > MAX_SIZE) {
        toast.error(`"${file.name}" exceeds 25 MB limit`);
        return false;
      }
      return true;
    });
    if (newFiles.length > 0) {
      setAttachments((prev) => [...prev, ...newFiles]);
    }
  }, []);
  addFilesRef.current = addFiles;

  const removeAttachment = useCallback((index: number) => {
    setAttachments((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const filesToBase64 = useCallback(
    async (files: File[]): Promise<EmailAttachmentUpload[]> => {
      return Promise.all(
        files.map(
          (file) =>
            new Promise<EmailAttachmentUpload>((resolve, reject) => {
              const reader = new FileReader();
              reader.onload = () => {
                const dataUrl = reader.result as string;
                const base64 = dataUrl.split(",")[1];
                resolve({
                  filename: file.name,
                  content: base64,
                  mime_type: file.type || "application/octet-stream",
                });
              };
              reader.onerror = reject;
              reader.readAsDataURL(file);
            })
        )
      );
    },
    []
  );

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.currentTarget === e.target || !e.currentTarget.contains(e.relatedTarget as Node)) {
      setIsDragOver(false);
    }
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      const alreadyHandled = e.defaultPrevented;
      e.preventDefault();
      e.stopPropagation();
      setIsDragOver(false);
      if (!alreadyHandled && e.dataTransfer.files.length > 0) {
        addFiles(e.dataTransfer.files);
      }
    },
    [addFiles]
  );

  const handleSend = useCallback(async () => {
    if (isSending) return;

    // Validate before sending
    if (!draft || draft.to.length === 0) {
      toast.error('Please add at least one recipient');
      return;
    }

    // Convert attachment files to base64
    let uploadAttachments: EmailAttachmentUpload[] | undefined;
    if (attachments.length > 0) {
      try {
        uploadAttachments = await filesToBase64(attachments);
      } catch {
        toast.error("Failed to read attachment files");
        return;
      }
    }

    // Capture threadId before sending for post-send refetch
    const threadId = draft.threadId;

    toast.promise(
      sendInlineReply(uploadAttachments).then(async (success) => {
        if (!success) throw new Error('Failed to send');
        setAttachments([]);
        if (threadId) {
          await queryClient.refetchQueries({ queryKey: emailKeys.thread(threadId) });
          clearOptimisticReply();
        } else {
          clearOptimisticReply();
        }
        return success;
      }),
      {
        loading: 'Sending...',
        success: 'Email sent',
        error: 'Failed to send reply',
      }
    );
  }, [draft, sendInlineReply, queryClient, clearOptimisticReply, isSending, attachments, filesToBase64]);

  const addLink = useCallback(() => {
    if (!editor) return;
    const url = window.prompt("Enter URL:");
    if (url) {
      editor.chain().focus().setLink({ href: url }).run();
    }
  }, [editor]);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (!isOpen) return;

      // Cmd/Ctrl + Enter to send
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
        e.preventDefault();
        handleSend();
      }

      // Escape to close
      if (e.key === "Escape") {
        e.preventDefault();
        discardInlineReply();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [isOpen, handleSend, discardInlineReply]);

  if (!isOpen || !draft) return null;

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: 10 }}
      transition={{ duration: 0.15 }}
      className="bg-bg-white rounded-lg border border-border-gray shadow-sm overflow-hidden"
    >
      {/* Error message */}
      {sendError && (
        <div className="px-4 py-2 bg-red-50 text-red-600 text-sm border-b border-red-100">
          {sendError}
        </div>
      )}

      {/* Recipients */}
      <div className="relative">
        <button
          onClick={discardInlineReply}
          className="absolute right-3 top-2 p-1 text-text-secondary hover:text-text-body rounded transition-colors z-10"
          title="Discard"
        >
          <XMarkIcon className="w-4 h-4" />
        </button>
        <div className="flex items-start">
          <div className="flex-1 min-w-0 pr-16">
            <ChipInput
              label="To"
              value={draft.to}
              onChange={(value) => updateInlineReplyDraft("to", value)}
              placeholder=""
              onTabToNext={() => {
                if (showCc) ccInputRef.current?.focus();
                else if (showBcc) bccInputRef.current?.focus();
              }}
            />
          </div>
          <div className="flex items-center gap-1 pr-10 pt-2 shrink-0">
            {!showCc && (
              <button
                onClick={() => { setShowCc(true); setTimeout(() => ccInputRef.current?.focus(), 50); }}
                className="text-sm text-text-secondary hover:text-text-body transition-colors"
              >
                Cc
              </button>
            )}
            {!showBcc && (
              <button
                onClick={() => { setShowBcc(true); setTimeout(() => bccInputRef.current?.focus(), 50); }}
                className="text-sm text-text-secondary hover:text-text-body transition-colors"
              >
                Bcc
              </button>
            )}
          </div>
        </div>
        <AnimatePresence initial={false}>
          {showCc && (
            <motion.div
              key="cc-field"
              initial={{ height: 0 }}
              animate={{ height: "auto" }}
              exit={{ height: 0 }}
              transition={{ type: "spring", duration: 0.3, bounce: 0 }}
              className="overflow-hidden"
            >
              <div className="mx-4 border-t border-border-gray" />
              <ChipInput
                ref={ccInputRef}
                label="Cc"
                value={draft.cc}
                onChange={(value) => updateInlineReplyDraft("cc", value)}
                placeholder=""
                onTabToNext={() => {
                  if (showBcc) bccInputRef.current?.focus();
                }}
              />
            </motion.div>
          )}
          {showBcc && (
            <motion.div
              key="bcc-field"
              initial={{ height: 0 }}
              animate={{ height: "auto" }}
              exit={{ height: 0 }}
              transition={{ type: "spring", duration: 0.3, bounce: 0 }}
              className="overflow-hidden"
            >
              <div className="mx-4 border-t border-border-gray" />
              <ChipInput
                ref={bccInputRef}
                label="Bcc"
                value={draft.bcc}
                onChange={(value) => updateInlineReplyDraft("bcc", value)}
                placeholder=""
              />
            </motion.div>
          )}
        </AnimatePresence>
      </div>
      <div className="mx-4 border-t border-border-gray" />

      {/* Editor with drag-and-drop */}
      <div
        className={`min-h-[100px] max-h-[250px] overflow-y-auto relative ${isDragOver ? "ring-2 ring-inset ring-brand-primary/40 bg-brand-primary/5" : ""}`}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
      >
        <EditorContent editor={editor} />
        {isDragOver && (
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <div className="flex items-center gap-2 px-4 py-2 bg-white/90 rounded-lg shadow-sm border border-brand-primary/30">
              <PaperClipIcon className="w-4 h-4 text-brand-primary" />
              <span className="text-sm font-medium text-brand-primary">Drop files to attach</span>
            </div>
          </div>
        )}
      </div>

      {/* Attachment list */}
      {attachments.length > 0 && (
        <div className="px-4 py-2 border-t border-border-gray flex flex-wrap gap-2">
          {attachments.map((file, idx) => (
            <div
              key={`${file.name}-${idx}`}
              className="flex items-center gap-1.5 px-2.5 py-1 bg-bg-gray-dark/50 rounded-md text-sm group"
            >
              <PaperClipIcon className="w-3.5 h-3.5 text-text-tertiary shrink-0" />
              <span className="text-text-body truncate max-w-[160px]">{file.name}</span>
              <span className="text-text-tertiary text-xs shrink-0">
                {file.size < 1024 ? `${file.size} B` : file.size < 1024 * 1024 ? `${(file.size / 1024).toFixed(0)} KB` : `${(file.size / (1024 * 1024)).toFixed(1)} MB`}
              </span>
              <button
                onClick={() => removeAttachment(idx)}
                className="p-0.5 text-text-tertiary hover:text-red-500 rounded transition-colors opacity-0 group-hover:opacity-100"
                title="Remove attachment"
              >
                <XMarkIcon className="w-3.5 h-3.5" />
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={(e) => {
          if (e.target.files) addFiles(e.target.files);
          e.target.value = "";
        }}
      />

      {/* Quoted content toggle */}
      <button
        onClick={() => setShowQuoted(!showQuoted)}
        className="flex items-center gap-1 px-4 py-2 text-sm text-text-secondary hover:text-text-body transition-colors w-full text-left border-t border-border-gray"
      >
        {showQuoted ? <ChevronUpIcon className="w-3.5 h-3.5" /> : <ChevronDownIcon className="w-3.5 h-3.5" />}
        {showQuoted ? "Hide" : "Show"} {draft.replyType === "forward" ? "forwarded message" : "quoted text"}
      </button>

      {/* Formatting toolbar + Send button */}
      <div className="flex items-center justify-between px-3 py-2 bg-brand-secondary border-t border-border-gray">
        <div className="flex items-center gap-3">
          {/* Send button */}
          <button
            onClick={handleSend}
            disabled={isSending}
            className="flex items-center gap-2 px-4 py-1.5 bg-brand-primary text-text-light rounded-lg text-sm font-medium hover:opacity-90 transition-opacity disabled:opacity-50"
          >
            <PaperAirplaneIcon className="w-4 h-4" />
            {isSending ? 'Sending...' : 'Send'}
          </button>

          {/* Attach files */}
          <button
            onClick={() => fileInputRef.current?.click()}
            className="p-1.5 text-text-secondary hover:text-text-body hover:bg-black/5 rounded transition-colors"
            title="Attach files"
          >
            <PaperClipIcon className="w-4 h-4" />
          </button>

          {/* Formatting buttons */}
          <div className="flex items-center gap-1">
          <button
            onClick={() => editor?.chain().focus().toggleBold().run()}
            className={`p-1.5 rounded transition-colors ${
              editor?.isActive("bold")
                ? "bg-bg-gray-dark text-text-body"
                : "text-text-secondary hover:text-text-body hover:bg-bg-gray-dark/50"
            }`}
            title="Bold (Cmd+B)"
          >
            <span className={`text-sm font-bold ${editor?.isActive("bold") ? "text-text-body" : ""}`}>B</span>
          </button>
          <button
            onClick={() => editor?.chain().focus().toggleItalic().run()}
            className={`p-1.5 rounded transition-colors ${
              editor?.isActive("italic")
                ? "bg-bg-gray-dark text-text-body"
                : "text-text-secondary hover:text-text-body hover:bg-bg-gray-dark/50"
            }`}
            title="Italic (Cmd+I)"
          >
            <span className={`text-sm italic ${editor?.isActive("italic") ? "font-bold" : ""}`}>I</span>
          </button>
          <button
            onClick={() => editor?.chain().focus().toggleUnderline().run()}
            className={`p-1.5 rounded transition-colors ${
              editor?.isActive("underline")
                ? "bg-bg-gray-dark text-text-body"
                : "text-text-secondary hover:text-text-body hover:bg-bg-gray-dark/50"
            }`}
            title="Underline (Cmd+U)"
          >
            <span className={`text-sm underline ${editor?.isActive("underline") ? "font-bold" : ""}`}>U</span>
          </button>
          <div className="w-px h-4 bg-border-gray mx-1" />
          <button
            onClick={addLink}
            className={`p-1.5 rounded transition-colors ${
              editor?.isActive("link")
                ? "bg-bg-gray-dark text-text-body"
                : "text-text-secondary hover:text-text-body hover:bg-bg-gray-dark/50"
            }`}
            title="Add Link"
          >
            <LinkIcon className="w-4 h-4" />
          </button>
          <div className="w-px h-4 bg-border-gray mx-1" />
          <button
            onClick={() => editor?.chain().focus().toggleBulletList().run()}
            className={`p-1.5 rounded transition-colors ${
              editor?.isActive("bulletList")
                ? "bg-bg-gray-dark text-text-body"
                : "text-text-secondary hover:text-text-body hover:bg-bg-gray-dark/50"
            }`}
            title="Bullet List"
          >
            <ListBulletIcon className="w-4 h-4" />
          </button>
            <button
              onClick={() => editor?.chain().focus().toggleOrderedList().run()}
              className={`p-1.5 rounded transition-colors ${
                editor?.isActive("orderedList")
                  ? "bg-bg-gray-dark text-text-body"
                  : "text-text-secondary hover:text-text-body hover:bg-bg-gray-dark/50"
              }`}
              title="Numbered List"
            >
              <span className="text-sm font-medium">1.</span>
            </button>
          </div>
        </div>
      </div>
    </motion.div>
  );
}
