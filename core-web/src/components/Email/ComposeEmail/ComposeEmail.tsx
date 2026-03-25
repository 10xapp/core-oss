import { useEffect, useCallback, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { motion, AnimatePresence } from "motion/react";
import { useEditor, EditorContent } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import Link from "@tiptap/extension-link";
import Underline from "@tiptap/extension-underline";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";
import {
  XMarkIcon,
  MinusIcon,
  ChevronUpIcon,
  PaperAirplaneIcon,
  PaperClipIcon,
  LinkIcon,
  ListBulletIcon,
} from "@heroicons/react/24/outline";
import { useEmailStore } from "../../../stores/emailStore";
import { emailKeys } from "../../../hooks/queries/keys";
import { type EmailAttachmentUpload } from "../../../api/client";
import ChipInput, { type ChipInputRef } from "./ChipInput";

const COMPOSE_WIDTH = 520;

export default function ComposeEmail() {
  const {
    compose,
    closeCompose,
    toggleComposeMinimize,
    updateComposeDraft,
    updateComposeBody,
    sendComposedEmail,
    discardCompose,
  } = useEmailStore();

  const { isOpen, isMinimized, draft, isSending, sendError } = compose;
  const queryClient = useQueryClient();

  // Refs for Tab navigation between fields
  const ccInputRef = useRef<ChipInputRef>(null);
  const bccInputRef = useRef<ChipInputRef>(null);
  const subjectInputRef = useRef<HTMLInputElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // CC/BCC field visibility - auto-show if draft already has values
  const [showCc, setShowCc] = useState(draft.cc.length > 0);
  const [showBcc, setShowBcc] = useState(draft.bcc.length > 0);

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
            class: "border-l-3 border-gray-300 pl-3 text-gray-600",
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
    content: draft.bodyHtml || "",
    editorProps: {
      attributes: {
        class: "prose prose-sm max-w-none focus:outline-none min-h-[200px] px-4 py-2 text-sm",
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
      updateComposeBody(editor.getHTML(), editor.getText());
    },
  });

  // Update editor content when draft changes externally (e.g., loading a saved draft)
  useEffect(() => {
    // Skip if this was triggered by the editor itself
    if (isEditorUpdateRef.current) {
      isEditorUpdateRef.current = false;
      return;
    }
    if (editor && draft.bodyHtml !== editor.getHTML()) {
      editor.commands.setContent(draft.bodyHtml || "");
    }
  }, [draft.bodyHtml, editor]);

  const handleClose = useCallback(async () => {
    await closeCompose();
    queryClient.invalidateQueries({ queryKey: emailKeys.folders() });
    queryClient.invalidateQueries({ queryKey: [...emailKeys.all, 'counts'] });
  }, [closeCompose, queryClient]);

  const addLink = useCallback(() => {
    if (!editor) return;
    const url = window.prompt("Enter URL:");
    if (url) {
      editor.chain().focus().setLink({ href: url }).run();
    }
  }, [editor]);

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

    if (draft.to.length === 0) {
      toast.error("Please add at least one recipient");
      return;
    }

    let uploadAttachments: EmailAttachmentUpload[] | undefined;
    if (attachments.length > 0) {
      try {
        uploadAttachments = await filesToBase64(attachments);
      } catch {
        toast.error("Failed to read attachment files");
        return;
      }
    }

    toast.promise(
      sendComposedEmail(uploadAttachments).then(() => {
        setAttachments([]);
        queryClient.invalidateQueries({ queryKey: emailKeys.folders() });
        queryClient.invalidateQueries({ queryKey: [...emailKeys.all, 'counts'] });
      }),
      {
        loading: "Sending...",
        success: "Email sent",
        error: (err) => err?.message || "Failed to send email",
      }
    );
  }, [sendComposedEmail, draft.to.length, isSending, attachments, filesToBase64, queryClient]);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (!isOpen) return;

      // Cmd/Ctrl + Enter to send
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
        e.preventDefault();
        handleSend();
      }

      // Escape to minimize
      if (e.key === "Escape" && !isMinimized) {
        e.preventDefault();
        toggleComposeMinimize();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [isOpen, isMinimized, handleSend, toggleComposeMinimize]);

  return createPortal(
    <AnimatePresence>
      {isOpen && (
      <motion.div
        key="compose-modal"
        initial={{ opacity: 0, y: 100, scale: 0.95 }}
        animate={{
          opacity: 1,
          y: 0,
          scale: 1,
          height: isMinimized ? 48 : "auto",
        }}
        exit={{ opacity: 0, y: 40, scale: 0.97 }}
        transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
        style={{
          position: "fixed",
          bottom: 16,
          right: 16,
          width: COMPOSE_WIDTH,
          maxHeight: isMinimized ? 48 : "calc(100vh - 100px)",
          zIndex: 9999,
        }}
        className="bg-bg-white rounded-lg border border-border-gray shadow-xl overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div
          className="flex items-center justify-between px-4 py-2.5 bg-white cursor-pointer"
          onClick={() => isMinimized && toggleComposeMinimize()}
        >
          <span className="text-sm font-medium text-text-body">
            {isMinimized && draft.subject ? draft.subject : "New Message"}
          </span>
          <div className="flex items-center gap-1">
            <button
              onClick={(e) => {
                e.stopPropagation();
                toggleComposeMinimize();
              }}
              className="p-1.5 text-text-secondary hover:text-text-body hover:bg-bg-gray-dark/50 rounded transition-colors"
            >
              {isMinimized ? <ChevronUpIcon className="w-4 h-4" /> : <MinusIcon className="w-4 h-4" />}
            </button>
            <button
              onClick={(e) => {
                e.stopPropagation();
                handleClose();
              }}
              className="p-1.5 text-text-secondary hover:text-text-body hover:bg-bg-gray-dark/50 rounded transition-colors"
            >
              <XMarkIcon className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Body - hidden when minimized */}
        {!isMinimized && (
          <>
            {/* Error message */}
            {sendError && (
              <div className="px-4 py-2 bg-red-50 border-b border-red-200 text-red-600 text-sm">
                {sendError}
              </div>
            )}

            {/* Recipients */}
            <div>
              <div className="flex items-start">
                <div className="flex-1 min-w-0">
                  <ChipInput
                    label="To"
                    value={draft.to}
                    onChange={(value) => updateComposeDraft("to", value)}
                    placeholder=""
                    onTabToNext={() => {
                      if (showCc) ccInputRef.current?.focus();
                      else if (showBcc) bccInputRef.current?.focus();
                      else subjectInputRef.current?.focus();
                    }}
                  />
                </div>
                <div className="flex items-center gap-1 pr-4 pt-2 shrink-0">
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
                      onChange={(value) => updateComposeDraft("cc", value)}
                      placeholder=""
                      onTabToNext={() => {
                        if (showBcc) bccInputRef.current?.focus();
                        else subjectInputRef.current?.focus();
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
                      onChange={(value) => updateComposeDraft("bcc", value)}
                      placeholder=""
                      onTabToNext={() => subjectInputRef.current?.focus()}
                    />
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
            <div className="mx-4 border-t border-border-gray" />

            {/* Subject */}
            <div className="flex items-center px-4 py-2 gap-2">
              <input
                ref={subjectInputRef}
                type="text"
                value={draft.subject}
                onChange={(e) => updateComposeDraft("subject", e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Tab' && !e.shiftKey) {
                    e.preventDefault();
                    editor?.commands.focus();
                  }
                }}
                placeholder="Subject"
                className="flex-1 text-sm text-text-body bg-transparent outline-none placeholder:text-text-tertiary"
              />
            </div>
            <div className="mx-4 border-t border-border-gray" />

            {/* Editor with drag-and-drop */}
            <div
              className={`flex-1 overflow-y-auto min-h-[200px] max-h-[300px] relative ${isDragOver ? "ring-2 ring-inset ring-brand-primary/40 bg-brand-primary/5" : ""}`}
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

            {/* Formatting toolbar */}
            <div className="flex items-center gap-1 px-3 py-1.5 bg-white">
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
                onClick={() =>
                  editor?.chain().focus().toggleOrderedList().run()
                }
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

            {/* Footer */}
            <div className="flex items-center justify-between px-4 py-3 bg-brand-secondary">
              <div className="flex items-center gap-2">
                <button
                  onClick={handleSend}
                  disabled={isSending}
                  className="flex items-center gap-2 px-4 py-2 bg-brand-primary text-text-light rounded-lg text-sm font-medium hover:opacity-90 transition-opacity disabled:opacity-50"
                >
                  <PaperAirplaneIcon className="w-4 h-4" />
                  {isSending ? 'Sending...' : 'Send'}
                </button>
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="p-2 text-text-secondary hover:text-text-body hover:bg-black/5 rounded-lg transition-colors"
                  title="Attach files"
                >
                  <PaperClipIcon className="w-4.5 h-4.5" />
                </button>
              </div>
              <button
                onClick={() => {
                  setAttachments([]);
                  discardCompose();
                  queryClient.invalidateQueries({ queryKey: emailKeys.folders() });
                  queryClient.invalidateQueries({ queryKey: [...emailKeys.all, 'counts'] });
                }}
                className="text-sm text-text-secondary hover:text-red-500 transition-colors"
              >
                Discard
              </button>
            </div>
          </>
        )}
      </motion.div>
      )}
    </AnimatePresence>,
    document.body,
  );
}
