import { useRef, useEffect, useCallback, useState } from 'react';
import { ArrowUpIcon, PlusIcon, StopIcon, XMarkIcon, AtSymbolIcon } from '@heroicons/react/24/solid';
import type { PendingAttachment } from '../../hooks/useChatAttachments';
import type { MentionData } from '../../types/mention';
import { MENTION_ICONS } from '../../types/mention';
import { UniversalMentionAutocomplete } from '../Mentions/UniversalMentionAutocomplete';
import { getTextareaCursorCoords } from '../../lib/textareaCaret';

interface SidebarChatInputProps {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  onStop?: () => void;
  onAddFiles?: (files: File[]) => void;
  onRemoveAttachment?: (id: string) => void;
  pendingAttachments?: PendingAttachment[];
  isUploading?: boolean;
  disabled?: boolean;
  isStreaming?: boolean;
  placeholder?: string;
  // Mention props
  mentions?: MentionData[];
  onMentionSelect?: (data: MentionData) => void;
  onRemoveMention?: (entityId: string) => void;
  workspaceId?: string | null;
}

export default function SidebarChatInput({
  value,
  onChange,
  onSubmit,
  onStop,
  onAddFiles,
  onRemoveAttachment,
  pendingAttachments = [],
  isUploading = false,
  disabled = false,
  isStreaming = false,
  placeholder = 'Ask anything...',
  mentions = [],
  onMentionSelect,
  onRemoveMention,
  workspaceId,
}: SidebarChatInputProps) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [showMentionAutocomplete, setShowMentionAutocomplete] = useState(false);
  const [mentionQuery, setMentionQuery] = useState('');
  const [mentionCursorCoords, setMentionCursorCoords] = useState<{ top: number; bottom: number; left: number } | null>(null);

  // Auto-resize textarea without causing layout flash
  const adjustHeight = useCallback(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    // Reset to minimum to get accurate scrollHeight
    textarea.style.height = '20px';
    const newHeight = Math.min(Math.max(textarea.scrollHeight, 20), 120);
    textarea.style.height = `${newHeight}px`;
  }, []);

  useEffect(() => {
    adjustHeight();
  }, [value, adjustHeight]);

  const handleChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const newValue = e.target.value;
    onChange(newValue);

    // Detect @ mention trigger
    const cursorPos = e.target.selectionStart ?? 0;
    const textBeforeCursor = newValue.substring(0, cursorPos);
    const mentionMatch = textBeforeCursor.match(/(^|[\s])@(\w*)$/);

    if (mentionMatch && workspaceId) {
      setMentionQuery(mentionMatch[2]);
      setShowMentionAutocomplete(true);
      const coords = getTextareaCursorCoords(e.target);
      setMentionCursorCoords(coords);
    } else {
      setShowMentionAutocomplete(false);
      setMentionQuery('');
    }
  }, [onChange, workspaceId]);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // When mention autocomplete is open, let it handle nav keys
    if (showMentionAutocomplete) {
      if (['ArrowDown', 'ArrowUp', 'ArrowRight', 'ArrowLeft', 'Enter', 'Tab', 'Escape'].includes(e.key)) {
        if (e.key === 'Enter' || e.key === 'Tab') {
          e.preventDefault();
        }
        return;
      }
    }

    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (canSubmit) {
        onSubmit();
      }
    }
  };

  const handleSubmit = () => {
    if (canSubmit) {
      onSubmit();
    }
  };

  const handleMentionSelect = useCallback((data: MentionData) => {
    const textarea = textareaRef.current;
    if (!textarea) return;

    const cursorPos = textarea.selectionStart ?? 0;
    const textBeforeCursor = value.substring(0, cursorPos);
    const mentionMatch = textBeforeCursor.match(/(^|[\s])@(\w*)$/);

    if (mentionMatch) {
      // Remove the @query text — the mention is shown as a pill only
      const prefix = mentionMatch[1]; // leading whitespace or empty
      const start = cursorPos - mentionMatch[0].length;
      const newValue = value.substring(0, start) + prefix + value.substring(cursorPos);

      onChange(newValue);
      onMentionSelect?.(data);

      requestAnimationFrame(() => {
        textarea.selectionStart = start;
        textarea.selectionEnd = start;
        textarea.focus();
      });
    }

    setShowMentionAutocomplete(false);
    setMentionQuery('');
  }, [value, onChange, onMentionSelect]);

  const handleAtButtonClick = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    const textarea = textareaRef.current;
    if (!textarea) return;

    const pos = textarea.selectionStart ?? value.length;
    const newValue = value.substring(0, pos) + '@' + value.substring(pos);
    onChange(newValue);

    requestAnimationFrame(() => {
      textarea.selectionStart = pos + 1;
      textarea.selectionEnd = pos + 1;
      textarea.focus();
      setShowMentionAutocomplete(true);
      setMentionQuery('');
      setMentionCursorCoords(getTextareaCursorCoords(textarea));
    });
  }, [value, onChange]);

  const handleAddClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!onAddFiles || !e.target.files?.length) return;
    onAddFiles(Array.from(e.target.files));
    e.target.value = '';
  };

  const handlePaste = (e: React.ClipboardEvent) => {
    if (!onAddFiles) return;
    const items = e.clipboardData?.items;
    if (!items) return;

    const imageFiles: File[] = [];
    for (const item of items) {
      if (item.type.startsWith('image/')) {
        const file = item.getAsFile();
        if (file) imageFiles.push(file);
      }
    }

    if (imageFiles.length > 0) {
      e.preventDefault();
      onAddFiles(imageFiles);
    }
  };

  const hasAttachments = pendingAttachments.length > 0;
  const hasMentions = mentions.length > 0;
  const canSubmit = (value.trim().length > 0 || hasAttachments) && !disabled && !isUploading;

  return (
    <>
      <div className="bg-bg-white border border-border-gray rounded-lg overflow-hidden" ref={containerRef}>
        {/* Attachment previews */}
        {hasAttachments && (
          <div className="flex gap-1.5 px-2.5 pt-2">
            {pendingAttachments.map((att) => (
              <div key={att.id} className="relative w-12 h-12 rounded-md overflow-hidden bg-bg-gray shrink-0">
                <img src={att.preview} alt="" className="w-full h-full object-cover" />
                {att.status === 'uploading' && (
                  <div className="absolute inset-0 bg-black/30 flex items-center justify-center">
                    <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  </div>
                )}
                {att.status === 'error' && (
                  <div className="absolute inset-0 bg-red-500/30 flex items-center justify-center">
                    <span className="text-white text-xs">!</span>
                  </div>
                )}
                {onRemoveAttachment && (
                  <button
                    onClick={() => onRemoveAttachment(att.id)}
                    className="absolute top-0.5 right-0.5 w-4 h-4 bg-black/50 hover:bg-black/70 rounded-full flex items-center justify-center transition-colors"
                  >
                    <XMarkIcon className="w-2.5 h-2.5 text-white" />
                  </button>
                )}
              </div>
            ))}
          </div>
        )}

        {/* Mention pills */}
        {hasMentions && (
          <div className="flex flex-wrap gap-1 px-2.5 pt-2">
            {mentions.map((mention) => (
              <span
                key={mention.entityId}
                className="inline-flex items-center gap-0.5 px-1.5 py-0.5 bg-blue-50 text-blue-700 text-xs rounded-full border border-blue-200"
              >
                <span>{mention.icon || MENTION_ICONS[mention.entityType]}</span>
                <span className="font-medium truncate max-w-[100px]">{mention.displayName}</span>
                {onRemoveMention && (
                  <button
                    onClick={(e) => { e.stopPropagation(); onRemoveMention(mention.entityId); }}
                    className="ml-0.5 text-blue-400 hover:text-blue-600"
                  >
                    <XMarkIcon className="w-2.5 h-2.5" />
                  </button>
                )}
              </span>
            ))}
          </div>
        )}

        {/* Input row */}
        <div className="flex items-center gap-1 px-1.5 min-h-[46px] py-2">
          <button
            type="button"
            onClick={handleAddClick}
            className="w-6 h-6 flex items-center justify-center rounded-full shrink-0 text-text-tertiary hover:text-text-body hover:bg-bg-gray-dark transition-colors"
            title="Add image"
          >
            <PlusIcon className="w-4 h-4" />
          </button>
          {workspaceId && (
            <button
              type="button"
              onClick={handleAtButtonClick}
              className="w-6 h-6 flex items-center justify-center rounded-full shrink-0 text-text-tertiary hover:text-text-body hover:bg-bg-gray-dark transition-colors"
              title="Mention"
            >
              <AtSymbolIcon className="w-4 h-4" />
            </button>
          )}
          <input
            ref={fileInputRef}
            type="file"
            accept="image/jpeg,image/png,image/heic,image/heif"
            multiple
            className="hidden"
            onChange={handleFileChange}
          />
          <textarea
            ref={textareaRef}
            value={value}
            onChange={handleChange}
            onKeyDown={handleKeyDown}
            onPaste={handlePaste}
            placeholder={placeholder}
            rows={1}
            className="flex-1 min-w-0 bg-transparent resize-none outline-none text-text-body placeholder-text-tertiary text-sm leading-5 h-5 min-h-[20px] max-h-[120px]"
          />
          {isStreaming ? (
            <button
              onClick={onStop}
              className="w-7 h-7 flex items-center justify-center rounded-full shrink-0 transition-all duration-200 bg-brand-primary text-text-light hover:opacity-90"
              title="Stop generating"
            >
              <StopIcon className="w-3.5 h-3.5" />
            </button>
          ) : (
            <button
              onClick={handleSubmit}
              disabled={!canSubmit}
              className={`w-7 h-7 flex items-center justify-center rounded-full shrink-0 transition-all duration-200 ${
                canSubmit
                  ? 'bg-brand-primary text-text-light hover:opacity-90'
                  : 'bg-border-gray text-text-tertiary cursor-not-allowed'
              }`}
            >
              <ArrowUpIcon className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>

      {/* Mention autocomplete dropdown (renders via portal) */}
      {showMentionAutocomplete && workspaceId && (
        <UniversalMentionAutocomplete
          query={mentionQuery}
          workspaceId={workspaceId}
          onSelect={handleMentionSelect}
          onClose={() => {
            setShowMentionAutocomplete(false);
            setMentionQuery('');
          }}
          anchorRef={containerRef}
          position="above"
          cursorCoords={mentionCursorCoords || undefined}
        />
      )}
    </>
  );
}
