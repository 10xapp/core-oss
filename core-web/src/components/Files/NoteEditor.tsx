import { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useParams } from 'react-router-dom';
import { useEditor, EditorContent, Editor } from '@tiptap/react';
import StarterKit from '@tiptap/starter-kit';
import Placeholder from '@tiptap/extension-placeholder';
import { Markdown } from '@tiptap/markdown';
import { EditorState } from '@tiptap/pm/state';
import { Table, renderTableToMarkdown } from '@tiptap/extension-table';
import { TableRow } from '@tiptap/extension-table-row';
import { TableCell } from '@tiptap/extension-table-cell';
import { TableHeader } from '@tiptap/extension-table-header';
import { TaskList } from '@tiptap/extension-task-list';
import { TaskItem } from '@tiptap/extension-task-item';
import { Link } from '@tiptap/extension-link';
import { Underline } from '@tiptap/extension-underline';
import { Highlight } from '@tiptap/extension-highlight';
import Image from '@tiptap/extension-image';
import { HugeiconsIcon } from '@hugeicons/react';
import {
  LeftToRightListBulletIcon,
  LeftToRightListNumberIcon,
  QuoteDownIcon,
  SourceCodeIcon,
  Download04Icon,
  CheckListIcon,
  TextUnderlineIcon,
  HighlighterIcon,
  Link01Icon,
  Clock01Icon,
  Minimize01Icon,
  Maximize01Icon,
} from '@hugeicons-pro/core-stroke-standard';
import { useUIStore } from '../../stores/uiStore';
import { UniversalMentionMark } from '../Mentions/MentionMark';
import { UniversalMentionAutocomplete } from '../Mentions/UniversalMentionAutocomplete';
import { MENTION_ICONS } from '../../types/mention';
import type { MentionData } from '../../types/mention';

// Extend Table so its markdown serializer never emits &nbsp;.
// TipTap injects non-breaking spaces in empty table cells for DOM layout;
// the default serializer passes them through as literal "&nbsp;" in the
// markdown output.  This override strips them at the source so downstream
// code never has to worry about round-trip mismatches.
const CleanTable = Table.extend({
  renderMarkdown(node, h) {
    return renderTableToMarkdown(node, h).replace(/&nbsp;/g, ' ');
  },
});

interface NoteEditorProps {
  content: string;
  onChange: (markdown: string) => void;
  placeholder?: string;
  autoFocus?: boolean;
  onEditorReady?: (editor: Editor | null) => void;
  editable?: boolean;
  onImageUpload?: (file: File) => Promise<string | null>;
  onBackspaceAtStart?: () => void;
  onArrowUpAtStart?: (pixelX: number) => void;
}

// Detect stored content format and normalize for the editor.
// Returns the content string and whether it's HTML (legacy) or markdown (new).
function normalizeContent(content: string): { content: string; isHtml: boolean } {
  if (!content || !content.trim()) {
    return { content: '', isHtml: false };
  }

  // Legacy JSON blocks format -> convert to HTML for TipTap to parse
  if (content.trim().startsWith('[')) {
    try {
      const blocks = JSON.parse(content);
      const html = blocks
        .map((block: { type: string; content?: string }) => {
          const text = block.content || '';
          const htmlContent = text.split('\n').map((line: string) => line || '<br>').join('<br>');
          return `<p>${htmlContent}</p>`;
        })
        .join('');
      return { content: html, isHtml: true };
    } catch {
      // Not valid JSON, fall through
    }
  }

  // HTML detection: look for common HTML tags
  const htmlTagPattern = /<(?:p|h[1-6]|div|span|ul|ol|li|blockquote|pre|code|strong|em|br|hr|a)\b[^>]*>/i;
  if (htmlTagPattern.test(content)) {
    return { content, isHtml: true };
  }

  // Already markdown
  return { content, isHtml: false };
}

// Left side formatting toolbar - moves with content width
export function NoteToolbar({
  editor,
  isHidden,
}: {
  editor: Editor | null;
  isHidden?: boolean;
}) {
  const [showTextFormatting, setShowTextFormatting] = useState(false);
  const [showHighlightMenu, setShowHighlightMenu] = useState(false);
  const highlightButtonRef = useRef<HTMLButtonElement>(null);
  const [linkPopover, setLinkPopover] = useState<{ x: number; y: number } | null>(null);
  const [linkUrl, setLinkUrl] = useState('');
  const linkInputRef = useRef<HTMLInputElement>(null);

  // Focus the link input when the popover opens
  useEffect(() => {
    if (linkPopover) {
      // Small delay to ensure the input is rendered
      requestAnimationFrame(() => linkInputRef.current?.focus());
    }
  }, [linkPopover]);

  const openLinkPopover = () => {
    if (!editor) return;
    const { view, state } = editor;
    const { from } = state.selection;
    const coords = view.coordsAtPos(from);
    setLinkUrl(editor.getAttributes('link').href || '');
    setLinkPopover({ x: coords.left, y: coords.bottom + 4 });
  };

  const applyLink = () => {
    if (linkUrl) {
      editor?.chain().focus().setLink({ href: linkUrl }).run();
    }
    setLinkPopover(null);
    setLinkUrl('');
  };

  const cancelLink = () => {
    setLinkPopover(null);
    setLinkUrl('');
    editor?.chain().focus().run();
  };

  if (!editor) return null;

  const ToolbarButton = ({
    onAction,
    isActive,
    children,
    title,
  }: {
    onAction: () => void;
    isActive?: boolean;
    children: React.ReactNode;
    title: string;
  }) => (
    <button
      onMouseDown={(e) => {
        e.preventDefault();
        onAction();
      }}
      title={title}
      className={`p-1.5 rounded transition-colors ${
        isActive
          ? 'bg-bg-gray-dark text-text-body'
          : 'text-text-secondary hover:text-text-body hover:bg-bg-gray'
      }`}
    >
      {children}
    </button>
  );

  return (
    <div className={`flex items-center py-2 shrink-0 -ml-1.5 transition-opacity duration-[600ms] ${isHidden ? 'opacity-0' : 'opacity-100'}`}>
      <div className="flex items-center gap-1">
        {/* Text formatting toggle button */}
        <button
          onClick={() => setShowTextFormatting(!showTextFormatting)}
          title="Text formatting"
          className="p-1.5 rounded transition-colors text-sm font-medium text-text-secondary hover:text-text-body hover:bg-bg-gray"
        >
          Aa
        </button>

        {/* Expandable text formatting options */}
        <div
          className={`flex items-center gap-1 overflow-hidden transition-all duration-200 ease-out ${
            showTextFormatting ? 'max-w-[300px] opacity-100' : 'max-w-0 opacity-0'
          }`}
        >
          <div className="w-px h-4 bg-border-gray mx-1" />
          <ToolbarButton
            onAction={() => editor.chain().focus().toggleHeading({ level: 1 }).run()}
            isActive={editor.isActive('heading', { level: 1 })}
            title="Heading 1"
          >
            <span className="w-4 h-4 flex items-center justify-center text-xs font-bold">H1</span>
          </ToolbarButton>
          <ToolbarButton
            onAction={() => editor.chain().focus().toggleHeading({ level: 2 }).run()}
            isActive={editor.isActive('heading', { level: 2 })}
            title="Heading 2"
          >
            <span className="w-4 h-4 flex items-center justify-center text-xs font-bold">H2</span>
          </ToolbarButton>
          <div className="w-px h-4 bg-border-gray mx-1" />
          <ToolbarButton
            onAction={() => editor.chain().focus().toggleBold().run()}
            isActive={editor.isActive('bold')}
            title="Bold"
          >
            <span className="w-4 h-4 flex items-center justify-center font-bold text-sm">B</span>
          </ToolbarButton>
          <ToolbarButton
            onAction={() => editor.chain().focus().toggleItalic().run()}
            isActive={editor.isActive('italic')}
            title="Italic"
          >
            <span className="w-4 h-4 flex items-center justify-center italic text-sm">I</span>
          </ToolbarButton>
          <ToolbarButton
            onAction={() => editor.chain().focus().toggleStrike().run()}
            isActive={editor.isActive('strike')}
            title="Strikethrough"
          >
            <span className="w-4 h-4 flex items-center justify-center line-through text-sm">S</span>
          </ToolbarButton>
          <ToolbarButton
            onAction={() => editor.chain().focus().toggleUnderline().run()}
            isActive={editor.isActive('underline')}
            title="Underline"
          >
            <HugeiconsIcon icon={TextUnderlineIcon} size={16} />
          </ToolbarButton>
          <button
            ref={highlightButtonRef}
            onMouseDown={(e) => {
              e.preventDefault();
              setShowHighlightMenu(!showHighlightMenu);
            }}
            title="Highlight"
            className={`p-1.5 rounded transition-colors ${
              editor.isActive('highlight')
                ? 'bg-bg-gray-dark text-text-body'
                : 'text-text-secondary hover:text-text-body hover:bg-bg-gray'
            }`}
          >
            <HugeiconsIcon icon={HighlighterIcon} size={16} />
          </button>
          <ToolbarButton
            onAction={() => {
              if (editor.isActive('link')) {
                editor.chain().focus().unsetLink().run();
              } else {
                openLinkPopover();
              }
            }}
            isActive={editor.isActive('link')}
            title="Link"
          >
            <HugeiconsIcon icon={Link01Icon} size={16} />
          </ToolbarButton>
        </div>

        <div className="w-px h-4 bg-border-gray mx-1" />
        <ToolbarButton
          onAction={() => editor.chain().focus().toggleBulletList().run()}
          isActive={editor.isActive('bulletList')}
          title="Bullet List"
        >
          <HugeiconsIcon icon={LeftToRightListBulletIcon} size={16} />
        </ToolbarButton>
        <ToolbarButton
          onAction={() => editor.chain().focus().toggleOrderedList().run()}
          isActive={editor.isActive('orderedList')}
          title="Numbered List"
        >
          <HugeiconsIcon icon={LeftToRightListNumberIcon} size={16} />
        </ToolbarButton>
        <div className="w-px h-4 bg-border-gray mx-1" />
        <ToolbarButton
          onAction={() => editor.chain().focus().toggleBlockquote().run()}
          isActive={editor.isActive('blockquote')}
          title="Quote"
        >
          <HugeiconsIcon icon={QuoteDownIcon} size={16} />
        </ToolbarButton>
        <ToolbarButton
          onAction={() => editor.chain().focus().toggleCode().run()}
          isActive={editor.isActive('code')}
          title="Code"
        >
          <HugeiconsIcon icon={SourceCodeIcon} size={16} />
        </ToolbarButton>
        <div className="w-px h-4 bg-border-gray mx-1" />
        <ToolbarButton
          onAction={() => editor.chain().focus().toggleTaskList().run()}
          isActive={editor.isActive('taskList')}
          title="Task List"
        >
          <HugeiconsIcon icon={CheckListIcon} size={16} />
        </ToolbarButton>
        <ToolbarButton
          onAction={() => editor.chain().focus().insertTable({ rows: 3, cols: 3, withHeaderRow: true }).run()}
          isActive={editor.isActive('table')}
          title="Insert Table"
        >
          <span className="w-4 h-4 flex items-center justify-center">
            <svg width="16" height="13" viewBox="0 0 24 19" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round">
              <rect x="1" y="1" width="22" height="17" rx="2" />
              <line x1="12" y1="1" x2="12" y2="18" />
              <line x1="1" y1="9.5" x2="23" y2="9.5" />
            </svg>
          </span>
        </ToolbarButton>
      </div>

      {/* Highlight color picker */}
      {showHighlightMenu && highlightButtonRef.current && createPortal(
        <>
          <div className="fixed inset-0 z-[9998]" onClick={() => setShowHighlightMenu(false)} />
          <div
            className="fixed z-[9999] bg-white border border-border-gray rounded-lg shadow-lg p-1.5 flex items-center gap-1"
            style={{
              left: highlightButtonRef.current.getBoundingClientRect().left + highlightButtonRef.current.getBoundingClientRect().width / 2,
              top: highlightButtonRef.current.getBoundingClientRect().bottom + 4,
              transform: 'translateX(-50%)',
            }}
          >
            {[
              { color: '#fef08a', label: 'Yellow' },
              { color: '#bbf7d0', label: 'Green' },
              { color: '#bfdbfe', label: 'Blue' },
              { color: '#e9d5ff', label: 'Purple' },
              { color: '#fecdd3', label: 'Pink' },
              { color: '#fed7aa', label: 'Orange' },
            ].map(({ color, label }) => (
              <button
                key={color}
                title={label}
                onMouseDown={(e) => {
                  e.preventDefault();
                  if (editor.isActive('highlight', { color })) {
                    editor.chain().focus().unsetHighlight().run();
                  } else {
                    editor.chain().focus().setHighlight({ color }).run();
                  }
                  setShowHighlightMenu(false);
                }}
                className={`w-5 h-5 rounded-full border transition-transform hover:scale-110 ${
                  editor.isActive('highlight', { color }) ? 'border-text-body ring-1 ring-text-body' : 'border-black/10'
                }`}
                style={{ backgroundColor: color }}
              />
            ))}
            {editor.isActive('highlight') && (
              <button
                title="Remove highlight"
                onMouseDown={(e) => {
                  e.preventDefault();
                  editor.chain().focus().unsetHighlight().run();
                  setShowHighlightMenu(false);
                }}
                className="w-5 h-5 rounded-full border border-border-gray flex items-center justify-center text-text-tertiary hover:text-text-body hover:border-text-body transition-colors text-xs"
              >
                ✕
              </button>
            )}
          </div>
        </>,
        document.body
      )}

      {/* Link URL popover */}
      {linkPopover && createPortal(
        <>
          <div className="fixed inset-0 z-[9998]" onClick={cancelLink} />
          <div
            className="fixed z-[9999] bg-bg-base border border-border-gray rounded-lg shadow-lg p-2 flex items-center gap-2"
            style={{ left: linkPopover.x, top: linkPopover.y }}
          >
            <input
              ref={linkInputRef}
              type="url"
              value={linkUrl}
              onChange={(e) => setLinkUrl(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  applyLink();
                } else if (e.key === 'Escape') {
                  cancelLink();
                }
              }}
              placeholder="https://..."
              className="bg-white border border-border-light rounded-md px-2 py-1 text-sm w-64 outline-none focus:border-text-tertiary"
            />
            <button
              onClick={applyLink}
              className="px-2.5 py-1 text-sm bg-black text-white rounded-md hover:bg-gray-800 transition-colors"
            >
              OK
            </button>
          </div>
        </>,
        document.body
      )}
    </div>
  );
}

// Right side toolbar buttons - stays fixed on right
export function NoteToolbarRight({
  dateLabel,
  saveStatus,
  isFullWidth,
  onToggleFullWidth,
  onDownload,
}: {
  dateLabel?: string;
  saveStatus?: 'saved' | 'saving' | 'unsaved' | 'error';
  isFullWidth?: boolean;
  onToggleFullWidth?: () => void;
  onDownload?: () => void;
}) {
  const isVersionHistoryOpen = useUIStore((s) => s.isVersionHistoryOpen);
  const toggleVersionHistory = useUIStore((s) => s.toggleVersionHistory);

  return (
    <div className="flex items-center gap-1">
      <button
        onClick={toggleVersionHistory}
        title="Version history"
        className={`p-1.5 rounded transition-colors ${
          isVersionHistoryOpen
            ? 'text-text-body bg-bg-gray-dark'
            : 'text-text-tertiary hover:text-text-body hover:bg-bg-gray'
        }`}
      >
        <HugeiconsIcon icon={Clock01Icon} size={16} />
      </button>
      {onDownload && (
        <button
          onClick={onDownload}
          title="Download as .md"
          className="p-1.5 rounded transition-colors text-text-tertiary hover:text-text-body hover:bg-bg-gray"
        >
          <HugeiconsIcon icon={Download04Icon} size={16} />
        </button>
      )}
      {onToggleFullWidth && (
        <button
          onClick={onToggleFullWidth}
          title={isFullWidth ? "Narrow width" : "Full width"}
          className="p-1.5 rounded transition-colors text-text-tertiary hover:text-text-body hover:bg-bg-gray"
        >
          <HugeiconsIcon icon={isFullWidth ? Minimize01Icon : Maximize01Icon} size={16} />
        </button>
      )}
      <div className="flex items-center gap-2 text-xs text-text-tertiary ml-2">
        {dateLabel && <span>{dateLabel}</span>}
        {saveStatus === 'saving' && <span className="text-text-tertiary">· Saving...</span>}
        {saveStatus === 'unsaved' && <span className="text-text-secondary">· Unsaved</span>}
        {saveStatus === 'error' && <span className="text-red-500">· Save failed</span>}
        {saveStatus === 'saved' && dateLabel && <span className="text-text-tertiary">· Saved</span>}
      </div>
    </div>
  );
}

function TableFloatingToolbar({ editor }: { editor: Editor }) {
  const [position, setPosition] = useState<{ top: number; left: number } | null>(null);
  const toolbarRef = useRef<HTMLDivElement>(null);

  const updatePosition = useCallback(() => {
    // Guard: editor may be destroyed or view not available
    if (editor.isDestroyed || !editor.view) {
      setPosition(null);
      return;
    }

    if (!editor.isActive('table')) {
      setPosition(null);
      return;
    }

    // Find the table DOM node from the current selection
    const { $from } = editor.state.selection;
    let depth = $from.depth;
    while (depth > 0 && $from.node(depth).type.name !== 'table') {
      depth--;
    }
    if (depth === 0) {
      setPosition(null);
      return;
    }

    try {
      const tablePos = $from.before(depth);
      let dom = editor.view.nodeDOM(tablePos);
      if (!(dom instanceof HTMLElement)) {
        setPosition(null);
        return;
      }
      // Ensure we have the <table> element itself, not a wrapper
      const tableEl = dom.tagName === 'TABLE' ? dom : dom.querySelector('table');
      if (!tableEl) {
        setPosition(null);
        return;
      }

      const toolbarWidth = toolbarRef.current?.offsetWidth ?? 320;
      const tableBottom = tableEl.offsetTop + tableEl.offsetHeight;
      const tableCenterLeft = tableEl.offsetLeft + tableEl.offsetWidth / 2 - toolbarWidth / 2;

      setPosition({
        top: tableBottom + 8,
        left: Math.max(0, tableCenterLeft),
      });
    } catch {
      setPosition(null);
    }
  }, [editor]);

  useEffect(() => {
    const handler = () => updatePosition();
    editor.on('selectionUpdate', handler);
    editor.on('transaction', handler);
    return () => {
      editor.off('selectionUpdate', handler);
      editor.off('transaction', handler);
    };
  }, [editor, updatePosition]);

  if (!position || !editor.isActive('table')) return null;

  const btn = (label: string, title: string, action: () => void) => (
    <button
      type="button"
      title={title}
      onMouseDown={(e) => {
        e.preventDefault();
        action();
      }}
      className="px-1.5 py-1 text-xs rounded hover:bg-black/10 text-text-secondary hover:text-text-body whitespace-nowrap transition-colors"
    >
      {label}
    </button>
  );

  const divider = <div className="w-px h-4 bg-border-gray shrink-0" />;

  return (
    <div
      ref={toolbarRef}
      className="absolute z-50 flex items-center gap-0.5 bg-white border border-border-gray rounded-lg shadow-md px-0.5 py-0.5"
      style={{ top: position.top, left: position.left }}
    >
      {btn('+ Row ↑', 'Add row above', () => editor.chain().focus().addRowBefore().run())}
      {btn('+ Row ↓', 'Add row below', () => editor.chain().focus().addRowAfter().run())}
      {divider}
      {btn('+ Col ←', 'Add column left', () => editor.chain().focus().addColumnBefore().run())}
      {btn('+ Col →', 'Add column right', () => editor.chain().focus().addColumnAfter().run())}
      {divider}
      {btn('− Row', 'Delete row', () => editor.chain().focus().deleteRow().run())}
      {btn('− Col', 'Delete column', () => editor.chain().focus().deleteColumn().run())}
      {divider}
      {btn('🗑 Table', 'Delete table', () => editor.chain().focus().deleteTable().run())}
    </div>
  );
}

export default function NoteEditor({ content, onChange, placeholder = 'Start writing...', autoFocus = false, onEditorReady, editable = true, onImageUpload, onBackspaceAtStart, onArrowUpAtStart }: NoteEditorProps) {
  const { workspaceId } = useParams<{ workspaceId: string }>();
  const { content: initialContent, isHtml: initialIsHtml } = normalizeContent(content);
  // Track when we're loading content (switching notes) to avoid triggering saves
  const isLoadingRef = useRef(false);
  // Track the last loaded content to detect actual changes
  const lastLoadedContentRef = useRef(initialContent);
  // Mention autocomplete state
  const [showMentionAutocomplete, setShowMentionAutocomplete] = useState(false);
  const [mentionQuery, setMentionQuery] = useState('');
  const [mentionCursorCoords, setMentionCursorCoords] = useState<{ top: number; bottom: number; left: number } | undefined>();
  const mentionAnchorRef = useRef<HTMLDivElement>(null);
  // Stable refs for drop/paste handlers (avoids stale closures in editorProps)
  const onImageUploadRef = useRef(onImageUpload);
  onImageUploadRef.current = onImageUpload;
  const onBackspaceAtStartRef = useRef(onBackspaceAtStart);
  onBackspaceAtStartRef.current = onBackspaceAtStart;
  const onArrowUpAtStartRef = useRef(onArrowUpAtStart);
  onArrowUpAtStartRef.current = onArrowUpAtStart;
  const editorRef = useRef<Editor | null>(null);
  // Track blob URLs being uploaded so saves can be skipped until resolved
  const pendingBlobUrls = useRef<Set<string>>(new Set());

  // Insert image instantly with blob URL, upload in background, then swap to real URL
  const insertImageOptimistic = useCallback((file: File, alt: string, upload: (file: File) => Promise<string | null>) => {
    const ed = editorRef.current;
    if (!ed || ed.isDestroyed) return;
    const blobUrl = URL.createObjectURL(file);
    pendingBlobUrls.current.add(blobUrl);
    ed.chain().focus().setImage({ src: blobUrl, alt }).run();
    upload(file).then(url => {
      pendingBlobUrls.current.delete(blobUrl);
      if (!ed.isDestroyed && url) {
        // Replace blob URL with real URL across all image nodes
        const { tr, doc } = ed.state;
        doc.descendants((node, pos) => {
          if (node.type.name === 'image' && node.attrs.src === blobUrl) {
            tr.setNodeMarkup(pos, undefined, { ...node.attrs, src: url });
          }
        });
        if (tr.docChanged) ed.view.dispatch(tr);
      } else if (!ed.isDestroyed) {
        // Upload failed — remove the placeholder image
        const { tr, doc } = ed.state;
        doc.descendants((node, pos) => {
          if (node.type.name === 'image' && node.attrs.src === blobUrl) {
            tr.delete(pos, pos + node.nodeSize);
          }
        });
        if (tr.docChanged) ed.view.dispatch(tr);
      }
      URL.revokeObjectURL(blobUrl);
    });
  }, []);

  const editor = useEditor({
    extensions: [
      StarterKit.configure({
        heading: {
          levels: [1, 2, 3],
        },
        link: false,
        underline: false,
      }),
      CleanTable.configure({
        resizable: true,
        HTMLAttributes: { class: 'tiptap-table' },
      }),
      TableRow,
      TableHeader,
      TableCell,
      TaskList,
      TaskItem.configure({
        nested: true,
      }),
      Link.configure({
        openOnClick: false,
        HTMLAttributes: { class: 'tiptap-link', target: '_blank', rel: 'noopener noreferrer' },
      }),
      Underline,
      Highlight.configure({
        multicolor: true,
      }),
      Image.configure({
        inline: false,
        allowBase64: false,
      }),
      UniversalMentionMark,
      Placeholder.configure({
        placeholder,
      }),
      Markdown,
    ],
    editable,
    content: initialContent,
    // If the initial content is markdown (not HTML), tell the extension to parse it
    ...(initialIsHtml ? {} : { contentType: 'markdown' as const }),
    onUpdate: ({ editor }) => {
      // Skip onChange during content loading (switching notes)
      if (isLoadingRef.current) return;
      // Skip save while images are still uploading (blob: URLs in content)
      if (pendingBlobUrls.current.size > 0) return;

      // Strip &nbsp; that TipTap injects in non-paragraph contexts (e.g. empty
      // list items, table cells) but preserve standalone &nbsp; lines which
      // represent intentional blank paragraphs.
      const currentMarkdown = editor.getMarkdown().replace(/^(?!&nbsp;$)(.*)&nbsp;/gm, '$1 ');

      // Guard: never save empty content if the note previously had content.
      // This prevents data loss when extensions fail to parse existing content.
      if (!currentMarkdown.trim() && lastLoadedContentRef.current.trim()) {
        return;
      }

      if (currentMarkdown !== lastLoadedContentRef.current) {
        // Update ref BEFORE calling onChange so the useEffect that fires
        // when the parent passes this content back as a prop will see it
        // matches and skip the destructive EditorState reset.
        lastLoadedContentRef.current = currentMarkdown;
        onChange(currentMarkdown);
      }

      // Mention detection
      const { from } = editor.state.selection;
      const textBefore = editor.state.doc.textBetween(
        Math.max(0, from - 50),
        from,
        '\n',
      );
      const mentionMatch = textBefore.match(/(^|[\s])@(\w*)$/);
      if (mentionMatch) {
        setMentionQuery(mentionMatch[2]);
        setShowMentionAutocomplete(true);
        // Compute cursor position for dropdown placement
        try {
          const coords = editor.view.coordsAtPos(from);
          setMentionCursorCoords({ top: coords.top, bottom: coords.bottom, left: coords.left });
        } catch {
          // coordsAtPos can fail if pos is out of view
        }
      } else {
        setShowMentionAutocomplete(false);
        setMentionQuery('');
        if (editor.isActive('mention')) {
          editor.commands.unsetMark('mention');
        }
      }
    },
    editorProps: {
      attributes: {
        class: 'tiptap note-editor focus:outline-none min-h-[200px]',
      },
      handleKeyDown: (view, event) => {
        const { from, to } = view.state.selection;
        if (event.key === 'Backspace') {
          // Position 1 = start of first paragraph text (0 is doc node, 1 is inside first <p>)
          if (from <= 1 && to <= 1 && onBackspaceAtStartRef.current) {
            // If the first block is empty, delete it before focusing title
            const firstChild = view.state.doc.firstChild;
            if (firstChild && firstChild.textContent === '' && view.state.doc.childCount > 1) {
              const tr = view.state.tr.delete(0, firstChild.nodeSize);
              view.dispatch(tr);
            }
            onBackspaceAtStartRef.current();
            return true;
          }
        }
        if (event.key === 'ArrowLeft') {
          if (from <= 1 && to <= 1 && onBackspaceAtStartRef.current) {
            onBackspaceAtStartRef.current();
            return true;
          }
        }
        if (event.key === 'ArrowUp') {
          // Only trigger when cursor is on the first line
          const coords = view.coordsAtPos(from);
          const firstLineCoords = view.coordsAtPos(1);
          if (coords.top === firstLineCoords.top && onArrowUpAtStartRef.current) {
            // Compute pixel X offset relative to editor left edge
            const editorLeft = view.dom.getBoundingClientRect().left;
            const pixelX = coords.left - editorLeft;
            onArrowUpAtStartRef.current(pixelX);
            return true;
          }
        }
        return false;
      },
      handleDrop: (_view, event, _slice, moved) => {
        if (moved || !event.dataTransfer?.files.length) return false;
        const images = Array.from(event.dataTransfer.files).filter(f => f.type.startsWith('image/'));
        if (images.length === 0) return false;
        const upload = onImageUploadRef.current;
        if (!upload) return false;
        event.preventDefault();
        for (const file of images) {
          insertImageOptimistic(file, file.name, upload);
        }
        return true;
      },
      handlePaste: (_view, event) => {
        const items = event.clipboardData?.items;
        if (!items) return false;
        const imageItems = Array.from(items).filter(item => item.type.startsWith('image/'));
        if (imageItems.length === 0) return false;
        const upload = onImageUploadRef.current;
        if (!upload) return false;
        event.preventDefault();
        for (const item of imageItems) {
          const file = item.getAsFile();
          if (file) {
            insertImageOptimistic(file, 'Pasted image', upload);
          }
        }
        return true;
      },
    },
  });

  // Keep editor ref in sync for drop/paste handlers
  useEffect(() => {
    editorRef.current = editor;
  }, [editor]);

  // Notify parent when editor is ready
  useEffect(() => {
    onEditorReady?.(editor);
    return () => onEditorReady?.(null);
  }, [editor, onEditorReady]);

  // Sync editable prop to editor (e.g., switching between owned and shared notes)
  useEffect(() => {
    if (editor && !editor.isDestroyed && editor.isEditable !== editable) {
      editor.setEditable(editable);
    }
  }, [editor, editable]);

  // Update editor content when prop changes (e.g., switching notes)
  useEffect(() => {
    if (!editor || editor.isDestroyed) return;

    const { content: normalized, isHtml } = normalizeContent(content);
    // Compare against our last-known content to avoid unnecessary resets.
    // This prevents the destructive EditorState.create() from firing when
    // the parent simply echoes back content that originated from our own
    // onUpdate callback (which would reset cursor position and scroll).
    if (normalized === lastLoadedContentRef.current) return;

    isLoadingRef.current = true;

    if (isHtml) {
      editor.commands.setContent(normalized);
    } else {
      editor.commands.setContent(normalized, { contentType: 'markdown' });
    }

    // Replace editor state to reset undo history — prevents Cmd+Z
    // from restoring the previous note's content when switching notes.
    // Guard: editor.view may not be available if the component hasn't mounted yet.
    try {
      editor.view.updateState(
        EditorState.create({
          doc: editor.state.doc,
          plugins: editor.state.plugins,
        })
      );
    } catch {
      // Editor view not available — skip undo history reset
    }

    lastLoadedContentRef.current = normalized;
    requestAnimationFrame(() => {
      isLoadingRef.current = false;
    });
  }, [content, editor]);

  // Auto-focus editor when requested (e.g., after creating a new note)
  useEffect(() => {
    if (autoFocus && editor) {
      // Small delay to ensure editor is ready
      requestAnimationFrame(() => {
        editor.commands.focus('end');
      });
    }
  }, [autoFocus, editor]);

  const handleMentionSelect = useCallback(
    (data: MentionData) => {
      if (!editor) return;
      const icon = data.icon || MENTION_ICONS[data.entityType] || '';
      const { from } = editor.state.selection;
      const textBefore = editor.state.doc.textBetween(
        Math.max(0, from - 50),
        from,
        '\n',
      );
      const mentionMatch = textBefore.match(/(^|[\s])@(\w*)$/);

      if (mentionMatch) {
        const prefixLen = mentionMatch[1].length; // leading whitespace
        const start = from - mentionMatch[0].length + prefixLen;
        editor
          .chain()
          .focus()
          .deleteRange({ from: start, to: from })
          .insertContent([
            {
              type: 'text',
              text: `${icon} ${data.displayName}`,
              marks: [
                {
                  type: 'mention',
                  attrs: {
                    entityType: data.entityType,
                    entityId: data.entityId,
                    displayName: data.displayName,
                    icon,
                  },
                },
              ],
            },
            { type: 'text', text: ' ' },
          ])
          .run();
      }

      setShowMentionAutocomplete(false);
      setMentionQuery('');
    },
    [editor],
  );

  if (!editor) {
    return null;
  }

  return (
    <div ref={mentionAnchorRef} className="relative">
      <TableFloatingToolbar editor={editor} />
      <EditorContent
        editor={editor}
        onClick={(e) => {
          const link = (e.target as HTMLElement).closest('a');
          if (link?.href) {
            e.preventDefault();
            window.open(link.href, '_blank', 'noopener,noreferrer');
          }
        }}
      />
      {/* Bottom spacer so the cursor isn't stuck at the screen edge */}
      <div className="h-[40vh]" onClick={() => editor?.commands.focus('end')} />
      {showMentionAutocomplete && workspaceId && (
        <UniversalMentionAutocomplete
          query={mentionQuery}
          workspaceId={workspaceId}
          onSelect={handleMentionSelect}
          onClose={() => {
            setShowMentionAutocomplete(false);
            setMentionQuery('');
          }}
          anchorRef={mentionAnchorRef}
          cursorCoords={mentionCursorCoords}
          position="auto"
        />
      )}
    </div>
  );
}
