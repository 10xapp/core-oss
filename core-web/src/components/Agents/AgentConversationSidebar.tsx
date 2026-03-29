import { useState, useRef, useEffect } from "react";
import { Plus, MoreHorizontal } from "lucide-react";
import { Icon } from "../ui/Icon";
import type { AgentConversation } from "../../api/client";

interface AgentConversationSidebarProps {
  conversations: AgentConversation[];
  activeConversationId: string | null;
  onSelect: (id: string) => void;
  onCreate: () => void;
  onRename: (id: string, title: string) => void;
  onDelete: (id: string) => void;
}

export default function AgentConversationSidebar({
  conversations,
  activeConversationId,
  onSelect,
  onCreate,
  onRename,
  onDelete,
}: AgentConversationSidebarProps) {
  const [menuOpen, setMenuOpen] = useState<string | null>(null);
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const renameInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (renamingId) {
      renameInputRef.current?.focus();
      renameInputRef.current?.select();
    }
  }, [renamingId]);

  const handleRenameSubmit = (id: string) => {
    const trimmed = renameValue.trim();
    if (trimmed) {
      onRename(id, trimmed);
    }
    setRenamingId(null);
  };

  return (
    <div className="flex flex-col h-full">
      {/* New conversation button */}
      <div className="px-2 pt-2 pb-1">
        <button
          onClick={onCreate}
          className="w-full flex items-center gap-2 px-2 py-2 rounded-md text-sm text-text-secondary hover:bg-black/5 transition-colors focus-visible:ring-2 focus-visible:ring-brand-primary focus-visible:ring-inset"
        >
          <Icon icon={Plus} size={14} aria-hidden="true" />
          <span>New conversation</span>
        </button>
      </div>

      {/* Conversation list */}
      <div className="flex-1 overflow-y-auto px-2">
        <div className="space-y-0.5">
          {conversations.map((conv) => (
            <div key={conv.id} className="group relative">
              {renamingId === conv.id ? (
                <input
                  ref={renameInputRef}
                  value={renameValue}
                  onChange={(e) => setRenameValue(e.target.value)}
                  onBlur={() => handleRenameSubmit(conv.id)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") handleRenameSubmit(conv.id);
                    if (e.key === "Escape") setRenamingId(null);
                  }}
                  className="w-full px-3 py-1.5 rounded-md text-sm bg-white border border-brand-primary focus:outline-none"
                />
              ) : (
                <div
                  role="button"
                  tabIndex={0}
                  onClick={() => onSelect(conv.id)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      onSelect(conv.id);
                    }
                  }}
                  className={`w-full flex items-center px-2 h-[32px] rounded-md text-sm transition-colors focus-visible:ring-2 focus-visible:ring-brand-primary focus-visible:ring-inset ${
                    activeConversationId === conv.id
                      ? "bg-black/8 text-black font-medium"
                      : "text-gray-700 hover:bg-black/5"
                  }`}
                >
                  <span className="flex-1 text-left truncate text-[13px]">{conv.title}</span>

                  {/* Ellipsis menu trigger */}
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      setMenuOpen(menuOpen === conv.id ? null : conv.id);
                    }}
                    aria-label={`Options for ${conv.title}`}
                    className="opacity-0 group-hover:opacity-100 group-focus-within:opacity-100 shrink-0 p-0.5 rounded hover:bg-black/10 focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-brand-primary transition-opacity"
                  >
                    <Icon icon={MoreHorizontal} size={14} className="text-text-tertiary" aria-hidden="true" />
                  </button>
                </div>
              )}

              {/* Dropdown menu */}
              {menuOpen === conv.id && (
                <>
                  <div className="fixed inset-0 z-10" onClick={() => setMenuOpen(null)} />
                  <div className="absolute right-1 top-8 z-20 bg-white rounded-lg shadow-lg border border-border-light py-1 min-w-[120px]">
                    <button
                      onClick={() => {
                        setRenameValue(conv.title);
                        setRenamingId(conv.id);
                        setMenuOpen(null);
                      }}
                      className="w-full text-left px-3 py-1.5 text-sm text-text-body hover:bg-gray-50"
                    >
                      Rename
                    </button>
                    <button
                      onClick={() => {
                        onDelete(conv.id);
                        setMenuOpen(null);
                      }}
                      className="w-full text-left px-3 py-1.5 text-sm text-red-600 hover:bg-red-50"
                    >
                      Delete
                    </button>
                  </div>
                </>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
