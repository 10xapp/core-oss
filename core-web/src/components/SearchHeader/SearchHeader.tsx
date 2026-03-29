import { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { X, Sparkles } from 'lucide-react';
import { Icon } from '../ui/Icon';
import { useWorkspaceStore } from '../../stores/workspaceStore';

interface WorkspaceScope {
  id: string;
  name: string;
  icon_url?: string;
}

export default function SearchHeader() {
  const navigate = useNavigate();
  const { workspaces } = useWorkspaceStore();

  const [query, setQuery] = useState('');
  const [scope, setScope] = useState<WorkspaceScope | null>(null);
  const [showScopeDropdown, setShowScopeDropdown] = useState(false);

  const inputRef = useRef<HTMLInputElement>(null);

  // Detect @mention for workspace scoping
  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setQuery(value);

    // Check for @mention at the start
    if (value.startsWith('@') && !scope) {
      setShowScopeDropdown(true);
    } else if (!value.includes('@')) {
      setShowScopeDropdown(false);
    }
  };

  // Filter workspaces based on @mention text
  const getMentionText = () => {
    if (!query.startsWith('@')) return '';
    const spaceIndex = query.indexOf(' ');
    return spaceIndex > 0 ? query.slice(1, spaceIndex) : query.slice(1);
  };

  const filteredWorkspaces = workspaces.filter(ws =>
    ws.name.toLowerCase().includes(getMentionText().toLowerCase())
  );

  // Select workspace scope from dropdown
  const selectScope = (ws: typeof workspaces[0]) => {
    setScope({ id: ws.id, name: ws.name, icon_url: ws.icon_url });
    // Remove @mention from query
    const mentionEnd = query.indexOf(' ');
    setQuery(mentionEnd > 0 ? query.slice(mentionEnd + 1) : '');
    setShowScopeDropdown(false);
    inputRef.current?.focus();
  };

  // Clear scope
  const clearScope = () => {
    setScope(null);
    inputRef.current?.focus();
  };

  // Handle form submission - navigate to chat with initial message
  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;

    // Build the message with context
    let message = query.trim();
    if (scope) {
      message = `[Context: ${scope.name} workspace] ${message}`;
    }

    // Navigate to chat with the message in state
    navigate('/chat', { state: { initialMessage: message } });
    setQuery('');
    setScope(null);
  };

  // Keyboard shortcut: Cmd+K to focus search
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        inputRef.current?.focus();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, []);

  // Close dropdown on outside click
  useEffect(() => {
    if (!showScopeDropdown) return;
    const handleClick = (e: MouseEvent) => {
      if (!inputRef.current?.contains(e.target as Node)) {
        setShowScopeDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [showScopeDropdown]);

  return (
    <div className="h-[48px] flex items-center justify-center px-4">
      <form onSubmit={handleSubmit} className="relative w-[400px]">
        <div className="flex items-center gap-2 bg-white border border-border-gray rounded-lg px-3 h-[32px] focus-within:border-text-tertiary transition-colors">
          {/* AI sparkle icon */}
          <Icon icon={Sparkles} size={18} className="text-brand-primary flex-shrink-0" />

          {/* Scope chip */}
          {scope && (
            <div className="flex items-center gap-1.5 bg-bg-gray border border-border-light rounded-md px-2 py-0.5 text-sm flex-shrink-0">
              {scope.icon_url ? (
                <img src={scope.icon_url} alt="" className="w-4 h-4 rounded object-cover" />
              ) : (
                <span className="w-4 h-4 flex items-center justify-center text-[10px] font-medium bg-gradient-to-br from-blue-500 to-purple-600 text-white rounded">
                  {scope.name.charAt(0).toUpperCase()}
                </span>
              )}
              <span className="text-text-body font-medium">{scope.name}</span>
              <button
                type="button"
                onClick={clearScope}
                className="ml-0.5 text-text-secondary hover:text-text-body"
              >
                <Icon icon={X} size={12} />
              </button>
            </div>
          )}

          {/* Input */}
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={handleInputChange}
            placeholder={scope ? `Ask about ${scope.name}...` : "Ask anything..."}
            className="flex-1 bg-transparent text-sm outline-none placeholder:text-text-secondary"
          />

          {/* Keyboard shortcut hint */}
          {!query && (
            <div className="hidden sm:flex items-center gap-1 text-xs text-text-secondary flex-shrink-0">
              <kbd className="px-1.5 py-0.5 bg-bg-gray border border-border-light rounded text-[10px] font-medium">⌘</kbd>
              <kbd className="px-1.5 py-0.5 bg-bg-gray border border-border-light rounded text-[10px] font-medium">K</kbd>
            </div>
          )}

          {/* Submit indicator */}
          {query && (
            <div className="text-xs text-text-secondary flex-shrink-0">
              Enter ↵
            </div>
          )}
        </div>

        {/* Workspace scope dropdown */}
        {showScopeDropdown && filteredWorkspaces.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-border-light rounded-lg shadow-lg z-50 max-h-64 overflow-y-auto">
            <div className="p-1">
              <div className="px-2 py-1.5 text-xs text-text-secondary font-medium">
                Scope to workspace
              </div>
              {filteredWorkspaces.map(ws => (
                <button
                  key={ws.id}
                  type="button"
                  onClick={() => selectScope(ws)}
                  className="w-full flex items-center gap-2 px-2 py-2 text-sm hover:bg-bg-gray rounded-md text-left"
                >
                  {ws.icon_url ? (
                    <img src={ws.icon_url} alt="" className="w-5 h-5 rounded object-cover" />
                  ) : (
                    <span className="w-5 h-5 flex items-center justify-center text-xs font-medium bg-gradient-to-br from-blue-500 to-purple-600 text-white rounded">
                      {ws.name.charAt(0).toUpperCase()}
                    </span>
                  )}
                  <span>{ws.name}</span>
                </button>
              ))}
            </div>
          </div>
        )}
      </form>
    </div>
  );
}
