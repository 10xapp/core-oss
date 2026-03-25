import { useEffect, useId, useRef, useState, type KeyboardEvent } from 'react';
import { searchUsersForSharing, type UserSearchResult } from '../../api/client';

interface UserSearchInputProps {
  value: string;
  onValueChange: (value: string) => void;
  onSelect: (user: UserSearchResult) => void;
}

export default function UserSearchInput({ value, onValueChange, onSelect }: UserSearchInputProps) {
  const [results, setResults] = useState<UserSearchResult[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeIndex, setActiveIndex] = useState(-1);
  const containerRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const listboxId = useId();

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (value.trim().length < 2) {
      setResults([]);
      setIsOpen(false);
      setError(null);
      setActiveIndex(-1);
      return;
    }

    setError(null);
    debounceRef.current = setTimeout(() => {
      setIsLoading(true);
      searchUsersForSharing(value.trim())
        .then((users) => {
          setResults(users);
          setIsOpen(true);
        })
        .catch(() => {
          setResults([]);
          setError('Search failed. Please try again.');
          setIsOpen(true);
        })
        .finally(() => setIsLoading(false));
    }, 250);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [value]);

  useEffect(() => {
    if (!isOpen) {
      setActiveIndex(-1);
      return;
    }
    if (results.length > 0) {
      setActiveIndex(0);
    } else {
      setActiveIndex(-1);
    }
  }, [isOpen, results]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (!isOpen) {
      if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        setIsOpen(true);
      }
      return;
    }

    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActiveIndex((prev) => Math.min(prev + 1, results.length - 1));
      return;
    }

    if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActiveIndex((prev) => Math.max(prev - 1, 0));
      return;
    }

    if (event.key === 'Enter') {
      if (activeIndex >= 0 && results[activeIndex]) {
        event.preventDefault();
        onSelect(results[activeIndex]);
        setIsOpen(false);
      }
      return;
    }

    if (event.key === 'Escape') {
      setIsOpen(false);
    }
  };

  return (
    <div className="relative" ref={containerRef}>
      <input
        type="text"
        value={value}
        onChange={(e) => onValueChange(e.target.value)}
        onFocus={() => value.trim().length >= 2 && setIsOpen(true)}
        onKeyDown={handleKeyDown}
        placeholder="Add people by email"
        role="combobox"
        aria-autocomplete="list"
        aria-expanded={isOpen}
        aria-controls={listboxId}
        aria-activedescendant={activeIndex >= 0 ? `${listboxId}-option-${activeIndex}` : undefined}
        className="w-full min-w-0 bg-white border border-border-gray rounded-md px-3 py-2 text-sm outline-none focus:border-text-tertiary"
      />
      {isOpen && (
        <div
          id={listboxId}
          role="listbox"
          className="absolute z-10 mt-1 w-full bg-white border border-border-gray rounded-md shadow-lg max-h-48 overflow-y-auto"
        >
          {isLoading && (
            <div className="px-3 py-2 text-xs text-text-secondary">Searching...</div>
          )}
          {!isLoading && error && (
            <div className="px-3 py-2 text-xs text-red-500">{error}</div>
          )}
          {!isLoading && !error && results.length === 0 && (
            <div className="px-3 py-2 text-xs text-text-secondary">No users found</div>
          )}
          {!isLoading && !error && results.map((user, index) => (
            <button
              key={user.id}
              id={`${listboxId}-option-${index}`}
              role="option"
              aria-selected={index === activeIndex}
              onMouseEnter={() => setActiveIndex(index)}
              onClick={() => {
                onSelect(user);
                setIsOpen(false);
              }}
              className={`w-full text-left px-3 py-2 text-sm flex flex-col ${
                index === activeIndex ? 'bg-bg-gray' : 'hover:bg-bg-gray'
              }`}
            >
              <span className="text-text-body">{user.name || user.email}</span>
              <span className="text-xs text-text-secondary">{user.email}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
