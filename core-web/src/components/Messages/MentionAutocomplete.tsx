import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { avatarGradient } from '../../utils/avatarGradient';
import { motion, AnimatePresence } from 'motion/react';
import type { ChannelMember } from '../../api/client';

interface MentionAutocompleteProps {
  query: string;
  members: ChannelMember[];
  onSelect: (member: ChannelMember) => void;
  onClose: () => void;
  anchorRef: React.RefObject<HTMLElement | null>;
}

export function MentionAutocomplete({
  query,
  members,
  onSelect,
  onClose,
  anchorRef,
}: MentionAutocompleteProps) {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState<{ bottom: number; left: number; width: number } | null>(null);

  // Filter members by query
  const filtered = members
    .filter(m => m.name && m.name.toLowerCase().includes(query.toLowerCase()))
    .slice(0, 8);

  // Reset selection when query or filtered results change
  useEffect(() => {
    setSelectedIndex(0);
  }, [query]);

  // Position above the composer
  useEffect(() => {
    if (anchorRef.current) {
      const rect = anchorRef.current.getBoundingClientRect();
      setPosition({
        bottom: window.innerHeight - rect.top + 4,
        left: rect.left,
        width: rect.width,
      });
    }
  }, [anchorRef]);

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        e.stopPropagation();
        setSelectedIndex(prev => (prev + 1) % Math.max(filtered.length, 1));
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        e.stopPropagation();
        setSelectedIndex(prev => (prev - 1 + filtered.length) % Math.max(filtered.length, 1));
      } else if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault();
        e.stopPropagation();
        if (filtered.length > 0) {
          onSelect(filtered[selectedIndex]);
        }
      } else if (e.key === 'Escape') {
        e.preventDefault();
        e.stopPropagation();
        onClose();
      }
    };

    document.addEventListener('keydown', handleKeyDown, true);
    return () => document.removeEventListener('keydown', handleKeyDown, true);
  }, [filtered, selectedIndex, onSelect, onClose]);

  // Click outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [onClose]);

  if (!position) return null;

  return createPortal(
    <AnimatePresence>
      <motion.div
        ref={dropdownRef}
        initial={{ opacity: 0, y: 4 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: 4 }}
        transition={{ duration: 0.15 }}
        className="fixed bg-white rounded-lg shadow-lg border border-border-gray py-1 z-[9999] overflow-hidden"
        style={{
          bottom: position.bottom,
          left: position.left,
          width: Math.min(position.width, 320),
        }}
      >
        {filtered.length === 0 ? (
          <div className="px-3 py-2 text-sm text-text-tertiary">No matches</div>
        ) : (
          filtered.map((member, index) => (
            <button
              key={member.user_id}
              onClick={() => onSelect(member)}
              onMouseEnter={() => setSelectedIndex(index)}
              className={`w-full flex items-center gap-2.5 px-3 py-2 text-left text-sm transition-colors ${
                index === selectedIndex ? 'bg-bg-gray' : 'hover:bg-bg-gray'
              }`}
            >
              {member.avatar_url ? (
                <img
                  src={member.avatar_url}
                  alt={member.name || ''}
                  className="w-6 h-6 rounded-full object-cover flex-shrink-0"
                />
              ) : (
                <div
                  className="w-6 h-6 rounded-full flex-shrink-0 flex items-center justify-center text-white font-semibold text-[10px]"
                  style={{ background: avatarGradient(member.name || member.user_id) }}
                >
                  {(member.name || "?").charAt(0).toUpperCase()}
                </div>
              )}
              <span className="text-text-body font-medium truncate">{member.name}</span>
            </button>
          ))
        )}
      </motion.div>
    </AnimatePresence>,
    document.body
  );
}
