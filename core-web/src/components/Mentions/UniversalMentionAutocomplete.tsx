import { useState, useEffect, useRef, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { avatarGradient } from '../../utils/avatarGradient';
import { motion, AnimatePresence } from 'motion/react';
import { ChevronRightIcon, ChevronLeftIcon } from '@heroicons/react/24/outline';
import { useMentionData } from '../../hooks/useMentionData';
import { MENTION_ICONS } from '../../types/mention';
import type { MentionData, MentionMenuItem, MentionMenuLevel } from '../../types/mention';

interface UniversalMentionAutocompleteProps {
  query: string;
  onSelect: (data: MentionData) => void;
  onClose: () => void;
  anchorRef: React.RefObject<HTMLElement | null>;
  workspaceId: string;
  position?: 'above' | 'below' | 'auto';
  /** Exact cursor coordinates (viewport-relative). When provided, dropdown positions at the cursor instead of the anchor element. */
  cursorCoords?: { top: number; bottom: number; left: number };
}

const MAX_VISIBLE_ITEMS = 20;
const DROPDOWN_HEIGHT = 380;

export function UniversalMentionAutocomplete({
  query,
  onSelect,
  onClose,
  anchorRef,
  workspaceId,
  position: positionMode = 'above',
  cursorCoords,
}: UniversalMentionAutocompleteProps) {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [menuStack, setMenuStack] = useState<MentionMenuLevel[]>([]);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [coords, setCoords] = useState<{ top?: number; bottom?: number; left: number; width: number } | null>(null);

  const mentionData = useMentionData(workspaceId);

  // Initialize root level
  useEffect(() => {
    setMenuStack([mentionData.getRootLevel()]);
  }, [mentionData.getRootLevel]);

  // Reset selection when query changes
  useEffect(() => {
    setSelectedIndex(0);
  }, [query]);

  // Position relative to cursor coords or anchor element
  useEffect(() => {
    if (cursorCoords) {
      // Position at exact cursor position, auto-detect above/below
      const spaceAbove = cursorCoords.top;
      const spaceBelow = window.innerHeight - cursorCoords.bottom;
      const showAbove = positionMode === 'above' || (positionMode === 'auto' && spaceAbove > spaceBelow) || (positionMode !== 'below' && spaceAbove >= DROPDOWN_HEIGHT);

      if (showAbove) {
        setCoords({
          bottom: window.innerHeight - cursorCoords.top + 4,
          left: cursorCoords.left,
          width: 320,
        });
      } else {
        setCoords({
          top: cursorCoords.bottom + 4,
          left: cursorCoords.left,
          width: 320,
        });
      }
    } else if (anchorRef.current) {
      const rect = anchorRef.current.getBoundingClientRect();
      const spaceAbove = rect.top;
      const spaceBelow = window.innerHeight - rect.bottom;
      const showAbove = positionMode === 'above' || (positionMode === 'auto' && spaceAbove >= DROPDOWN_HEIGHT) || (positionMode !== 'below' && spaceAbove > spaceBelow);

      if (showAbove) {
        setCoords({
          bottom: window.innerHeight - rect.top + 4,
          left: rect.left,
          width: rect.width,
        });
      } else {
        setCoords({
          top: rect.bottom + 4,
          left: rect.left,
          width: rect.width,
        });
      }
    }
  }, [anchorRef, positionMode, cursorCoords]);

  const currentLevel = menuStack[menuStack.length - 1];
  const isAtRoot = menuStack.length <= 1;

  // Get filtered items for current level
  // Only filter by query at root level — drilled-in levels show all items
  // (the root query like "pro" from typing @pro shouldn't filter sub-level contents)
  const filteredItems = currentLevel
    ? mentionData.filterLevel(currentLevel, isAtRoot ? query : '').slice(0, MAX_VISIBLE_ITEMS)
    : [];

  // Split root level into apps, inline file results, and people for rendering with dividers
  const appItems = isAtRoot ? filteredItems.filter((i) => i.hasChildren) : [];
  const inlineFileItems = isAtRoot ? filteredItems.filter((i) => !i.hasChildren && i.entityType !== 'person') : [];
  const peopleItems = isAtRoot ? filteredItems.filter((i) => i.entityType === 'person') : [];
  const drillItems = !isAtRoot ? filteredItems : [];

  // All selectable items in display order
  const allItems = isAtRoot ? [...appItems, ...inlineFileItems, ...peopleItems] : drillItems;

  const drillInto = useCallback(
    (item: MentionMenuItem) => {
      let subLevel: MentionMenuLevel | null = null;

      if (item.entityType === 'folder' && item.appType === 'files' && item.appId && item.id !== item.appId) {
        // Drill into a file subfolder (not the root app item)
        subLevel = mentionData.getDrillDownForFolder(item.id, item.displayName, item.appId);
      } else {
        // Top-level app drill-down (files root, projects, messages, tasks)
        subLevel = mentionData.getDrillDown(item);
      }

      if (subLevel) {
        setMenuStack((prev) => [...prev, subLevel]);
        setSelectedIndex(0);
      }
    },
    [mentionData],
  );

  const goBack = useCallback(() => {
    if (menuStack.length > 1) {
      setMenuStack((prev) => prev.slice(0, -1));
      setSelectedIndex(0);
    }
  }, [menuStack.length]);

  const handleSelect = useCallback(
    (item: MentionMenuItem) => {
      if (item.hasChildren) {
        drillInto(item);
        return;
      }

      onSelect({
        entityType: item.entityType,
        entityId: item.id,
        displayName: item.displayName,
        icon: item.icon || MENTION_ICONS[item.entityType],
      });
    },
    [drillInto, onSelect],
  );

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        e.stopPropagation();
        setSelectedIndex((prev) => (prev + 1) % Math.max(allItems.length, 1));
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        e.stopPropagation();
        setSelectedIndex((prev) => (prev - 1 + allItems.length) % Math.max(allItems.length, 1));
      } else if (e.key === 'ArrowRight') {
        const item = allItems[selectedIndex];
        if (item?.hasChildren) {
          e.preventDefault();
          e.stopPropagation();
          drillInto(item);
        }
      } else if (e.key === 'ArrowLeft' || (e.key === 'Backspace' && !query && !isAtRoot)) {
        if (!isAtRoot) {
          e.preventDefault();
          e.stopPropagation();
          goBack();
        }
      } else if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault();
        e.stopPropagation();
        if (allItems.length > 0) {
          handleSelect(allItems[selectedIndex]);
        }
      } else if (e.key === 'Escape') {
        e.preventDefault();
        e.stopPropagation();
        if (!isAtRoot) {
          goBack();
        } else {
          onClose();
        }
      }
    };

    document.addEventListener('keydown', handleKeyDown, true);
    return () => document.removeEventListener('keydown', handleKeyDown, true);
  }, [allItems, selectedIndex, isAtRoot, query, drillInto, goBack, handleSelect, onClose]);

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

  // Scroll selected item into view
  useEffect(() => {
    const el = dropdownRef.current?.querySelector(`[data-mention-index="${selectedIndex}"]`);
    el?.scrollIntoView({ block: 'nearest' });
  }, [selectedIndex]);

  if (!coords) return null;

  const style: React.CSSProperties = {
    left: coords.left,
    width: Math.min(coords.width, 320),
    ...(coords.bottom != null ? { bottom: coords.bottom } : {}),
    ...(coords.top != null ? { top: coords.top } : {}),
  };

  let globalIndex = 0;

  const renderItem = (item: MentionMenuItem) => {
    const idx = globalIndex++;
    const isSelected = idx === selectedIndex;

    return (
      <button
        key={item.id}
        data-mention-index={idx}
        onClick={() => handleSelect(item)}
        onMouseEnter={() => setSelectedIndex(idx)}
        className={`w-[calc(100%-8px)] mx-1 flex items-center gap-2.5 px-2.5 py-1.5 text-left text-sm transition-colors rounded-lg ${
          isSelected ? 'bg-bg-gray' : 'hover:bg-bg-gray'
        }`}
      >
        {item.avatarUrl ? (
          <img
            src={item.avatarUrl}
            alt={item.displayName}
            className="w-6 h-6 rounded-full object-cover flex-shrink-0"
          />
        ) : item.entityType === 'person' ? (
          <div
            className="w-6 h-6 rounded-full flex-shrink-0 flex items-center justify-center text-white font-semibold text-[10px]"
            style={{ background: avatarGradient(item.displayName || item.id) }}
          >
            {(item.displayName || "?").charAt(0).toUpperCase()}
          </div>
        ) : (
          <span className="w-6 text-center flex-shrink-0">{item.icon}</span>
        )}
        <div className="flex-1 min-w-0 flex items-center gap-1.5">
          <span className="text-text-body font-medium truncate">{item.displayName}</span>
          {item.subtitle && (
            <span className="text-text-tertiary text-xs truncate">{item.subtitle}</span>
          )}
        </div>
        {item.hasChildren && (
          <ChevronRightIcon className="w-4 h-4 text-text-tertiary flex-shrink-0" />
        )}
      </button>
    );
  };

  return createPortal(
    <AnimatePresence>
      <motion.div
        ref={dropdownRef}
        initial={{ opacity: 0, y: coords?.bottom != null ? 4 : -4 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: coords?.bottom != null ? 4 : -4 }}
        transition={{ duration: 0.15 }}
        className="fixed bg-white rounded-lg shadow-lg border border-border-gray py-1 z-[10001] overflow-hidden"
        style={style}
      >
        {/* Back button header when drilled in */}
        {!isAtRoot && (
          <button
            onClick={goBack}
            className="w-[calc(100%-8px)] mx-1 flex items-center gap-2 px-2.5 py-2 text-sm text-text-secondary hover:bg-bg-gray rounded-lg border-b border-border-gray transition-colors"
          >
            <ChevronLeftIcon className="w-4 h-4" />
            <span className="font-medium">{currentLevel?.title}</span>
          </button>
        )}

        <div className="max-h-[340px] overflow-y-auto py-1">
          {allItems.length === 0 ? (
            <div className="px-3 py-2 text-sm text-text-tertiary">No matches</div>
          ) : isAtRoot ? (
            <>
              {/* Mini-app sections */}
              {appItems.length > 0 && appItems.map(renderItem)}

              {/* Inline file results when searching */}
              {inlineFileItems.length > 0 && (
                <>
                  {appItems.length > 0 && (
                    <div className="my-1 border-t border-border-gray" />
                  )}
                  <div className="px-3 py-1 text-xs font-medium text-text-tertiary uppercase tracking-wide">
                    Files
                  </div>
                  {inlineFileItems.map(renderItem)}
                </>
              )}

              {/* Divider before people */}
              {(appItems.length > 0 || inlineFileItems.length > 0) && peopleItems.length > 0 && (
                <div className="my-1 border-t border-border-gray" />
              )}

              {/* People section */}
              {peopleItems.length > 0 && (
                <>
                  <div className="px-3 py-1 text-xs font-medium text-text-tertiary uppercase tracking-wide">
                    People
                  </div>
                  {peopleItems.map(renderItem)}
                </>
              )}
            </>
          ) : (
            drillItems.map(renderItem)
          )}
        </div>
      </motion.div>
    </AnimatePresence>,
    document.body,
  );
}
