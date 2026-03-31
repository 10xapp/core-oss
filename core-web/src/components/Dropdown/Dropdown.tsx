import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { motion, AnimatePresence } from 'motion/react';

interface DropdownProps {
  isOpen: boolean;
  onClose: () => void;
  trigger: React.RefObject<HTMLElement | null>;
  children: React.ReactNode;
  align?: 'left' | 'right';
  position?: 'bottom' | 'top';
}

export default function Dropdown({ isOpen, onClose, trigger, children, align = 'left', position: dropdownPosition = 'bottom' }: DropdownProps) {
  const [position, setPosition] = useState({ top: 0, left: 0, bottom: 'auto' as number | 'auto' });
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Calculate position based on trigger element
  useEffect(() => {
    if (isOpen && trigger.current) {
      const rect = trigger.current.getBoundingClientRect();
      if (dropdownPosition === 'top') {
        setPosition({
          top: 0,
          bottom: window.innerHeight - rect.top + 4,
          left: align === 'left' ? rect.left : rect.right,
        });
      } else {
        setPosition({
          top: rect.bottom + 4,
          bottom: 'auto',
          left: align === 'left' ? rect.left : rect.right,
        });
      }
    }
  }, [isOpen, trigger, align, dropdownPosition]);

  // Close on click outside
  useEffect(() => {
    if (!isOpen) return;

    const handleClickOutside = (e: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node) &&
        trigger.current &&
        !trigger.current.contains(e.target as Node)
      ) {
        onClose();
      }
    };

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };

    document.addEventListener('mousedown', handleClickOutside);
    document.addEventListener('keydown', handleEscape);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [isOpen, onClose, trigger]);

  return createPortal(
    <AnimatePresence>
      {isOpen && (
        <motion.div
          ref={dropdownRef}
          initial={{ opacity: 0, y: -4 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -4 }}
          transition={{ duration: 0.1 }}
          style={{
            position: 'fixed',
            top: dropdownPosition === 'top' ? 'auto' : position.top,
            bottom: dropdownPosition === 'top' ? position.bottom : 'auto',
            left: align === 'left' ? position.left : 'auto',
            right: align === 'right' ? window.innerWidth - position.left : 'auto',
            zIndex: 9999,
            minWidth: 180,
            backgroundColor: '#FFFFFF',
            border: '1px solid #E7E7E6',
            borderRadius: 8,
            boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1)',
            padding: '4px 0',
          }}
        >
          {children}
        </motion.div>
      )}
    </AnimatePresence>,
    document.body
  );
}
