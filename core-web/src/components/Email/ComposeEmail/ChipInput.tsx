import { useState, useRef, useImperativeHandle, forwardRef, type KeyboardEvent } from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import { toast } from 'sonner';

interface ChipInputProps {
  value: string[];
  onChange: (value: string[]) => void;
  placeholder?: string;
  label?: string;
  onTabToNext?: () => void;  // Called when Tab should move to next field
}

export interface ChipInputRef {
  focus: () => void;
}

// Simple but robust email validation regex
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const isValidEmail = (email: string): boolean => {
  return EMAIL_REGEX.test(email);
};

const ChipInput = forwardRef<ChipInputRef, ChipInputProps>(({ value, onChange, placeholder, label, onTabToNext }, ref) => {
  const [inputValue, setInputValue] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  // Expose focus method to parent
  useImperativeHandle(ref, () => ({
    focus: () => inputRef.current?.focus()
  }));

  const addEmail = (email: string) => {
    const trimmed = email.trim();
    if (!trimmed) {
      setInputValue('');
      return;
    }

    // Validate email format
    if (!isValidEmail(trimmed)) {
      toast.error(`Invalid email address: ${trimmed}`);
      return;
    }

    if (!value.includes(trimmed)) {
      onChange([...value, trimmed]);
    }
    setInputValue('');
  };

  const removeEmail = (index: number) => {
    onChange(value.filter((_, i) => i !== index));
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' || e.key === ',') {
      if (inputValue.trim()) {
        e.preventDefault();
        addEmail(inputValue);
      }
    } else if (e.key === 'Tab') {
      // Add email if there's text
      if (inputValue.trim()) {
        e.preventDefault();
        addEmail(inputValue);
        // After adding, move to next field
        onTabToNext?.();
      } else if (onTabToNext) {
        // No text, but we have a next field - prevent default tab and move there
        e.preventDefault();
        onTabToNext();
      }
      // If no onTabToNext, let default tab behavior happen
    } else if (e.key === 'Backspace' && !inputValue && value.length > 0) {
      removeEmail(value.length - 1);
    }
  };

  const handleBlur = () => {
    if (inputValue.trim()) {
      addEmail(inputValue);
    }
  };

  const handleContainerClick = () => {
    inputRef.current?.focus();
  };

  return (
    <div className="flex items-start px-4 py-2 gap-2">
      {label && <label className="text-sm text-text-secondary shrink-0 pt-0.5">{label}</label>}
      <div
        className="flex-1 flex flex-wrap items-center gap-1.5 cursor-text min-h-[24px]"
        onClick={handleContainerClick}
      >
        {value.map((email, index) => (
          <span
            key={index}
            className="inline-flex items-center gap-1 px-2 py-0.5 bg-bg-gray-dark rounded-full text-sm text-text-body"
          >
            {email}
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                removeEmail(index);
              }}
              className="text-text-tertiary hover:text-text-body transition-colors"
            >
              <XMarkIcon className="w-3 h-3 stroke-2" />
            </button>
          </span>
        ))}
        <input
          ref={inputRef}
          type="text"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
          onBlur={handleBlur}
          placeholder={value.length === 0 ? placeholder : ''}
          className="flex-1 min-w-[120px] text-sm text-text-body bg-transparent outline-none placeholder:text-text-tertiary"
        />
      </div>
    </div>
  );
});

ChipInput.displayName = 'ChipInput';

export default ChipInput;
