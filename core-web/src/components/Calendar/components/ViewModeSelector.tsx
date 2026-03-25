import type { ViewMode } from '../types/calendar.types';

interface ViewModeSelectorProps {
  value: ViewMode;
  onChange: (mode: ViewMode) => void;
}

const modes: { value: ViewMode; label: string }[] = [
  { value: 'day', label: 'D' },
  { value: 'week', label: 'W' },
  { value: 'month', label: 'M' },
  { value: 'year', label: 'Y' }
];

export default function ViewModeSelector({ value, onChange }: ViewModeSelectorProps) {
  return (
    <div className="flex items-center gap-0.5">
      {modes.map((mode) => (
        <button
          key={mode.value}
          onClick={() => onChange(mode.value)}
          className={`
            w-7 h-7 flex items-center justify-center rounded-md text-xs font-medium transition-colors
            ${value === mode.value
              ? 'bg-black/10 text-text-body'
              : 'text-text-tertiary hover:bg-black/5 hover:text-text-body'
            }
          `}
        >
          {mode.label}
        </button>
      ))}
    </div>
  );
}
