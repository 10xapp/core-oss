import { motion } from "motion/react";
import OnboardingProgress from "../OnboardingProgress";

const MAX_LENGTH = 50;

interface WorkspaceNameStepProps {
  value: string;
  onChange: (name: string) => void;
  onNext: () => void;
  onBack: () => void;
}

export default function WorkspaceNameStep({
  value,
  onChange,
  onNext,
}: WorkspaceNameStepProps) {
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && value.trim()) {
      onNext();
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, x: 30 }}
      animate={{ opacity: 1, x: 0 }}
      exit={{ opacity: 0, x: -30 }}
      transition={{ duration: 0.25, ease: [0.4, 0, 0.2, 1] }}
    >
      <OnboardingProgress currentStep="workspace-name" />

      <h2 className="text-3xl font-bold text-gray-900 tracking-tight mb-3">
        Name your workspace
      </h2>
      <p className="text-base text-gray-500 leading-relaxed mb-8">
        Choose something your team will recognize like the name of your
        organization or team. You can always update it later.
      </p>

      <div className="relative">
        <input
          type="text"
          value={value}
          onChange={(e) =>
            onChange(e.target.value.slice(0, MAX_LENGTH))
          }
          onKeyDown={handleKeyDown}
          placeholder="ex. Core Inc."
          autoFocus
          className="w-full bg-white border border-border-gray rounded-xl px-5 py-3.5 pr-14 text-base text-gray-900 placeholder:text-gray-400 outline-none focus:border-text-tertiary transition-all"
        />
        <span className="absolute right-5 top-1/2 -translate-y-1/2 text-sm text-gray-400 tabular-nums">
          {MAX_LENGTH - value.length}
        </span>
      </div>

      <div className="flex justify-end mt-8">
        <button
          onClick={onNext}
          disabled={!value.trim()}
          className="px-7 py-2.5 bg-gray-900 text-white rounded-xl text-base font-medium hover:bg-gray-800 active:scale-[0.98] transition-all disabled:opacity-30 disabled:cursor-not-allowed"
        >
          Next
        </button>
      </div>
    </motion.div>
  );
}
