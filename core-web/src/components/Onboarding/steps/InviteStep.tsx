import { useState, useRef } from "react";
import { motion, AnimatePresence } from "motion/react";
import OnboardingProgress from "../OnboardingProgress";

interface InviteStepProps {
  emails: string[];
  onEmailsChange: (emails: string[]) => void;
  onNext: () => void;
  onSkip: () => void;
  onBack: () => void;
}

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

export default function InviteStep({
  emails,
  onEmailsChange,
  onNext,
  onSkip,
}: InviteStepProps) {
  const [inputValue, setInputValue] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  const addEmail = (raw: string) => {
    const email = raw.trim().toLowerCase();
    if (isValidEmail(email) && !emails.includes(email)) {
      onEmailsChange([...emails, email]);
    }
  };

  const removeEmail = (email: string) => {
    onEmailsChange(emails.filter((e) => e !== email));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" || e.key === "," || e.key === "Tab") {
      e.preventDefault();
      if (inputValue.trim()) {
        addEmail(inputValue);
        setInputValue("");
      }
    }
    if (e.key === " " && isValidEmail(inputValue.trim())) {
      e.preventDefault();
      addEmail(inputValue);
      setInputValue("");
    }
    if (e.key === "Backspace" && !inputValue && emails.length > 0) {
      removeEmail(emails[emails.length - 1]);
    }
  };

  const handlePaste = (e: React.ClipboardEvent) => {
    e.preventDefault();
    const text = e.clipboardData.getData("text");
    const pasted = text
      .split(/[\n,;]+/)
      .map((s) => s.trim().toLowerCase())
      .filter((s) => isValidEmail(s) && !emails.includes(s));
    if (pasted.length > 0) {
      onEmailsChange([...emails, ...pasted]);
    }
  };

  const handleBlur = () => {
    if (inputValue.trim()) {
      addEmail(inputValue);
      setInputValue("");
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, x: 30 }}
      animate={{ opacity: 1, x: 0 }}
      exit={{ opacity: 0, x: -30 }}
      transition={{ duration: 0.25, ease: [0.4, 0, 0.2, 1] }}
    >
      <OnboardingProgress currentStep="invite" />

      <h2 className="text-3xl font-bold text-gray-900 tracking-tight mb-3">
        Invite your teammates
      </h2>
      <p className="text-base text-gray-500 leading-relaxed mb-8">
        Core works better with more people. Add your core collaborators.
      </p>

      <label className="block text-base font-medium text-gray-700 mb-2">
        Add teammate by email
      </label>
      <div
        className="w-full min-h-[52px] bg-white border border-border-gray rounded-xl px-3 py-2.5 flex flex-wrap gap-2 items-center cursor-text focus-within:border-text-tertiary transition-all"
        onClick={() => inputRef.current?.focus()}
      >
        <AnimatePresence>
          {emails.map((email) => (
            <motion.span
              key={email}
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.8 }}
              transition={{ duration: 0.15 }}
              className="inline-flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-medium"
              style={{ background: "#e5f7fd", color: "#1768a5" }}
            >
              {email}
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  removeEmail(email);
                }}
                className="ml-0.5 hover:opacity-70 transition-opacity"
              >
                <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                  <path
                    d="M3 3l6 6M9 3l-6 6"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                  />
                </svg>
              </button>
            </motion.span>
          ))}
        </AnimatePresence>
        <input
          ref={inputRef}
          type="text"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
          onPaste={handlePaste}
          onBlur={handleBlur}
          placeholder={emails.length === 0 ? "Ex. ellis@gmail.com, maria@gmail.com" : ""}
          className="flex-1 min-w-[120px] text-base text-gray-900 placeholder:text-gray-400 outline-none bg-transparent"
        />
      </div>

      <div className="flex items-center justify-end gap-6 mt-10">
        <button
          onClick={onSkip}
          className="text-base text-gray-400 hover:text-gray-600 transition-colors"
        >
          Skip this step
        </button>
        <button
          onClick={() => onNext()}
          disabled={emails.length === 0}
          className="px-8 py-3 bg-gray-900 text-white rounded-xl text-base font-medium hover:bg-gray-800 active:scale-[0.98] transition-all disabled:opacity-30 disabled:cursor-not-allowed"
        >
          Next
        </button>
      </div>
    </motion.div>
  );
}
