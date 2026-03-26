import { useState, useRef } from "react";
import { motion } from "motion/react";
import { avatarGradient } from "../../../utils/avatarGradient";
import { uploadAvatar } from "../../../api/client";
import { useAuthStore } from "../../../stores/authStore";
import OnboardingProgress from "../OnboardingProgress";

const MAX_NAME_LENGTH = 50;

interface ProfileStepProps {
  name: string;
  avatarUrl: string | null;
  onNameChange: (name: string) => void;
  onAvatarChange: (url: string | null) => void;
  onNext: () => void;
  onBack: () => void;
}

export default function ProfileStep({
  name,
  avatarUrl,
  onNameChange,
  onAvatarChange,
  onNext,
}: ProfileStepProps) {
  const [uploading, setUploading] = useState(false);
  const [avatarPreview, setAvatarPreview] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const displayAvatar = avatarPreview || avatarUrl;
  const initial = name ? name.charAt(0).toUpperCase() : "?";

  const handleFileChange = async (
    e: React.ChangeEvent<HTMLInputElement>
  ) => {
    const file = e.target.files?.[0];
    if (!file) return;

    if (!file.type.startsWith("image/")) return;
    if (file.size > 5 * 1024 * 1024) return;

    // Show local preview immediately
    const previewUrl = URL.createObjectURL(file);
    setAvatarPreview(previewUrl);

    setUploading(true);
    try {
      const url = await uploadAvatar(file);
      onAvatarChange(url);
      useAuthStore.getState().updateAvatarUrl(url);
      setAvatarPreview(null);
    } catch {
      setAvatarPreview(null);
    } finally {
      setUploading(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && name.trim()) {
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
      <OnboardingProgress currentStep="profile" />

      <h2 className="text-3xl font-bold text-gray-900 tracking-tight mb-6">
        What's your name?
      </h2>

      {/* Avatar + Name input row */}
      <div className="flex items-center gap-3 mb-6">
        {/* Avatar upload */}
        <div className="relative group w-12 h-12 shrink-0">
          <input
            ref={fileInputRef}
            type="file"
            accept="image/jpeg,image/png,image/gif,image/webp"
            onChange={handleFileChange}
            className="hidden"
          />

          {displayAvatar ? (
            <img
              src={displayAvatar}
              alt="Profile"
              className="w-12 h-12 rounded-lg object-cover"
            />
          ) : (
            <div className="w-12 h-12 rounded-lg flex items-center justify-center" style={{ background: avatarGradient(name || "?") }}>
              <span className="text-xl font-semibold text-white">
                {initial}
              </span>
            </div>
          )}

          {/* Camera overlay */}
          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            disabled={uploading}
            className="absolute inset-0 flex items-center justify-center bg-black/40 rounded-lg opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer disabled:cursor-wait"
          >
            <svg
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="none"
              stroke="white"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z" />
              <circle cx="12" cy="13" r="4" />
            </svg>
          </button>

          {/* Edit badge */}
          <div className="absolute -bottom-1 -right-1 w-6 h-6 bg-white border border-border-gray rounded-full flex items-center justify-center shadow-sm cursor-pointer hover:bg-bg-gray transition-colors"
            onClick={() => fileInputRef.current?.click()}
          >
            <svg
              width="12"
              height="12"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="text-gray-500"
            >
              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" />
              <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
            </svg>
          </div>
        </div>{/* end avatar */}

        {/* Name input */}
        <div className="relative flex-1">
          <input
            type="text"
            value={name}
            onChange={(e) =>
              onNameChange(e.target.value.slice(0, MAX_NAME_LENGTH))
            }
            onKeyDown={handleKeyDown}
            placeholder="Your name"
            autoFocus
            className="w-full bg-white border border-border-gray rounded-xl px-5 py-3.5 pr-14 text-base text-gray-900 placeholder:text-gray-400 outline-none focus:border-text-tertiary transition-all"
          />
          <span className="absolute right-5 top-1/2 -translate-y-1/2 text-sm text-gray-400 tabular-nums">
            {MAX_NAME_LENGTH - name.length}
          </span>
        </div>
      </div>{/* end avatar + name row */}

      <div className="flex justify-end mt-2">
        <button
          onClick={onNext}
          disabled={!name.trim()}
          className="px-7 py-2.5 bg-gray-900 text-white rounded-xl text-base font-medium hover:bg-gray-800 active:scale-[0.98] transition-all disabled:opacity-30 disabled:cursor-not-allowed"
        >
          Next
        </button>
      </div>
    </motion.div>
  );
}
