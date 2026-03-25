import { motion, AnimatePresence } from "motion/react";

interface OnboardingSidebarPreviewProps {
  workspaceName: string;
  userName: string;
  avatarUrl: string | null;
}

export default function OnboardingSidebarPreview({
  workspaceName,
  userName,
  avatarUrl,
}: OnboardingSidebarPreviewProps) {
  const wsInitial = workspaceName
    ? workspaceName.charAt(0).toUpperCase()
    : "W";
  const userInitial = userName ? userName.charAt(0).toUpperCase() : "?";

  return (
    <div className="w-[340px] overflow-hidden select-none shadow-lg rounded-lg">
      <div className="flex">
        {/* Mini icon sidebar */}
        <div className="w-14 bg-[#E3E3E5] rounded-l-lg flex flex-col items-center py-3 gap-2 shrink-0">
          {/* Workspace icon */}
          <div className="w-10 h-10 rounded-lg bg-blue-600 flex items-center justify-center mb-1">
            <AnimatePresence mode="wait">
              <motion.span
                key={wsInitial}
                initial={{ opacity: 0, scale: 0.5 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.5 }}
                transition={{ duration: 0.15 }}
                className="text-white text-sm font-bold"
              >
                {wsInitial}
              </motion.span>
            </AnimatePresence>
          </div>
          {/* App icon placeholders */}
          {[...Array(5)].map((_, i) => (
            <div
              key={i}
              className="w-9 h-9 rounded-md bg-black/6"
            />
          ))}
          {/* User avatar at bottom */}
          <div className="mt-auto pt-2">
            {avatarUrl ? (
              <img
                src={avatarUrl}
                alt=""
                className="w-9 h-9 rounded-full object-cover"
              />
            ) : (
              <div className="w-9 h-9 rounded-full bg-[#5BA4A4] flex items-center justify-center">
                <AnimatePresence mode="wait">
                  <motion.span
                    key={userInitial}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.1 }}
                    className="text-xs font-semibold text-white"
                  >
                    {userInitial}
                  </motion.span>
                </AnimatePresence>
              </div>
            )}
          </div>
        </div>

        {/* Content area */}
        <div className="flex-1 bg-white rounded-r-lg border border-l-0 border-border-light p-4 min-w-0">
          <p className="text-sm font-semibold text-gray-700 mb-4 truncate">
            Messages
          </p>

          <p className="text-[10px] uppercase tracking-wider text-gray-300 font-medium px-2 mb-2">
            Channels
          </p>
          <div className="space-y-2 mb-5">
            {["# general", "# design", "# engineering", "+ Add channels"].map((label) => (
              <div key={label} className="flex items-center gap-2.5 px-2">
                <span className="text-xs text-gray-400">{label}</span>
              </div>
            ))}
          </div>

          <p className="text-[10px] uppercase tracking-wider text-gray-300 font-medium px-2 mb-2">
            Direct messages
          </p>
          <div className="space-y-2">
            <div className="flex items-center gap-2.5 px-2">
              {avatarUrl ? (
                <img
                  src={avatarUrl}
                  alt=""
                  className="w-4 h-4 rounded-full object-cover"
                />
              ) : (
                <div className="w-4 h-4 rounded-full bg-[#5BA4A4]" />
              )}
              <span className="text-xs text-gray-500 truncate">
                {userName || "You"}{" "}
                <span className="text-gray-300">you</span>
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
