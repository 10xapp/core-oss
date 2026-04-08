import { useState, useEffect, useCallback } from "react";
import { createPortal } from "react-dom";
import { motion, AnimatePresence } from "motion/react";
import { Turnstile } from "react-turnstile";
import { useAuthStore } from "../../stores/authStore";
import { API_BASE } from "../../lib/apiBase";

const TURNSTILE_SITE_KEY = import.meta.env.VITE_TURNSTILE_SITE_KEY || "";

/* ──────────────────── Styles ──────────────────── */

const heroAnimationStyles = `
@keyframes enter {
  from {
    transform: translateY(8px);
    filter: blur(5px);
    opacity: 0;
  }
}

.animate-enter {
  animation: enter 800ms cubic-bezier(0.25, 0.46, 0.45, 0.94) both;
  animation-delay: calc(var(--delay, 0) * var(--stagger, 0));
}
`;

/* ──────────────────── Icons ──────────────────── */

function GoogleIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path d="M19.6 10.23c0-.68-.06-1.36-.17-2.02H10v3.82h5.38a4.6 4.6 0 0 1-2 3.02v2.5h3.24c1.89-1.74 2.98-4.3 2.98-7.32Z" fill="#4285F4" />
      <path d="M10 20c2.7 0 4.96-.89 6.62-2.42l-3.24-2.5c-.9.6-2.04.95-3.38.95-2.6 0-4.8-1.76-5.58-4.12H1.07v2.58A9.99 9.99 0 0 0 10 20Z" fill="#34A853" />
      <path d="M4.42 11.91A6.01 6.01 0 0 1 4.1 10c0-.66.11-1.3.32-1.91V5.51H1.07A9.99 9.99 0 0 0 0 10c0 1.61.39 3.14 1.07 4.49l3.35-2.58Z" fill="#FBBC05" />
      <path d="M10 3.98c1.47 0 2.78.5 3.82 1.5l2.86-2.86C14.96.99 12.7 0 10 0A9.99 9.99 0 0 0 1.07 5.51l3.35 2.58C5.2 5.74 7.4 3.98 10 3.98Z" fill="#EA4335" />
    </svg>
  );
}

function MicrosoftIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 21 21" fill="none" aria-hidden="true">
      <rect x="1" y="1" width="9" height="9" fill="#F25022" />
      <rect x="11" y="1" width="9" height="9" fill="#7FBA00" />
      <rect x="1" y="11" width="9" height="9" fill="#00A4EF" />
      <rect x="11" y="11" width="9" height="9" fill="#FFB900" />
    </svg>
  );
}

/* ──────────────────── Sign-in modal ──────────────────── */

function SignInModal({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
  const { signInWithGoogle, signInWithMicrosoft } = useAuthStore();
  const [turnstileToken, setTurnstileToken] = useState<string | null>(null);
  const [verifying, setVerifying] = useState(false);

  const hasTurnstile = !!TURNSTILE_SITE_KEY;

  // Reset token when modal closes
  useEffect(() => {
    if (!isOpen) setTurnstileToken(null);
  }, [isOpen]);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    if (isOpen) {
      document.addEventListener("keydown", handleEscape);
      return () => document.removeEventListener("keydown", handleEscape);
    }
  }, [isOpen, onClose]);

  const handleSignIn = useCallback(async (provider: "google" | "microsoft") => {
    if (hasTurnstile) {
      if (!turnstileToken) return;
      setVerifying(true);
      try {
        const res = await fetch(`${API_BASE.replace("/api", "")}/api/auth/verify-turnstile`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token: turnstileToken }),
        });
        if (!res.ok) {
          setTurnstileToken(null);
          return;
        }
      } catch {
        setTurnstileToken(null);
        return;
      } finally {
        setVerifying(false);
      }
    }
    if (provider === "google") signInWithGoogle();
    else signInWithMicrosoft();
  }, [hasTurnstile, turnstileToken, signInWithGoogle, signInWithMicrosoft]);

  const buttonsDisabled = hasTurnstile && (!turnstileToken || verifying);

  return createPortal(
    <AnimatePresence>
      {isOpen && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.15 }}
          className="fixed inset-0 z-[100] flex items-center justify-center"
        >
          <div className="absolute inset-0 bg-black/50 backdrop-blur-sm" onClick={onClose} />
          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 0.95 }}
            transition={{ duration: 0.15 }}
            className="relative bg-white rounded-lg p-8 shadow-2xl w-[384px] max-w-[calc(100%-2rem)]"
          >
            <button
              onClick={onClose}
              className="absolute top-4 right-4 text-text-tertiary hover:text-text-body transition-colors"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M18 6L6 18M6 6l12 12" />
              </svg>
            </button>
            <h2 className="text-xl font-semibold text-text-body mb-2">Sign in to Core</h2>
            <p className="text-text-tertiary text-sm mb-6">Choose your account to continue</p>
            <div className="space-y-3">
              <button
                onClick={() => handleSignIn("google")}
                disabled={buttonsDisabled}
                className="w-full inline-flex items-center justify-center gap-3 px-4 py-3 bg-text-body text-white rounded-xl text-base font-medium hover:bg-black/80 transition-all disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <GoogleIcon />
                Continue with Google
              </button>
              <button
                onClick={() => handleSignIn("microsoft")}
                disabled={buttonsDisabled}
                className="w-full inline-flex items-center justify-center gap-3 px-4 py-3 border border-black/20 text-text-body rounded-xl text-base font-medium hover:bg-black/5 transition-all disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <MicrosoftIcon />
                Continue with Microsoft
              </button>
            </div>
            {hasTurnstile && (
              <div className="mt-4 flex justify-center">
                <Turnstile
                  sitekey={TURNSTILE_SITE_KEY}
                  onVerify={(token: string) => setTurnstileToken(token)}
                  onExpire={() => setTurnstileToken(null)}
                  theme="light"
                  size="flexible"
                />
              </div>
            )}
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>,
    document.body
  );
}

/* ──────────────────── Main Component ──────────────────── */

export default function LandingPage() {
  const [showSignInModal, setShowSignInModal] = useState(false);
  const [activeApp, setActiveApp] = useState<'chat' | 'messages' | 'projects' | 'email' | 'calendar'>('chat');

  return (
    <div className="h-screen overflow-hidden bg-white fixed inset-0 z-50 flex flex-col">
      <style>{heroAnimationStyles}</style>

      {/* ────────── Header ────────── */}
      <nav className="shrink-0 px-6 sm:px-12 lg:px-40 h-16 flex items-center justify-between max-w-[1400px] mx-auto w-full">
        <div className="flex items-center gap-2.5">
          <img src="/CoreLogo.png" alt="Core" className="w-7 h-7" />
          <span className="font-semibold text-[17px] text-text-body tracking-tight">Core</span>
        </div>
        <button
          onClick={() => setShowSignInModal(true)}
          className="px-4 py-2 bg-text-body text-white rounded-sm text-sm font-medium hover:bg-black/80 transition-all active:scale-[0.98]"
        >
          Get started
        </button>
      </nav>

      {/* ────────── Hero ────────── */}
      <div className="flex-1 flex items-center px-6 sm:px-12 lg:px-40 max-w-[1400px] mx-auto w-full">
        <div className="grid grid-cols-1 md:grid-cols-[auto_1fr] gap-12 items-center w-full">
          {/* Left: Text */}
          <div style={{ "--delay": "80ms" } as React.CSSProperties}>
            <h1 className="text-3xl font-medium text-text-body tracking-tight leading-[1.1] mb-4 whitespace-nowrap">
              {["Everything", "in", "one", "place."].map((word, i) => (
                <span
                  key={word}
                  style={{ "--stagger": i } as React.CSSProperties}
                  className="inline-block mr-[0.3em] animate-enter"
                >
                  {word}
                </span>
              ))}
            </h1>
            <p
              style={{ "--stagger": 5 } as React.CSSProperties}
              className="text-base text-text-tertiary mb-6 animate-enter"
            >
              The workspace for people who value productivity.
            </p>
            <button
              style={{ "--stagger": 6 } as React.CSSProperties}
              onClick={() => setShowSignInModal(true)}
              className="px-4 py-2 bg-text-body text-white rounded-sm text-sm font-medium hover:bg-black/80 transition-all active:scale-[0.98] animate-enter"
            >
              Get started
            </button>
          </div>

          {/* Right: App Mockup */}
          <div
            style={{ "--delay": "80ms", "--stagger": 8 } as React.CSSProperties}
            className="hidden md:block animate-enter"
          >
            <div className="rounded-xl border border-black/8 shadow-xl shadow-black/6 overflow-hidden bg-bg-sidebar max-w-[560px] ml-auto">
              <div className="aspect-16/10 flex">
                {/* Light sidebar */}
                <div className="w-9 bg-bg-sidebar flex flex-col items-center py-2 gap-px shrink-0">
                  <div className="w-5 h-5 rounded bg-black/10 flex items-center justify-center mb-1">
                    <div className="w-2.5 h-2.5 rounded-sm bg-black/60" />
                  </div>
                  {[
                    { id: 'chat', icon: (isActive: boolean) => <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke={isActive ? 'black' : 'rgba(0,0,0,0.4)'} strokeWidth="1.5"><path d="M7.99902 12.4971H15.499M8.00391 7.49982L11.499 7.49707M19.9983 2H4C2.89543 2 2 2.89543 2 4V15.9971C2 17.1016 2.89543 17.9971 4 17.9971H5.99902V20.0333C5.99902 20.8506 6.92623 21.3227 7.58719 20.842L11.499 17.9971H19.9983C21.1029 17.9971 21.9983 17.1016 21.9983 15.9971V4C21.9983 2.89543 21.1029 2 19.9983 2Z"/></svg> },
                    { id: 'messages', icon: (isActive: boolean) => <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke={isActive ? 'black' : 'rgba(0,0,0,0.4)'} strokeWidth="1.5"><path d="M13 7C13 9.20914 11.2091 11 9 11C6.79086 11 5 9.20914 5 7C5 4.79086 6.79086 3 9 3C11.2091 3 13 4.79086 13 7Z"/><path d="M15 11C17.2091 11 19 9.20914 19 7C19 4.79086 17.2091 3 15 3"/><path d="M16 19C16 16.2386 13.7614 14 11 14H7C4.23858 14 2 16.2386 2 19V21H16V19Z"/><path d="M19 21H22V19C22 16.2386 19.7614 14 17 14"/></svg> },
                    { id: 'projects', icon: (isActive: boolean) => <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke={isActive ? 'black' : 'rgba(0,0,0,0.4)'} strokeWidth="1.5"><path d="M3 21L8.5 15.5"/><path d="M13.3625 7.39582L15.7087 3.4855C15.8894 3.1843 16.2158 2.99365 16.5657 3.02451C19.0645 3.2449 20.7551 4.93548 20.9755 7.43434C21.0064 7.78425 20.8157 8.11058 20.5145 8.2913L16.6043 10.6374C16.2419 10.8549 16.0553 11.2766 16.1382 11.691L16.4198 13.0989C16.7856 14.9278 16.28 16.8229 15.0517 18.2266L14.2039 19.1956C13.8229 19.6309 13.1532 19.6532 12.7442 19.2442L4.7554 11.2554C4.34652 10.8465 4.36862 10.1771 4.80359 9.79608L5.769 8.95041C7.17262 7.72088 9.06839 7.21437 10.8982 7.58001L12.3091 7.86194C12.7234 7.94473 13.1451 7.75815 13.3625 7.39582Z"/></svg> },
                    { id: 'email', icon: (isActive: boolean) => <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke={isActive ? 'black' : 'rgba(0,0,0,0.4)'} strokeWidth="1.5"><rect x="2" y="4" width="20" height="16" rx="2"/><path d="m22 7-8.97 5.7a1.94 1.94 0 0 1-2.06 0L2 7"/></svg> },
                    { id: 'calendar', icon: (isActive: boolean) => <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke={isActive ? 'black' : 'rgba(0,0,0,0.4)'} strokeWidth="1.5"><rect x="3" y="4" width="18" height="18" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/></svg> },
                  ].map((app) => (
                    <div
                      key={app.id}
                      onMouseEnter={() => setActiveApp(app.id as typeof activeApp)}
                      className={`w-6 h-6 rounded flex items-center justify-center cursor-pointer transition-colors ${activeApp === app.id ? 'bg-black/10' : 'hover:bg-black/5'}`}
                    >
                      {app.icon(activeApp === app.id)}
                    </div>
                  ))}
                </div>

                {/* Main content */}
                <div className="flex-1 flex min-w-0 m-1 ml-0 rounded-lg overflow-hidden bg-bg-surface">
                      <AnimatePresence mode="wait">
                        {/* ── Chat ── */}
                        {activeApp === 'chat' && (
                          <motion.div
                            key="chat"
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            exit={{ opacity: 0 }}
                            transition={{ duration: 0.15 }}
                            className="flex-1 flex flex-col"
                          >
                            <div className="flex-1 flex flex-col bg-white">
                              <div className="flex-1 px-6 py-2 space-y-2 overflow-hidden flex flex-col justify-center">
                                <div className="flex justify-end">
                                  <div className="bg-bg-bubble rounded-[10px] px-2.5 py-1.5 max-w-[70%]">
                                    <p className="text-[9px] text-[#000]">What's on my schedule today?</p>
                                  </div>
                                </div>
                                <div className="max-w-[85%]">
                                  <p className="text-[9px] text-[#000] mb-1.5">You have 3 meetings today:</p>
                                  <div className="space-y-1">
                                    {[
                                      { time: "9:00 AM", title: "Team Standup", color: "#4285F4" },
                                      { time: "11:30 AM", title: "Design Review", color: "#34A853" },
                                      { time: "2:00 PM", title: "1:1 with Sarah", color: "#9333EA" },
                                    ].map((evt) => (
                                      <div key={evt.title} className="flex items-center gap-1.5 bg-gray-50 rounded px-2 py-1">
                                        <div className="w-0.5 h-4 rounded-full shrink-0" style={{ backgroundColor: evt.color }} />
                                        <div>
                                          <p className="text-[7px] text-text-tertiary">{evt.time}</p>
                                          <p className="text-[8px] font-medium text-[#000]">{evt.title}</p>
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                                <div className="flex justify-end">
                                  <div className="bg-bg-bubble rounded-[10px] px-2.5 py-1.5 max-w-[70%]">
                                    <p className="text-[9px] text-[#000]">Draft a reply to Sarah's email</p>
                                  </div>
                                </div>
                              </div>
                              <div className="px-6 pb-2 shrink-0">
                                <div className="border border-border-gray rounded-lg px-2.5 py-1.5 flex items-center gap-1.5">
                                  <span className="text-[8px] text-text-tertiary flex-1">Message Core...</span>
                                  <div className="w-3.5 h-3.5 bg-black/8 rounded flex items-center justify-center">
                                    <svg className="w-2 h-2 text-text-tertiary" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3"><path d="M12 19V5M5 12l7-7 7 7"/></svg>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </motion.div>
                        )}
                        {/* ── Messages ── */}
                        {activeApp === 'messages' && (
                          <motion.div
                            key="messages"
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            exit={{ opacity: 0 }}
                            transition={{ duration: 0.15 }}
                            className="flex-1 flex"
                          >
                            <div className="w-[80px] shrink-0 flex flex-col bg-bg-panel border-r border-black/[0.04]">
                              <div className="h-7 flex items-center px-2 shrink-0">
                                <h2 className="text-[9px] font-semibold">Messages</h2>
                              </div>
                              <div className="flex-1 px-1.5">
                                <div className="text-[6px] font-medium text-text-tertiary uppercase px-1 mb-0.5">Channels</div>
                                <div className="space-y-px">
                                  {["general", "core-app", "design", "random"].map((ch, i) => (
                                    <div key={ch} className={`px-1.5 py-1 rounded text-[8px] ${i === 0 ? 'bg-white font-medium' : 'text-text-secondary'}`}>
                                      # {ch}
                                    </div>
                                  ))}
                                </div>
                                <div className="text-[6px] font-medium text-text-tertiary uppercase px-1 mt-2 mb-0.5">DMs</div>
                                <div className="space-y-px">
                                  {[{ name: "Sarah", color: "#7C3AED" }, { name: "Mike", color: "#2563EB" }].map((dm) => (
                                    <div key={dm.name} className="px-1.5 py-1 rounded text-[8px] text-text-secondary flex items-center gap-1">
                                      <div className="w-2.5 h-2.5 rounded-full flex items-center justify-center text-white text-[5px] font-semibold shrink-0" style={{ backgroundColor: dm.color }}>{dm.name[0]}</div>
                                      {dm.name}
                                    </div>
                                  ))}
                                </div>
                              </div>
                            </div>
                            <div className="flex-1 flex flex-col bg-white">
                              <div className="h-7 border-b border-border-gray flex items-center px-2">
                                <span className="text-[9px] font-medium"># general</span>
                              </div>
                              <div className="flex-1 px-2 py-1.5 space-y-1.5">
                                {[
                                  { name: "Sarah", msg: "Sounds good! I'll review it tonight.", avatar: "S", color: "#7C3AED" },
                                  { name: "Mike", msg: "Shared the new designs in the project", avatar: "M", color: "#2563EB" },
                                  { name: "Alex", msg: "Let's discuss at standup tomorrow", avatar: "A", color: "#DC2626" },
                                ].map((m) => (
                                  <div key={m.name} className="flex items-start gap-1.5">
                                    <div className="w-4 h-4 rounded-[3px] flex items-center justify-center text-white text-[6px] font-semibold shrink-0" style={{ backgroundColor: m.color }}>{m.avatar}</div>
                                    <div className="min-w-0">
                                      <p className="text-[7px] font-semibold">{m.name}</p>
                                      <p className="text-[8px] text-text-secondary truncate">{m.msg}</p>
                                    </div>
                                  </div>
                                ))}
                              </div>
                              <div className="px-2 pb-1.5 shrink-0">
                                <div className="border border-border-gray rounded-lg px-2 py-1.5 flex items-center gap-1.5">
                                  <span className="text-[8px] text-text-tertiary flex-1">Message # general...</span>
                                  <div className="w-3.5 h-3.5 bg-black/8 rounded flex items-center justify-center">
                                    <svg className="w-2 h-2 text-text-tertiary" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3"><path d="M12 19V5M5 12l7-7 7 7"/></svg>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </motion.div>
                        )}
                        {/* ── Email ── */}
                        {activeApp === 'email' && (
                          <motion.div
                            key="email"
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            exit={{ opacity: 0 }}
                            transition={{ duration: 0.15 }}
                            className="flex-1 flex"
                          >
                            <div className="w-[80px] shrink-0 flex flex-col bg-bg-panel border-r border-black/[0.04]">
                              <div className="h-7 flex items-center px-2">
                                <h2 className="text-[9px] font-semibold">Email</h2>
                              </div>
                              <div className="px-1 space-y-px">
                                {[
                                  { name: "Inbox", count: 3 },
                                  { name: "Flagged", count: 0 },
                                  { name: "Sent", count: 0 },
                                  { name: "Drafts", count: 0 },
                                ].map((f, i) => (
                                  <div key={f.name} className={`px-1.5 py-1 rounded text-[7px] flex items-center justify-between ${i === 0 ? 'bg-white font-medium' : 'text-text-secondary'}`}>
                                    <span className="truncate">{f.name}</span>
                                    {f.count > 0 && <span className="text-[6px] font-semibold">{f.count}</span>}
                                  </div>
                                ))}
                              </div>
                            </div>
                            <div className="w-[80px] shrink-0 flex flex-col bg-white border-r border-black/[0.04]">
                              <div className="h-7 border-b border-border-gray flex items-center px-2">
                                <span className="text-[9px] font-medium">Inbox</span>
                              </div>
                              <div className="flex-1">
                                {[
                                  { from: "Sarah Chen", subject: "Q1 Report review", snippet: "Hey, can you take a look...", unread: true, selected: true },
                                  { from: "Mike Johnson", subject: "Updated designs", snippet: "New mockups attached", unread: true, selected: false },
                                  { from: "Alex Rivera", subject: "API changes", snippet: "Pushed the new endpoints", unread: false, selected: false },
                                ].map((e) => (
                                  <div key={e.subject} className={`px-2 py-1.5 border-b border-black/[0.04] ${e.selected ? 'bg-black/[0.03]' : ''}`}>
                                    <div className="flex items-center gap-1">
                                      {e.unread && <div className="w-1 h-1 rounded-full bg-blue-600 shrink-0" />}
                                      <p className={`text-[7px] truncate ${e.unread ? 'font-semibold' : ''}`}>{e.from}</p>
                                    </div>
                                    <p className="text-[6.5px] truncate font-medium">{e.subject}</p>
                                    <p className="text-[6px] text-text-tertiary truncate">{e.snippet}</p>
                                  </div>
                                ))}
                              </div>
                            </div>
                            <div className="flex-1 flex flex-col bg-white">
                              <div className="h-7 border-b border-border-gray" />
                              <div className="flex-1 px-2 py-2 overflow-hidden">
                                <p className="text-[9px] font-semibold mb-1">Q1 Report review</p>
                                <div className="flex items-center gap-1 mb-2">
                                  <div className="w-3 h-3 rounded-full bg-[#7C3AED] flex items-center justify-center text-white text-[5px] font-semibold shrink-0">S</div>
                                  <div>
                                    <p className="text-[7px] font-medium">Sarah Chen</p>
                                    <p className="text-[6px] text-text-tertiary">to me</p>
                                  </div>
                                </div>
                                <p className="text-[7px] text-text-secondary leading-relaxed">Hey, can you take a look at the Q1 report when you get a chance? I've attached the latest version with the updated numbers.</p>
                              </div>
                            </div>
                          </motion.div>
                        )}
                        {/* ── Calendar ── */}
                        {activeApp === 'calendar' && (
                          <motion.div
                            key="calendar"
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            exit={{ opacity: 0 }}
                            transition={{ duration: 0.15 }}
                            className="flex-1 flex"
                          >
                            <div className="w-[80px] shrink-0 flex flex-col bg-bg-panel border-r border-black/[0.04]">
                              <div className="h-7 flex items-center px-2">
                                <h2 className="text-[9px] font-semibold">Calendar</h2>
                              </div>
                              <div className="px-1.5">
                                <div className="text-[6px] font-medium text-text-tertiary uppercase px-0.5 mb-0.5">Accounts</div>
                                {[{ name: "Work", color: "#35A9DD" }, { name: "Personal", color: "#34A853" }].map((acc) => (
                                  <div key={acc.name} className="flex items-center gap-1 px-0.5 py-0.5">
                                    <div className="w-1.5 h-1.5 rounded-sm shrink-0" style={{ backgroundColor: acc.color }} />
                                    <span className="text-[7px] text-text-secondary">{acc.name}</span>
                                  </div>
                                ))}
                              </div>
                            </div>
                            <div className="flex-1 flex flex-col bg-white">
                              <div className="h-7 border-b border-border-gray flex items-center justify-between px-2">
                                <span className="text-[9px] font-medium">Sun, Mar 8</span>
                                <div className="flex gap-0.5">
                                  {["D", "W", "M"].map((v, i) => (
                                    <span key={v} className={`text-[6px] px-1 py-0.5 rounded ${i === 0 ? 'bg-black/[0.06] font-medium' : 'text-text-tertiary'}`}>{v}</span>
                                  ))}
                                </div>
                              </div>
                              <div className="flex-1 flex overflow-hidden">
                                <div className="w-6 shrink-0 flex flex-col">
                                  {["8a", "9a", "10a", "11a", "12p", "1p", "2p", "3p", "4p"].map((t) => (
                                    <div key={t} className="flex-1 text-[5px] text-text-tertiary text-right pr-1 pt-px">{t}</div>
                                  ))}
                                </div>
                                <div className="flex-1 relative border-l border-black/[0.04]">
                                  {[0, 1, 2, 3, 4, 5, 6, 7, 8].map((h) => (
                                    <div key={h} className="h-[11.11%] border-b border-black/[0.04]" />
                                  ))}
                                  {/* Team Standup: 9a-10a */}
                                  <div className="absolute left-1 right-1 rounded-md overflow-hidden" style={{ top: '11.11%', height: '11.11%', backgroundColor: '#D6EFF8' }}>
                                    <div className="h-full flex items-start gap-1.5 px-1.5 py-1">
                                      <div className="w-[3px] rounded-full self-stretch shrink-0" style={{ backgroundColor: '#35A9DD' }} />
                                      <div className="min-w-0">
                                        <p className="text-[7px] font-medium truncate" style={{ color: '#19556E' }}>Team Standup</p>
                                        <p className="text-[6px] truncate" style={{ color: '#2680A5' }}>9 – 10 AM</p>
                                      </div>
                                    </div>
                                  </div>
                                  {/* Design Review: 11a-12:30p */}
                                  <div className="absolute left-1 right-1 rounded-md overflow-hidden" style={{ top: '33.33%', height: '16.66%', backgroundColor: '#D6F5DD' }}>
                                    <div className="h-full flex items-start gap-1.5 px-1.5 py-1">
                                      <div className="w-[3px] rounded-full self-stretch shrink-0" style={{ backgroundColor: '#34A853' }} />
                                      <div className="min-w-0">
                                        <p className="text-[7px] font-medium truncate" style={{ color: '#1A5C2B' }}>Design Review</p>
                                        <p className="text-[6px] truncate" style={{ color: '#268040' }}>11 – 12:30</p>
                                      </div>
                                    </div>
                                  </div>
                                  {/* 1:1 with Sarah: 2p-3p */}
                                  <div className="absolute left-1 right-1 rounded-md overflow-hidden" style={{ top: '66.66%', height: '11.11%', backgroundColor: '#E8DEF8' }}>
                                    <div className="h-full flex items-start gap-1.5 px-1.5 py-1">
                                      <div className="w-[3px] rounded-full self-stretch shrink-0" style={{ backgroundColor: '#9333EA' }} />
                                      <div className="min-w-0">
                                        <p className="text-[7px] font-medium truncate" style={{ color: '#4A1B6D' }}>1:1 with Sarah</p>
                                        <p className="text-[6px] truncate" style={{ color: '#7028B5' }}>2 – 3 PM</p>
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </motion.div>
                        )}
                        {/* ── Projects ── */}
                        {activeApp === 'projects' && (
                          <motion.div
                            key="projects"
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            exit={{ opacity: 0 }}
                            transition={{ duration: 0.15 }}
                            className="flex-1 flex"
                          >
                            <div className="w-[80px] shrink-0 flex flex-col bg-bg-panel border-r border-black/[0.04]">
                              <div className="h-7 flex items-center px-2">
                                <h2 className="text-[9px] font-semibold">Projects</h2>
                              </div>
                              <div className="px-1 space-y-px">
                                {["Core Web", "Mobile App", "Marketing"].map((p, i) => (
                                  <div key={p} className={`px-2 py-1 rounded text-[7px] truncate flex items-center gap-1 ${i === 0 ? 'bg-white font-medium' : 'text-text-secondary'}`}>
                                    <svg className="w-2.5 h-2.5 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                      <path d="M19 21C20.1046 21 21 20.1046 21 19V5C21 3.89543 20.1046 3 19 3H5C3.89543 3 3 3.89543 3 5V19C3 20.1046 3.89543 21 5 21H19Z" />
                                      <path d="M12 7V11M17 7V17M7 7V14" />
                                    </svg>
                                    {p}
                                  </div>
                                ))}
                              </div>
                            </div>
                            <div className="flex-1 flex flex-col">
                              <div className="h-7 border-b border-black/[0.04] flex items-center gap-1 px-1.5 bg-white">
                                <svg className="w-2.5 h-2.5 text-text-body" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M19 21C20.1046 21 21 20.1046 21 19V5C21 3.89543 20.1046 3 19 3H5C3.89543 3 3 3.89543 3 5V19C3 20.1046 3.89543 21 5 21H19Z" />
                                  <path d="M12 7V11M17 7V17M7 7V14" />
                                </svg>
                                <span className="text-[9px] font-medium">Core Web</span>
                              </div>
                              <div className="flex-1 flex gap-1 p-1.5 bg-white overflow-hidden">
                                {[
                                  { title: "To Do", color: "#6B7280", cards: [
                                    { title: "Update onboarding flow", labels: ["#2563EB"] },
                                    { title: "Add dark mode", labels: ["#7C3AED"] },
                                  ]},
                                  { title: "In Progress", color: "#3B82F6", cards: [
                                    { title: "Review PR #142", labels: ["#EA4335", "#F59E0B"] },
                                  ]},
                                  { title: "Done", color: "#22C55E", cards: [
                                    { title: "Setup CI/CD", labels: ["#34A853"] },
                                    { title: "Auth flow", labels: ["#2563EB"] },
                                  ]},
                                ].map((col) => (
                                  <div key={col.title} className="flex-1 min-w-0 flex flex-col bg-bg-panel rounded p-1">
                                    <div className="flex items-center gap-1 mb-1">
                                      <div className="w-1.5 h-1.5 rounded-full shrink-0" style={{ backgroundColor: col.color }} />
                                      <span className="text-[6px] font-medium text-text-secondary truncate">{col.title}</span>
                                      <span className="text-[6px] text-text-tertiary">{col.cards.length}</span>
                                    </div>
                                    <div className="space-y-1">
                                      {col.cards.map((card) => (
                                        <div key={card.title} className="bg-white rounded p-1.5 border border-black/[0.04]">
                                          <p className="text-[7px] font-medium leading-tight">{card.title}</p>
                                          <div className="flex items-center gap-1 mt-1">
                                            {card.labels.map((color, li) => (
                                              <div key={li} className="w-1 h-1 rounded-full" style={{ backgroundColor: color }} />
                                            ))}
                                          </div>
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          </motion.div>
                        )}
                </AnimatePresence>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* ────────── Footer ────────── */}
      <footer className="shrink-0 px-6 sm:px-12 lg:px-40 py-4 max-w-[1400px] mx-auto w-full">
        <span className="text-text-tertiary text-sm">
          Core &copy; 2026
        </span>
      </footer>

      <SignInModal isOpen={showSignInModal} onClose={() => setShowSignInModal(false)} />
    </div>
  );
}
