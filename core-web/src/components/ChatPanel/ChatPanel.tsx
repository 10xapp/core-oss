import { Suspense, lazy, useRef } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { useUIStore } from '../../stores/uiStore';

const SidebarChat = lazy(() => import('../SidebarChat'));

export default function ChatPanel() {
  const isSidebarChatOpen = useUIStore((s) => s.isSidebarChatOpen);

  // Track if sidebar was already open on mount - skip animation if so
  const wasOpenOnMount = useRef(isSidebarChatOpen);

  return (
    <AnimatePresence>
      {isSidebarChatOpen && (
        <motion.div
          initial={wasOpenOnMount.current ? false : { width: 0, opacity: 0 }}
          animate={{ width: 340, opacity: 1 }}
          exit={{ width: 0, opacity: 0 }}
          transition={{ duration: 0.2, ease: [0.4, 0, 0.2, 1] }}
          className="h-full shrink-0 flex flex-col overflow-hidden rounded-lg bg-white border border-border-light shadow-[0_0_1px_rgba(0,0,0,0.25)]"
        >
          <Suspense
            fallback={
              <div className="h-full flex items-center justify-center">
                <div className="text-text-secondary text-sm">Loading...</div>
              </div>
            }
          >
            <SidebarChat />
          </Suspense>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
