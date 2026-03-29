import { useState, useEffect, useRef, useCallback } from "react";
import { motion } from "motion/react";
import { ClockIcon } from "@heroicons/react/24/outline";
import { Brain } from "lucide-react";
import { Icon } from "../ui/Icon";
import {
  invokeAgent,
  getConversationTasks,
  type AgentInstance,
  type AgentTask,
  type AgentConversation,
} from "../../api/client";
import { useAgentConversationStore } from "../../stores/agentConversationStore";
import {
  useAgentConversationsRealtime,
  useConversationTasksRealtime,
} from "../../hooks/useAgentRealtime";
import AgentChatInput from "./AgentChatInput";
import AgentChatMessage from "./AgentChatMessage";
import AgentConversationSidebar from "./AgentConversationSidebar";
import { SIDEBAR } from "../../lib/sidebar";

interface AgentChatProps {
  agent: AgentInstance;
}

export default function AgentChat({ agent }: AgentChatProps) {
  const [tasks, setTasks] = useState<AgentTask[]>([]);
  const [loadingTasks, setLoadingTasks] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [sending, setSending] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const skipNextFetchRef = useRef(false);

  const {
    conversations,
    activeConversationId,
    fetchConversations,
    addConversation,
    removeConversation,
    updateTitle,
    setActiveConversationId,
    handleRealtimeInsert,
    handleRealtimeUpdate,
    handleRealtimeDelete,
  } = useAgentConversationStore();

  // Fetch conversations when agent changes
  useEffect(() => {
    fetchConversations(agent.id);
  }, [agent.id, fetchConversations]);

  // Realtime: conversation list updates
  useAgentConversationsRealtime(
    agent.id,
    useCallback((conv: AgentConversation) => handleRealtimeInsert(conv), [handleRealtimeInsert]),
    useCallback((conv: AgentConversation) => handleRealtimeUpdate(conv), [handleRealtimeUpdate]),
    useCallback((id: string) => handleRealtimeDelete(id), [handleRealtimeDelete]),
  );

  // Load tasks when active conversation changes
  useEffect(() => {
    if (!activeConversationId) {
      setTasks([]);
      return;
    }
    // Skip fetch when we just created a conversation inline (handleSend sets this)
    if (skipNextFetchRef.current) {
      skipNextFetchRef.current = false;
      return;
    }
    setLoadingTasks(true);
    getConversationTasks(agent.id, activeConversationId)
      .then((result) => setTasks(result.tasks))
      .catch(console.error)
      .finally(() => setLoadingTasks(false));
  }, [agent.id, activeConversationId]);

  // Realtime: task updates scoped to active conversation
  useConversationTasksRealtime(
    activeConversationId,
    useCallback((newTask: AgentTask) => {
      setTasks((prev) => {
        if (prev.some((t) => t.id === newTask.id)) return prev;
        return [...prev, newTask];
      });
    }, []),
    useCallback((updatedTask: AgentTask) => {
      setTasks((prev) => prev.map((t) => (t.id === updatedTask.id ? updatedTask : t)));
    }, []),
  );

  // Poll for task updates while any task is active (fallback for realtime latency)
  const hasActiveTasks = tasks.some((t) => t.status === "running" || t.status === "queued");
  useEffect(() => {
    if (!activeConversationId || !hasActiveTasks) return;

    const interval = setInterval(async () => {
      try {
        const result = await getConversationTasks(agent.id, activeConversationId);
        setTasks(result.tasks);
      } catch {
        // ignore – realtime or next poll will catch up
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [agent.id, activeConversationId, hasActiveTasks]);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [tasks.length, tasks[tasks.length - 1]?.status]);

  const handleSend = async (message: string) => {
    if (sending) return;
    setSending(true);

    try {
      let conversationId = activeConversationId;

      // Auto-create conversation if none active
      if (!conversationId) {
        const title = message.length > 40 ? message.slice(0, 40) + "..." : message;
        skipNextFetchRef.current = true; // Don't let useEffect wipe our optimistic task
        const conv = await addConversation(agent.id, title);
        conversationId = conv.id;
      }

      const task = await invokeAgent(agent.id, message, undefined, conversationId);
      // Optimistically add
      setTasks((prev) => {
        if (prev.some((t) => t.id === task.id)) return prev;
        return [...prev, task];
      });
    } catch (err) {
      console.error("Failed to send message:", err);
    } finally {
      setSending(false);
    }
  };

  const handleNewConversation = async () => {
    try {
      await addConversation(agent.id);
      setTasks([]);
    } catch (err) {
      console.error("Failed to create conversation:", err);
    }
  };

  const isAgentWorking = tasks.some((t) => t.status === "running" || t.status === "queued");

  return (
    <div className="flex-1 flex min-w-0 overflow-hidden">
      {/* Animated history sidebar */}
      <motion.div
        initial={false}
        animate={{ width: showHistory ? 212 : 48 }}
        transition={{ duration: 0.2, ease: [0.4, 0, 0.2, 1] }}
        className={`h-full flex flex-col overflow-hidden shrink-0 ${
          showHistory ? `${SIDEBAR.bg} border-r border-black/5` : "bg-white"
        }`}
      >
        {showHistory ? (
          <>
            {/* Expanded header */}
            <div className="flex items-center justify-between px-3 pt-3 pb-1">
              <span className="text-xs font-medium text-text-secondary">History</span>
              <button
                onClick={() => setShowHistory(false)}
                className="p-1 rounded hover:bg-black/5 transition-colors"
                title="Hide history"
              >
                <ClockIcon className="w-4 h-4 stroke-2 text-text-tertiary" />
              </button>
            </div>
            <AgentConversationSidebar
              conversations={conversations}
              activeConversationId={activeConversationId}
              onSelect={setActiveConversationId}
              onCreate={handleNewConversation}
              onRename={(id, title) => updateTitle(agent.id, id, title)}
              onDelete={(id) => removeConversation(agent.id, id)}
            />
          </>
        ) : (
          /* Collapsed — just the clock icon */
          <div className="flex flex-col items-center pt-3 gap-2">
            <button
              onClick={() => setShowHistory(true)}
              className="p-2 rounded-lg hover:bg-black/5 transition-colors"
              title="Show history"
            >
              <ClockIcon className="w-4 h-4 stroke-2 text-text-tertiary" />
            </button>
          </div>
        )}
      </motion.div>

      {/* Chat area */}
      <div className="flex-1 flex flex-col min-w-0 bg-white">
        {activeConversationId ? (
          <>
            {/* Messages */}
            <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
              {loadingTasks ? (
                <div className="flex items-center justify-center h-full">
                  <p className="text-sm text-text-tertiary">Loading...</p>
                </div>
              ) : tasks.length === 0 ? (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center">
                    <Icon
                      icon={Brain}
                      size={28}
                      className="mx-auto text-text-tertiary opacity-40 mb-2"
                    />
                    <p className="text-sm text-text-tertiary">Start a conversation with {agent.name}</p>
                  </div>
                </div>
              ) : (
                <>
                  {tasks.map((task) => (
                    <AgentChatMessage
                      key={task.id}
                      task={task}
                      agentId={agent.id}
                      agentName={agent.name}
                    />
                  ))}
                  <div ref={messagesEndRef} />
                </>
              )}
            </div>

            {/* Input */}
            <AgentChatInput
              onSend={handleSend}
              disabled={sending || isAgentWorking}
              placeholder={isAgentWorking ? `${agent.name} is working...` : `Message ${agent.name}...`}
            />
          </>
        ) : (
          /* No conversation selected */
          <div className="flex-1 flex flex-col">
            <div className="flex-1 flex items-center justify-center">
              <div className="text-center">
                <Icon
                  icon={Brain}
                  size={32}
                  className="mx-auto text-text-tertiary opacity-40 mb-3"
                />
                <p className="text-sm text-text-tertiary">
                  {conversations.length === 0
                    ? `Start a conversation with ${agent.name}`
                    : "Select a conversation or start a new one"}
                </p>
              </div>
            </div>
            <AgentChatInput
              onSend={handleSend}
              disabled={sending}
              placeholder={`Message ${agent.name}...`}
            />
          </div>
        )}
      </div>
    </div>
  );
}
