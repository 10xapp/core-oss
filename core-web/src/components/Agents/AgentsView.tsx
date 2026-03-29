import { useState, useEffect, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { Plus, Brain } from "lucide-react";
import { Icon } from "../ui/Icon";
import { useWorkspaceStore } from "../../stores/workspaceStore";
import {
  getWorkspaceAgents,
  type AgentInstance,
} from "../../api/client";
import { useAgentStatusRealtime } from "../../hooks/useAgentRealtime";
import TemplateStore from "./TemplateStore";
import AgentChat from "./AgentChat";
import AgentConfigPanel from "./AgentConfigPanel";
import { useAgentConversationStore } from "../../stores/agentConversationStore";
import { SIDEBAR } from "../../lib/sidebar";

export default function AgentsView() {
  const { workspaceId, agentId } = useParams<{ workspaceId: string; agentId?: string }>();
  const navigate = useNavigate();
  const workspaces = useWorkspaceStore((s) => s.workspaces);
  const workspace = workspaces.find((w) => w.id === workspaceId);

  const [agents, setAgents] = useState<AgentInstance[]>([]);
  const [selectedAgent, setSelectedAgent] = useState<AgentInstance | null>(null);
  const [loading, setLoading] = useState(true);
  const [showTemplateStore, setShowTemplateStore] = useState(false);

  const loadAgents = useCallback(async () => {
    if (!workspaceId) return;
    try {
      const result = await getWorkspaceAgents(workspaceId);
      setAgents(result.agents);
    } catch (err) {
      console.error("Failed to load agents:", err);
    } finally {
      setLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    loadAgents();
  }, [loadAgents]);

  // Sync selected agent from URL param once agents are loaded
  useEffect(() => {
    if (!agentId || agents.length === 0) return;
    const match = agents.find((a) => a.id === agentId);
    if (match && match.id !== selectedAgent?.id) {
      setSelectedAgent(match);
    }
  }, [agentId, agents]);

  // Realtime: agent instance updates (status, sandbox_status, last_active_at)
  useAgentStatusRealtime(
    workspaceId ?? null,
    useCallback((updated: AgentInstance) => {
      setAgents((prev) => prev.map((a) => (a.id === updated.id ? updated : a)));
      setSelectedAgent((prev) => (prev?.id === updated.id ? updated : prev));
    }, []),
  );

  const sandboxStatusIndicator = (agent: AgentInstance) => {
    const ss = agent.sandbox_status || "off";
    switch (ss) {
      case "running": return "bg-green-400";
      case "starting": return "bg-yellow-400 animate-pulse";
      case "error": return "bg-red-400";
      default: return "bg-gray-300";
    }
  };

  const selectAgent = (agent: AgentInstance) => {
    // Clear stale conversation state synchronously before React re-renders
    const store = useAgentConversationStore.getState();
    if (store.currentAgentId !== agent.id) {
      useAgentConversationStore.setState({ activeConversationId: null, conversations: [] });
    }
    setSelectedAgent(agent);
    navigate(`/workspace/${workspaceId}/agents/${agent.id}`, { replace: true });
  };

  const handleAgentUpdate = (updated: AgentInstance) => {
    setAgents((prev) => prev.map((a) => (a.id === updated.id ? updated : a)));
    setSelectedAgent(updated);
  };

  const handleAgentDelete = () => {
    if (!selectedAgent) return;
    setAgents((prev) => prev.filter((a) => a.id !== selectedAgent.id));
    setSelectedAgent(null);
    navigate(`/workspace/${workspaceId}/agents`, { replace: true });
  };

  if (!workspace) {
    return (
      <div className="flex-1 flex items-center justify-center bg-background-primary">
        <p className="text-text-tertiary">Workspace not found</p>
      </div>
    );
  }

  return (
    <div className="flex-1 flex h-full overflow-hidden">
      <div className="flex-1 flex overflow-hidden bg-bg-mini-app">
        {/* Agent list sidebar */}
        <div className={`w-[212px] shrink-0 flex flex-col overflow-hidden ${SIDEBAR.bg} border-r border-black/5`}>
          {/* Header */}
          <div className="h-12 flex items-center justify-between pl-4 pr-2 shrink-0">
            <h2 className="text-base font-semibold text-text-body">Agents</h2>
            <button
              onClick={() => setShowTemplateStore(true)}
              className="p-1 rounded bg-white border border-black/10 hover:border-black/20 text-text-secondary hover:text-text-body transition-colors"
              title="New agent"
            >
              <Icon icon={Plus} size={16} />
            </button>
          </div>

          {/* Agents list */}
          <div className="flex-1 overflow-y-auto">
            {loading ? (
              <div className="px-4 py-8 text-center">
                <p className="text-sm text-text-tertiary">Loading...</p>
              </div>
            ) : agents.length === 0 ? (
              <div className="px-4 py-8 text-center">
                <Icon
                  icon={Brain}
                  size={32}
                  className="mx-auto text-text-tertiary opacity-50 mb-2"
                />
                <p className="text-sm text-text-tertiary">No agents yet</p>
                <button
                  onClick={() => setShowTemplateStore(true)}
                  className="mt-3 text-sm text-brand-primary hover:underline font-medium"
                >
                  Deploy one
                </button>
              </div>
            ) : (
              <div className="px-2">
                <div className="space-y-0.5">
                  {agents.map((agent) => {
                    const role = (agent.config as { role?: string })?.role;
                    return (
                      <button
                        key={agent.id}
                        onClick={() => selectAgent(agent)}
                        className={`w-full flex items-center gap-2 px-3 rounded-md text-sm transition-colors cursor-pointer ${
                          role ? "py-1.5" : "h-[32px]"
                        } ${
                          selectedAgent?.id === agent.id
                            ? SIDEBAR.selected
                            : `${SIDEBAR.item} hover:bg-black/5`
                        }`}
                      >
                        {agent.avatar_url ? (
                          <img src={agent.avatar_url} className="w-5 h-5 rounded-full shrink-0 object-cover" />
                        ) : (
                          <div className={`w-2 h-2 rounded-full shrink-0 ${sandboxStatusIndicator(agent)}`} />
                        )}
                        <div className="flex-1 min-w-0">
                          <span className="block text-left truncate">{agent.name}</span>
                          {role && (
                            <span className="block text-[10px] text-text-tertiary truncate leading-tight">
                              {role}
                            </span>
                          )}
                        </div>
                        {agent.status === "working" && (
                          <div className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse shrink-0" />
                        )}
                      </button>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Main content: Chat + Config panel */}
        <div className="flex-1 flex min-w-0 overflow-hidden bg-white rounded-r-lg">
          {selectedAgent ? (
            <>
              <AgentChat key={selectedAgent.id} agent={selectedAgent} />
              <AgentConfigPanel
                agent={selectedAgent}
                onAgentUpdate={handleAgentUpdate}
                onAgentDelete={handleAgentDelete}
              />
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center">
              <div className="text-center">
                <Icon
                  icon={Brain}
                  size={32}
                  className="mx-auto text-text-tertiary opacity-50 mb-3"
                />
                <p className="text-sm text-text-tertiary">Select an agent or deploy a new one</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Template Store modal */}
      {showTemplateStore && workspaceId && (
        <TemplateStore
          workspaceId={workspaceId}
          onClose={() => setShowTemplateStore(false)}
          onCreated={(agent) => {
            setAgents((prev) => [...prev, agent]);
            selectAgent(agent);
            setShowTemplateStore(false);
          }}
        />
      )}
    </div>
  );
}
