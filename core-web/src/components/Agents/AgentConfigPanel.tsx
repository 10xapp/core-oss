import { useState, useEffect, useRef } from "react";
import {
  updateAgent,
  pauseAgent,
  resumeAgent,
  deleteAgent,
  type AgentInstance,
} from "../../api/client";
import { supabase } from "../../lib/supabase";
import AgentStorageBrowser from "./AgentStorageBrowser";
import SandboxFileBrowser from "./SandboxFileBrowser";

interface AgentIdentity {
  name?: string;
  role?: string;
  backstory?: string;
  objective?: string;
  personality?: string;
}

interface AgentConfigPanelProps {
  agent: AgentInstance;
  onAgentUpdate: (agent: AgentInstance) => void;
  onAgentDelete: () => void;
}

export default function AgentConfigPanel({ agent, onAgentUpdate, onAgentDelete }: AgentConfigPanelProps) {
  const [systemPrompt, setSystemPrompt] = useState(agent.system_prompt);
  const [isSaving, setIsSaving] = useState(false);
  const [lifecycleLoading, setLifecycleLoading] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const saveTimeoutRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Identity state
  const [identity, setIdentity] = useState<AgentIdentity | null>(null);
  const [identitySaving, setIdentitySaving] = useState(false);
  const identitySaveRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Memories state
  const [memories, setMemories] = useState<string | null>(null);

  // Sync when agent changes
  useEffect(() => {
    setSystemPrompt(agent.system_prompt);
  }, [agent.id, agent.system_prompt]);

  const hasIdentitySupport = !!(agent.config as Record<string, unknown>)?.supports_identity
    || !!(agent.config as Record<string, unknown>)?.role;

  // Load identity.json from Storage (only for identity-enabled agents)
  useEffect(() => {
    if (!hasIdentitySupport) { setIdentity(null); return; }
    let cancelled = false;
    const loadIdentity = async () => {
      try {
        const { data, error } = await supabase.storage
          .from("agent-data")
          .download(`${agent.id}/personal/identity.json`);
        if (cancelled) return;
        if (error || !data) {
          setIdentity(null);
          return;
        }
        const text = await data.text();
        setIdentity(JSON.parse(text));
      } catch {
        if (!cancelled) setIdentity(null);
      }
    };
    loadIdentity();
    return () => { cancelled = true; };
  }, [agent.id, hasIdentitySupport]);

  // Load memories.md from Storage (only for identity-enabled agents)
  useEffect(() => {
    if (!hasIdentitySupport) { setMemories(null); return; }
    let cancelled = false;
    const loadMemories = async () => {
      try {
        const { data, error } = await supabase.storage
          .from("agent-data")
          .download(`${agent.id}/personal/memories.md`);
        if (cancelled) return;
        if (error || !data) {
          setMemories(null);
          return;
        }
        setMemories(await data.text());
      } catch {
        if (!cancelled) setMemories(null);
      }
    };
    loadMemories();
    return () => { cancelled = true; };
  }, [agent.id, hasIdentitySupport]);

  // Save identity to Storage (debounced)
  const saveIdentity = (updated: AgentIdentity) => {
    setIdentity(updated);
    if (identitySaveRef.current) clearTimeout(identitySaveRef.current);
    identitySaveRef.current = setTimeout(async () => {
      setIdentitySaving(true);
      try {
        const content = JSON.stringify(updated, null, 2);
        const blob = new Blob([content], { type: "application/json" });
        const storagePath = `${agent.id}/personal/identity.json`;
        const { error } = await supabase.storage
          .from("agent-data")
          .update(storagePath, blob, { contentType: "application/json" });
        if (error) {
          await supabase.storage
            .from("agent-data")
            .upload(storagePath, blob, { contentType: "application/json" });
        }
      } catch (err) {
        console.error("Failed to save identity:", err);
      } finally {
        setIdentitySaving(false);
      }
    }, 1000);
  };

  // Auto-save system prompt with debounce
  const handlePromptChange = (value: string) => {
    setSystemPrompt(value);
    if (saveTimeoutRef.current) clearTimeout(saveTimeoutRef.current);
    saveTimeoutRef.current = setTimeout(async () => {
      setIsSaving(true);
      try {
        const updated = await updateAgent(agent.id, { system_prompt: value });
        onAgentUpdate(updated);
      } catch (err) {
        console.error("Failed to save system prompt:", err);
      } finally {
        setIsSaving(false);
      }
    }, 1000);
  };

  const handlePause = async () => {
    setLifecycleLoading(true);
    try {
      const updated = await pauseAgent(agent.id);
      onAgentUpdate(updated);
    } catch (err) {
      console.error("Failed to pause:", err);
    } finally {
      setLifecycleLoading(false);
    }
  };

  const handleResume = async () => {
    setLifecycleLoading(true);
    try {
      const updated = await resumeAgent(agent.id);
      onAgentUpdate(updated);
    } catch (err) {
      console.error("Failed to resume:", err);
    } finally {
      setLifecycleLoading(false);
    }
  };

  const handleDelete = async () => {
    setLifecycleLoading(true);
    try {
      await deleteAgent(agent.id);
      onAgentDelete();
    } catch (err) {
      console.error("Failed to delete:", err);
    } finally {
      setLifecycleLoading(false);
      setShowDeleteConfirm(false);
    }
  };

  const statusColor = (status: string) => {
    switch (status) {
      case "working": return "bg-green-400";
      case "error": return "bg-red-400";
      default: return "bg-gray-300";
    }
  };

  const sandboxColor = (status?: string) => {
    switch (status) {
      case "running": return "bg-green-400";
      case "starting": return "bg-yellow-400 animate-pulse";
      case "error": return "bg-red-400";
      default: return "bg-gray-300";
    }
  };

  const model = (agent.config as { model?: string })?.model || "claude-opus-4-6";

  return (
    <div className="w-[280px] shrink-0 flex flex-col border-l border-border-light bg-bg-panel overflow-y-auto">
      <div className="p-4 space-y-5">
        {/* Agent header */}
        <div>
          <div className="flex items-center gap-2 mb-2">
            {agent.avatar_url ? (
              <img
                src={agent.avatar_url}
                className="w-8 h-8 rounded-lg object-cover shrink-0"
              />
            ) : (
              <div
                className="w-8 h-8 rounded-lg flex items-center justify-center text-white font-semibold text-sm shrink-0"
                style={{ background: "linear-gradient(135deg, #6366F1 0%, #8B5CF6 100%)" }}
              >
                {agent.name.charAt(0).toUpperCase()}
              </div>
            )}
            <div className="flex-1 min-w-0">
              <h3 className="text-sm font-semibold text-text-body truncate">{agent.name}</h3>
              <div className="flex items-center gap-1.5 text-[11px] text-text-tertiary">
                <span className={`inline-block w-1.5 h-1.5 rounded-full ${statusColor(agent.status)}`} />
                <span className="capitalize">{agent.status}</span>
              </div>
            </div>
          </div>
        </div>

        {/* Identity */}
        {identity && (
          <div>
            <div className="flex items-center justify-between mb-1">
              <p className="text-[11px] uppercase tracking-wide text-text-tertiary">Identity</p>
              {identitySaving && <span className="text-[10px] text-text-tertiary">Saving...</span>}
            </div>
            <div className="space-y-2.5">
              {identity.role && (
                <div>
                  <p className="text-[10px] text-text-tertiary mb-0.5">Role</p>
                  <p className="text-xs text-text-body">{identity.role}</p>
                </div>
              )}
              <div>
                <p className="text-[10px] text-text-tertiary mb-0.5">Backstory</p>
                <textarea
                  value={identity.backstory || ""}
                  onChange={(e) => saveIdentity({ ...identity, backstory: e.target.value })}
                  className="w-full text-xs text-text-body bg-white border border-border-light rounded-lg p-2 resize-none h-20 focus:outline-none focus:ring-1 focus:ring-brand-primary"
                  placeholder="Agent backstory..."
                />
              </div>
              <div>
                <p className="text-[10px] text-text-tertiary mb-0.5">Objective</p>
                <textarea
                  value={identity.objective || ""}
                  onChange={(e) => saveIdentity({ ...identity, objective: e.target.value })}
                  className="w-full text-xs text-text-body bg-white border border-border-light rounded-lg p-2 resize-none h-16 focus:outline-none focus:ring-1 focus:ring-brand-primary"
                  placeholder="Agent objective..."
                />
              </div>
              <div>
                <p className="text-[10px] text-text-tertiary mb-0.5">Personality</p>
                <input
                  type="text"
                  value={identity.personality || ""}
                  onChange={(e) => saveIdentity({ ...identity, personality: e.target.value })}
                  className="w-full text-xs text-text-body bg-white border border-border-light rounded-lg p-2 focus:outline-none focus:ring-1 focus:ring-brand-primary"
                  placeholder="warm, professional, concise..."
                />
              </div>
            </div>
          </div>
        )}

        {/* Memories */}
        {memories !== null && (
          <div>
            <p className="text-[11px] uppercase tracking-wide text-text-tertiary mb-1">Memories</p>
            <div className="border border-border-light rounded-lg bg-white p-2.5 max-h-[200px] overflow-y-auto">
              <pre className="text-[11px] text-text-body whitespace-pre-wrap break-words font-sans leading-relaxed">
                {memories || "No memories yet."}
              </pre>
            </div>
          </div>
        )}

        {/* Sandbox status */}
        <div>
          <p className="text-[11px] uppercase tracking-wide text-text-tertiary mb-2">Sandbox</p>
          <div className="flex items-center gap-2 mb-2">
            <span className={`w-2 h-2 rounded-full ${sandboxColor(agent.sandbox_status)}`} />
            <span className="text-xs text-text-secondary capitalize">{agent.sandbox_status || "off"}</span>
          </div>
          <div className="flex gap-1.5">
            {(agent.sandbox_status === "running" || agent.sandbox_status === "idle") && (
              <button
                onClick={handlePause}
                disabled={lifecycleLoading}
                className="text-[11px] px-2.5 py-1 rounded-md border border-border-gray text-text-secondary hover:bg-white disabled:opacity-50 transition-colors"
              >
                Pause
              </button>
            )}
            {(!agent.sandbox_status || agent.sandbox_status === "paused" || agent.sandbox_status === "off") && (
              <button
                onClick={handleResume}
                disabled={lifecycleLoading}
                className="text-[11px] px-2.5 py-1 rounded-md border border-border-gray text-text-secondary hover:bg-white disabled:opacity-50 transition-colors"
              >
                Start
              </button>
            )}
            {!showDeleteConfirm ? (
              <button
                onClick={() => setShowDeleteConfirm(true)}
                disabled={lifecycleLoading}
                className="text-[11px] px-2.5 py-1 rounded-md border border-red-200 text-red-500 hover:bg-red-50 disabled:opacity-50 transition-colors"
              >
                Delete
              </button>
            ) : (
              <div className="flex gap-1">
                <button
                  onClick={handleDelete}
                  disabled={lifecycleLoading}
                  className="text-[11px] px-2.5 py-1 rounded-md bg-red-600 text-white hover:bg-red-700 disabled:opacity-50 transition-colors"
                >
                  {lifecycleLoading ? "..." : "Confirm"}
                </button>
                <button
                  onClick={() => setShowDeleteConfirm(false)}
                  className="text-[11px] px-2.5 py-1 rounded-md border border-border-gray text-text-secondary hover:bg-white transition-colors"
                >
                  Cancel
                </button>
              </div>
            )}
          </div>
        </div>

        {/* Model */}
        <div>
          <p className="text-[11px] uppercase tracking-wide text-text-tertiary mb-1">Model</p>
          <p className="text-xs text-text-body font-mono">{model}</p>
        </div>

        {/* Tools */}
        <div>
          <p className="text-[11px] uppercase tracking-wide text-text-tertiary mb-1">
            Tools ({agent.enabled_tools.length})
          </p>
          <div className="flex flex-wrap gap-1">
            {agent.enabled_tools.map((tool) => (
              <span
                key={tool}
                className="text-[11px] px-2 py-0.5 rounded-full bg-white border border-border-light text-text-secondary"
              >
                {tool}
              </span>
            ))}
          </div>
        </div>

        {/* Agent's Personal Data */}
        <AgentStorageBrowser
          agentId={agent.id}
          label="Personal Data"
          rootPath="personal"
          emptyMessage="No personal files yet. The agent will create files as it works."
        />

        {/* Workspace Data */}
        <AgentStorageBrowser
          agentId={agent.id}
          label="Workspace Data"
          rootPath="workspace"
          emptyMessage="No workspace data synced yet."
        />

        {/* Sandbox Files */}
        <SandboxFileBrowser agentId={agent.id} sandboxStatus={agent.sandbox_status} />

        {/* System prompt */}
        <div>
          <div className="flex items-center justify-between mb-1">
            <p className="text-[11px] uppercase tracking-wide text-text-tertiary">System Prompt</p>
            {isSaving && <span className="text-[10px] text-text-tertiary">Saving...</span>}
          </div>
          <textarea
            value={systemPrompt}
            onChange={(e) => handlePromptChange(e.target.value)}
            className="w-full h-48 text-xs text-text-body bg-white border border-border-light rounded-lg p-2.5 resize-none focus:outline-none focus:ring-1 focus:ring-brand-primary"
            placeholder="System prompt for this agent..."
          />
        </div>
      </div>
    </div>
  );
}
