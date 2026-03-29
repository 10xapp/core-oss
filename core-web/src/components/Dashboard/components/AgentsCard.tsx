import { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Brain } from 'lucide-react';
import { Icon } from '../../ui/Icon';
import { useWorkspaceStore } from '../../../stores/workspaceStore';
import { getWorkspaceAgents, type AgentInstance } from '../../../api/client';
import BentoCard from './BentoCard';

const MAX_ITEMS = 5;

interface AgentWithWorkspace extends AgentInstance {
  workspaceId: string;
  workspaceName: string;
  workspaceIcon?: string;
}

function getStatusColor(status: string): string {
  switch (status) {
    case 'working':
      return 'bg-green-400';
    case 'error':
      return 'bg-red-400';
    default:
      return 'bg-gray-300';
  }
}

function getSandboxStatusColor(sandboxStatus?: string): string {
  switch (sandboxStatus) {
    case 'running':
      return 'bg-green-400';
    case 'starting':
      return 'bg-yellow-400';
    case 'error':
      return 'bg-red-400';
    default:
      return 'bg-gray-300';
  }
}

function AgentRow({
  agent,
  onClick,
}: {
  agent: AgentWithWorkspace;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className="w-full flex items-center gap-3 px-5 py-2.5 hover:bg-bg-gray/50 transition-colors text-left"
    >
      {/* Agent icon with status indicator */}
      <div className="relative flex-shrink-0">
        <div
          className="w-7 h-7 rounded-lg flex items-center justify-center text-white font-semibold text-xs"
          style={{ background: 'linear-gradient(135deg, #6366F1 0%, #8B5CF6 100%)' }}
        >
          {agent.name.charAt(0).toUpperCase()}
        </div>
        {/* Sandbox status dot */}
        <div
          className={`absolute -bottom-0.5 -right-0.5 w-2.5 h-2.5 rounded-full border-2 border-white ${getSandboxStatusColor(agent.sandbox_status)}`}
        />
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-[13px] font-medium text-text-body truncate">
            {agent.name}
          </span>
          {agent.status === 'working' && (
            <div className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse flex-shrink-0" />
          )}
        </div>
        <span className="text-[11px] text-text-tertiary truncate block">
          {agent.workspaceName}
        </span>
      </div>

      {/* Status indicator */}
      <div
        className={`w-2 h-2 rounded-full flex-shrink-0 ${getStatusColor(agent.status)}`}
        title={agent.status}
      />
    </button>
  );
}

export default function AgentsCard() {
  const navigate = useNavigate();
  const workspaces = useWorkspaceStore((state) => state.workspaces);
  const [agentsByWorkspace, setAgentsByWorkspace] = useState<Record<string, AgentWithWorkspace[]>>({});
  const [loading, setLoading] = useState(true);

  // Get all workspaces (agents can exist in any workspace)
  const workspaceInfos = useMemo(() => {
    return workspaces.map((ws) => ({
      workspaceId: ws.id,
      workspaceName: ws.name,
      workspaceIcon: ws.icon_url,
    }));
  }, [workspaces]);

  // Fetch agents from all workspaces
  useEffect(() => {
    const fetchAllAgents = async () => {
      setLoading(true);
      const results: Record<string, AgentWithWorkspace[]> = {};

      await Promise.all(
        workspaceInfos.map(async (ws) => {
          try {
            const result = await getWorkspaceAgents(ws.workspaceId);
            results[ws.workspaceId] = result.agents.map((agent) => ({
              ...agent,
              workspaceId: ws.workspaceId,
              workspaceName: ws.workspaceName,
              workspaceIcon: ws.workspaceIcon,
            }));
          } catch (err) {
            console.error(`Failed to fetch agents for workspace ${ws.workspaceId}:`, err);
            results[ws.workspaceId] = [];
          }
        })
      );

      setAgentsByWorkspace(results);
      setLoading(false);
    };

    if (workspaceInfos.length > 0) {
      fetchAllAgents();
    } else {
      setLoading(false);
    }
  }, [workspaceInfos]);

  // Aggregate and sort all agents
  const allAgents = useMemo(() => {
    const agents = Object.values(agentsByWorkspace).flat();
    return agents.sort((a, b) => {
      // Sort by status (working first), then by sandbox status (running first), then by name
      if (a.status === 'working' && b.status !== 'working') return -1;
      if (a.status !== 'working' && b.status === 'working') return 1;
      if (a.sandbox_status === 'running' && b.sandbox_status !== 'running') return -1;
      if (a.sandbox_status !== 'running' && b.sandbox_status === 'running') return 1;
      return a.name.localeCompare(b.name);
    });
  }, [agentsByWorkspace]);

  const displayAgents = allAgents.slice(0, MAX_ITEMS);
  const activeCount = allAgents.filter((a) => a.sandbox_status === 'running').length;

  const handleAgentClick = (agent: AgentWithWorkspace) => {
    navigate(`/workspace/${agent.workspaceId}/agents`);
  };

  const handleViewAll = () => {
    // Navigate to first workspace
    if (workspaceInfos.length > 0) {
      navigate(`/workspace/${workspaceInfos[0].workspaceId}/agents`);
    }
  };

  return (
    <BentoCard
      title="Agents"
      icon={
        <Icon
          icon={Brain}
          size={18}
        />
      }
      headerAction={
        allAgents.length > 0 ? (
          <span className="text-[12px] text-text-tertiary">
            {activeCount} active
          </span>
        ) : (
          <button
            onClick={handleViewAll}
            className="text-[12px] font-medium text-text-secondary hover:text-text-body transition-colors"
          >
            View all
          </button>
        )
      }
    >
      {loading ? (
        <div className="flex-1 flex flex-col items-center justify-center py-12 text-text-tertiary">
          <p className="text-[13px]">Loading...</p>
        </div>
      ) : displayAgents.length === 0 ? (
        <div className="flex-1 flex flex-col items-center justify-center py-12 text-text-tertiary">
          <Icon
            icon={Brain}
            size={32}
            className="mb-2 opacity-40"
          />
          <p className="text-[13px]">No agents deployed</p>
        </div>
      ) : (
        <div>
          {displayAgents.map((agent) => (
            <AgentRow
              key={agent.id}
              agent={agent}
              onClick={() => handleAgentClick(agent)}
            />
          ))}
        </div>
      )}
    </BentoCard>
  );
}
