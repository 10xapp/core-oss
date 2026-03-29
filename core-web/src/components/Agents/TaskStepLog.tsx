import { useState, useEffect, useRef } from "react";
import { ChevronDown } from "lucide-react";
import { Icon } from "../ui/Icon";
import { getTaskSteps, type AgentTaskStep } from "../../api/client";
import { useAgentStepsRealtime } from "../../hooks/useAgentRealtime";

interface TaskStepLogProps {
  agentId: string;
  taskId: string;
  isRunning: boolean;
}

const STEP_TYPE_STYLES: Record<string, { label: string; color: string; bg: string }> = {
  thinking: { label: "Thinking", color: "text-gray-600", bg: "bg-gray-50" },
  tool_call: { label: "Tool Call", color: "text-blue-600", bg: "bg-blue-50" },
  tool_result: { label: "Result", color: "text-blue-500", bg: "bg-blue-50/50" },
  message: { label: "Message", color: "text-green-600", bg: "bg-green-50" },
  error: { label: "Error", color: "text-red-600", bg: "bg-red-50" },
  log: { label: "Log", color: "text-purple-600", bg: "bg-purple-50" },
};

export default function TaskStepLog({ agentId, taskId, isRunning }: TaskStepLogProps) {
  const [initialSteps, setInitialSteps] = useState<AgentTaskStep[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedSteps, setExpandedSteps] = useState<Set<string>>(new Set());
  const bottomRef = useRef<HTMLDivElement>(null);

  // Load initial steps once
  useEffect(() => {
    getTaskSteps(agentId, taskId)
      .then((result) => setInitialSteps(result.steps))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [agentId, taskId]);

  // Subscribe to Realtime for new steps (replaces polling)
  const steps = useAgentStepsRealtime(taskId, initialSteps);

  // Auto-scroll to bottom when new steps arrive
  useEffect(() => {
    if (isRunning) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [steps.length, isRunning]);

  const toggleStep = (stepId: string) => {
    setExpandedSteps((prev) => {
      const next = new Set(prev);
      if (next.has(stepId)) {
        next.delete(stepId);
      } else {
        next.add(stepId);
      }
      return next;
    });
  };

  if (loading) {
    return <p className="text-xs text-text-tertiary py-2">Loading steps...</p>;
  }

  if (steps.length === 0) {
    return <p className="text-xs text-text-tertiary py-2">No steps recorded yet.</p>;
  }

  return (
    <div className="space-y-1">
      {steps.map((step) => {
        const style = STEP_TYPE_STYLES[step.step_type] || STEP_TYPE_STYLES.thinking;
        const isExpanded = expandedSteps.has(step.id);
        const hasDetail =
          step.step_type === "tool_call" ||
          step.step_type === "tool_result" ||
          (step.content && step.content.length > 120);

        return (
          <div key={step.id} className={`rounded-md ${style.bg} border border-transparent`}>
            <button
              onClick={() => hasDetail && toggleStep(step.id)}
              className={`w-full flex items-start gap-2 px-3 py-2 text-left ${
                hasDetail ? "cursor-pointer hover:border-gray-200" : "cursor-default"
              }`}
            >
              {/* Turn indicator */}
              <span className="text-[10px] text-text-tertiary font-mono mt-0.5 shrink-0 w-4 text-right">
                {step.turn}
              </span>

              {/* Step type badge */}
              <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium shrink-0 ${style.color} ${style.bg}`}>
                {step.step_type === "tool_call" && step.tool_name
                  ? step.tool_name
                  : style.label}
              </span>

              {/* Content preview */}
              <span className="flex-1 text-xs text-text-body truncate min-w-0">
                {step.step_type === "tool_call" && step.tool_args
                  ? truncateJson(step.tool_args)
                  : step.step_type === "tool_result" && step.tool_result
                    ? truncateJson(step.tool_result)
                    : step.content
                      ? step.content.slice(0, 120)
                      : ""}
              </span>

              {/* Duration */}
              {step.duration_ms != null && (
                <span className="text-[10px] text-text-tertiary shrink-0">
                  {step.duration_ms}ms
                </span>
              )}

              {/* Expand indicator */}
              {hasDetail && (
                <Icon
                  icon={ChevronDown}
                  size={12}
                  className={`text-text-tertiary shrink-0 transition-transform ${isExpanded ? "rotate-180" : ""}`}
                />
              )}
            </button>

            {/* Expanded detail */}
            {isExpanded && (
              <div className="px-3 pb-2 ml-6">
                {step.step_type === "tool_call" && step.tool_args && (
                  <pre className="text-[11px] text-text-secondary bg-white rounded p-2 overflow-x-auto max-h-40 border border-border-light">
                    {JSON.stringify(step.tool_args, null, 2)}
                  </pre>
                )}
                {step.step_type === "tool_result" && step.tool_result && (
                  <pre className="text-[11px] text-text-secondary bg-white rounded p-2 overflow-x-auto max-h-40 border border-border-light">
                    {typeof step.tool_result === "string"
                      ? step.tool_result
                      : JSON.stringify(step.tool_result, null, 2)}
                  </pre>
                )}
                {step.content && step.content.length > 120 && (
                  <p className="text-xs text-text-secondary whitespace-pre-wrap">{step.content}</p>
                )}
              </div>
            )}
          </div>
        );
      })}

      {isRunning && (
        <div className="flex items-center gap-2 px-3 py-2">
          <div className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse" />
          <span className="text-xs text-text-tertiary">Agent is working...</span>
        </div>
      )}

      <div ref={bottomRef} />
    </div>
  );
}

function truncateJson(obj: unknown): string {
  const str = typeof obj === "string" ? obj : JSON.stringify(obj);
  return str.length > 120 ? str.slice(0, 120) + "..." : str;
}
