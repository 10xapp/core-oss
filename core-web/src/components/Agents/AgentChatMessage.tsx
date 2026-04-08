import { useState, memo } from "react";
import ReactMarkdown from "react-markdown";
import type { AgentTask } from "../../api/client";
import TaskStepLog from "./TaskStepLog";

interface AgentChatMessageProps {
  task: AgentTask;
  agentId: string;
  agentName: string;
}

function AgentChatMessage({ task, agentId, agentName }: AgentChatMessageProps) {
  const [showSteps, setShowSteps] = useState(false);

  const instruction = (task.input as { instruction?: string })?.instruction || "";
  const response = (task.output as { response?: string })?.response || "";
  const isRunning = task.status === "running" || task.status === "queued";
  const isFailed = task.status === "failed";

  return (
    <div className="space-y-3">
      {/* User message */}
      <div className="flex justify-end">
        <div className="max-w-[75%] rounded-2xl rounded-br-md bg-bg-input px-4 py-2.5">
          <p className="text-sm text-text-body whitespace-pre-wrap">{instruction}</p>
        </div>
      </div>

      {/* Agent response */}
      <div className="flex justify-start">
        <div className="max-w-[85%] space-y-1">
          {/* Agent name */}
          <p className="text-[11px] font-medium text-text-tertiary ml-1">{agentName}</p>

          <div className="rounded-2xl rounded-bl-md bg-white border border-border-light px-4 py-2.5">
            {isRunning && !response ? (
              <div className="flex items-center gap-2">
                <div className="flex gap-1">
                  <div className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: "0ms" }} />
                  <div className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: "150ms" }} />
                  <div className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: "300ms" }} />
                </div>
                <span className="text-xs text-text-tertiary">Working...</span>
              </div>
            ) : isFailed ? (
              <p className="text-sm text-red-600">{task.error || "Task failed"}</p>
            ) : response ? (
              <div className="prose prose-sm max-w-none text-text-body prose-p:my-1 prose-headings:my-2 prose-pre:my-2 prose-ul:my-1 prose-ol:my-1">
                <ReactMarkdown>{response}</ReactMarkdown>
              </div>
            ) : null}

            {/* Step toggle */}
            {(task.status === "completed" || task.status === "running") && (
              <button
                onClick={() => setShowSteps(!showSteps)}
                className="mt-2 flex items-center gap-1.5 text-[11px] text-text-tertiary hover:text-text-secondary transition-colors"
              >
                <span className={`transition-transform ${showSteps ? "rotate-90" : ""}`}>&#9654;</span>
                {task.token_usage > 0 && <span>{task.token_usage.toLocaleString()} tokens</span>}
                {task.token_usage > 0 && <span className="text-text-tertiary/40">·</span>}
                <span>{showSteps ? "Hide steps" : "View steps"}</span>
              </button>
            )}
          </div>

          {/* Inline step log */}
          {showSteps && (
            <div className="ml-2 mt-1 rounded-lg border border-border-light bg-gray-50/50 p-2">
              <TaskStepLog
                agentId={agentId}
                taskId={task.id}
                isRunning={task.status === "running"}
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default memo(AgentChatMessage);
