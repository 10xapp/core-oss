import { useCallback, useEffect, useRef, useState } from "react";
import { useBuilderStore } from "../../stores/builderStore";
import { streamBuilderGeneration, type BuilderMessage } from "../../api/client";
import { ArrowUp } from "lucide-react";
import { Icon } from "../ui/Icon";
import ReactMarkdown from "react-markdown";

export default function BuilderChat() {
  const {
    activeProjectId,
    messages,
    isGenerating,
    setIsGenerating,
    setGenerationStatus,
    generationStatus,
    addMessage,
    setFileTree,
    fetchVersions,
    fileTree,
    pendingPrompt,
    setPendingPrompt,
    setBuildError,
  } = useBuilderStore();

  const [input, setInput] = useState("");
  const [streamingContent, setStreamingContent] = useState("");
  const [filesCreated, setFilesCreated] = useState<string[]>([]);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const pendingFilesRef = useRef<Record<string, string>>({});

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto";
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 150)}px`;
    }
  }, [input]);

  // Auto-send pending prompt (from home page or "Fix with AI" button)
  useEffect(() => {
    if (pendingPrompt && activeProjectId && !isGenerating) {
      const prompt = pendingPrompt;
      setPendingPrompt(null);
      void sendMessage(prompt);
    }
  }, [pendingPrompt, activeProjectId, isGenerating]);

  const handleStop = () => {
    abortRef.current?.abort();
  };

  const handleSend = async () => {
    if (!input.trim() || !activeProjectId || isGenerating) return;
    void sendMessage(input.trim());
    setInput("");
  };

  const sendMessage = async (messageText: string) => {
    if (!activeProjectId || isGenerating) return;
    setIsGenerating(true);
    setStreamingContent("");
    setFilesCreated([]);
    setBuildError(null);
    setGenerationStatus("Thinking...");

    // Add user message optimistically
    const userMessage: BuilderMessage = {
      id: `temp-${Date.now()}`,
      conversation_id: "",
      role: "user",
      content: messageText,
      content_parts: [],
      version_id: null,
      created_at: new Date().toISOString(),
    };
    addMessage(userMessage);

    abortRef.current = new AbortController();

    try {
      const response = await streamBuilderGeneration(activeProjectId, messageText);
      const reader = response.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let buffer = "";
      let accumulatedContent = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const event = JSON.parse(line);

            switch (event.type) {
              case "content":
                accumulatedContent += event.delta;
                setStreamingContent(accumulatedContent);
                break;

              case "builder_plan":
                setGenerationStatus("Building...");
                break;

              case "builder_file":
                // Accumulate files in ref — no store update yet (avoids preview remount per file)
                pendingFilesRef.current[event.path] = event.content;
                setFilesCreated((prev) => [...prev, event.path]);
                setGenerationStatus(`Writing ${event.path}`);
                break;

              case "builder_complete": {
                // Flush all files to store at once — single preview remount
                const current = useBuilderStore.getState().fileTree;
                setFileTree({ ...current, ...pendingFilesRef.current });
                pendingFilesRef.current = {};
                setGenerationStatus("Done");
                await fetchVersions(activeProjectId);
                break;
              }

              case "status":
                setGenerationStatus(event.message);
                break;

              case "error":
                console.error("Generation error:", event.message);
                break;

              case "done": {
                const assistantMessage: BuilderMessage = {
                  id: event.message_id || `msg-${Date.now()}`,
                  conversation_id: "",
                  role: "assistant",
                  content: accumulatedContent,
                  content_parts: [],
                  version_id: null,
                  created_at: new Date().toISOString(),
                };
                addMessage(assistantMessage);
                setStreamingContent("");
                setFilesCreated([]);
                break;
              }
            }
          } catch {
            // Ignore malformed JSON lines
          }
        }
      }
    } catch (err) {
      if ((err as Error).name === "AbortError") return;
      console.error("Generation failed:", err);
      const errorMessage: BuilderMessage = {
        id: `error-${Date.now()}`,
        conversation_id: "",
        role: "assistant",
        content: `Error: ${err instanceof Error ? err.message : "Generation failed"}`,
        content_parts: [],
        version_id: null,
        created_at: new Date().toISOString(),
      };
      addMessage(errorMessage);
    } finally {
      setIsGenerating(false);
      setGenerationStatus(null);
      setStreamingContent("");
      setFilesCreated([]);
      pendingFilesRef.current = {};
      abortRef.current = null;
    }
  };

  const hasFiles = Object.keys(fileTree).length > 0;

  return (
    <div className="flex-1 flex flex-col min-h-0">
      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-4 py-4">
        {messages.length === 0 && !isGenerating && (
          <div className="h-full flex items-center justify-center">
            <div className="text-center max-w-sm">
              <h3 className="text-base font-medium text-text-body mb-2">
                What would you like to build?
              </h3>
              <p className="text-sm text-text-secondary leading-relaxed">
                Describe your app and I'll build it for you.
              </p>
            </div>
          </div>
        )}

        {messages.map((msg) => (
          <div key={msg.id} className="mb-4">
            {msg.role === "user" ? (
              <div className="flex justify-end">
                <div className="bg-[#F4F4F4] rounded-[18px] px-4 py-2.5 max-w-[85%] text-sm text-text-body">
                  {msg.content}
                </div>
              </div>
            ) : (
              <div className="max-w-[90%]">
                <div className="text-sm text-text-body prose prose-sm max-w-none [&_pre]:bg-gray-50 [&_pre]:rounded-lg [&_pre]:p-3 [&_pre]:text-xs [&_code]:text-xs [&_code]:font-mono">
                  <ReactMarkdown>{msg.content}</ReactMarkdown>
                </div>
              </div>
            )}
          </div>
        ))}

        {/* Streaming content */}
        {streamingContent && (
          <div className="mb-4">
            <div className="max-w-[90%]">
              <div className="text-sm text-text-body prose prose-sm max-w-none [&_pre]:bg-gray-50 [&_pre]:rounded-lg [&_pre]:p-3 [&_pre]:text-xs [&_code]:text-xs [&_code]:font-mono">
                <ReactMarkdown>{streamingContent}</ReactMarkdown>
              </div>
            </div>
          </div>
        )}

        {/* File creation progress */}
        {isGenerating && filesCreated.length > 0 && (
          <div className="mb-4 ml-1">
            <div className="flex flex-col gap-1">
              {filesCreated.map((file) => (
                <div key={file} className="flex items-center gap-2 text-xs text-text-secondary">
                  <div className="w-1 h-1 rounded-full bg-green-400" />
                  <span className="font-mono">{file}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Thinking indicator */}
        {isGenerating && !streamingContent && (
          <div className="mb-4">
            <div className="flex items-center gap-2 text-sm text-text-tertiary">
              <div className="flex gap-1">
                <div className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-bounce [animation-delay:0ms]" />
                <div className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-bounce [animation-delay:150ms]" />
                <div className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-bounce [animation-delay:300ms]" />
              </div>
              {generationStatus && (
                <span className="text-xs">{generationStatus}</span>
              )}
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="px-3 pb-3">
        <div className="border border-border-gray rounded-2xl bg-bg-white focus-within:border-border-gray-dark transition-colors">
          <textarea
            ref={textareaRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                void handleSend();
              }
            }}
            placeholder={
              hasFiles
                ? "Describe a change..."
                : "Describe your app..."
            }
            rows={1}
            disabled={isGenerating}
            className="w-full resize-none outline-none text-sm text-text-body placeholder:text-text-tertiary px-4 pt-3 pb-1 rounded-2xl disabled:opacity-60"
          />
          <div className="flex justify-end px-3 pb-2">
            {isGenerating ? (
              <button
                onClick={handleStop}
                className="p-1.5 rounded-full bg-gray-900 text-white hover:opacity-90 transition-opacity"
              >
                <svg width="14" height="14" viewBox="0 0 14 14" fill="currentColor"><rect x="3" y="3" width="8" height="8" rx="1.5" /></svg>
              </button>
            ) : (
              <button
                onClick={() => void handleSend()}
                disabled={!input.trim()}
                className="p-1.5 rounded-full bg-gray-900 text-white disabled:opacity-30 hover:opacity-90 transition-opacity"
              >
                <Icon icon={ArrowUp} size={14} />
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
