import { useState, useEffect, useRef } from "react";
import { useBuilderStore } from "../../stores/builderStore";
import { Snack } from "snack-sdk";
import PhoneFrame from "./PhoneFrame";

type Tab = "preview" | "code";

/** Convert our fileTree to Snack file format. */
function toSnackFiles(fileTree: Record<string, string>) {
  const files: Record<string, { type: "CODE"; contents: string }> = {};
  for (const [path, content] of Object.entries(fileTree)) {
    files[path] = { type: "CODE", contents: content };
  }
  return files;
}

export default function BuilderPreview() {
  const { fileTree, viewMode, setViewMode, isGenerating, buildError, setBuildError, setPendingPrompt } = useBuilderStore();
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);

  // Refs for Snack SDK
  const snackRef = useRef<Snack | null>(null);
  const iframeWindowRef = useRef<Window | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);

  // Debounce file tree updates during generation
  const [debouncedFileTree, setDebouncedFileTree] = useState(fileTree);
  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (isGenerating) {
      if (debounceTimer.current) clearTimeout(debounceTimer.current);
      debounceTimer.current = setTimeout(() => {
        setDebouncedFileTree(fileTree);
      }, 800);
    } else {
      if (debounceTimer.current) clearTimeout(debounceTimer.current);
      setDebouncedFileTree(fileTree);
    }
    return () => {
      if (debounceTimer.current) clearTimeout(debounceTimer.current);
    };
  }, [fileTree, isGenerating]);

  const files = Object.entries(fileTree);
  const hasFiles = files.length > 0;
  const hasAppEntry = debouncedFileTree["App.js"] != null || debouncedFileTree["App.tsx"] != null;

  const activeTab: Tab = viewMode === "preview" ? "preview" : "code";
  const setActiveTab = (tab: Tab) => setViewMode(tab === "preview" ? "preview" : "code");

  const activeFile = selectedFile && fileTree[selectedFile] ? selectedFile : files[0]?.[0] ?? null;
  const activeContent = activeFile ? fileTree[activeFile] : null;

  // Initialize Snack when iframe mounts and we have files
  const handleIframeLoad = (iframe: HTMLIFrameElement | null) => {
    if (!iframe || !hasAppEntry) return;

    // Already initialized with this iframe
    if (iframeWindowRef.current === iframe.contentWindow && snackRef.current) return;

    // Cleanup previous
    cleanupRef.current?.();

    iframeWindowRef.current = iframe.contentWindow;

    const snack = new Snack({
      files: toSnackFiles(debouncedFileTree),
      codeChangesDelay: 500,
      webPreviewRef: { current: iframe.contentWindow },
    });

    // Capture errors
    const logUnsub = snack.addLogListener((log) => {
      if (log.type === "error") {
        setBuildError(log.message);
      }
    });

    // Watch for preview URL
    const stateUnsub = snack.addStateListener((state) => {
      if (state.webPreviewURL && state.webPreviewURL !== previewUrl) {
        setPreviewUrl(state.webPreviewURL);
      }
    });

    snack.setOnline(true);
    snackRef.current = snack;

    cleanupRef.current = () => {
      logUnsub();
      stateUnsub();
      snackRef.current = null;
      iframeWindowRef.current = null;
    };
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      cleanupRef.current?.();
    };
  }, []);

  // Update files when debounced tree changes
  useEffect(() => {
    if (snackRef.current && hasAppEntry) {
      snackRef.current.updateFiles(toSnackFiles(debouncedFileTree));
    }
  }, [debouncedFileTree, hasAppEntry]);

  if (!hasFiles) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <div className="w-16 h-16 mx-auto mb-3 rounded-2xl bg-bg-white border border-border-light flex items-center justify-center">
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none" className="text-gray-200">
              <path d="M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v6m12 0v10a2 2 0 01-2 2H5a2 2 0 01-2-2V9m18 0H9m0 0H3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </div>
          <p className="text-sm text-text-tertiary">
            App preview will appear here
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col min-h-0">
      {/* Tab bar */}
      <div className="h-10 border-b border-border-light flex items-center px-3 gap-1 shrink-0 bg-bg-white">
        <button
          onClick={() => setActiveTab("preview")}
          className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
            activeTab === "preview"
              ? "bg-gray-100 text-text-body"
              : "text-text-tertiary hover:text-text-secondary"
          }`}
        >
          Preview
        </button>
        <button
          onClick={() => setActiveTab("code")}
          className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
            activeTab === "code"
              ? "bg-gray-100 text-text-body"
              : "text-text-tertiary hover:text-text-secondary"
          }`}
        >
          Code
        </button>
      </div>

      {/* Content */}
      {activeTab === "preview" ? (
        <div className="flex-1 min-h-0 bg-bg-input flex flex-col">
          <div className="flex-1 min-h-0">
            {hasAppEntry ? (
              <PhoneFrame>
                <iframe
                  ref={handleIframeLoad}
                  src={previewUrl || "about:blank"}
                  title="App Preview"
                  allow="geolocation; camera; microphone"
                  style={{
                    width: "100%",
                    height: "100%",
                    border: "none",
                    backgroundColor: "#fff",
                  }}
                />
              </PhoneFrame>
            ) : (
              <div className="flex items-center justify-center h-full">
                <div className="text-center">
                  <p className="text-sm text-text-tertiary">
                    Waiting for App.js...
                  </p>
                  {isGenerating && (
                    <div className="flex gap-1 justify-center mt-2">
                      <div className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-bounce [animation-delay:0ms]" />
                      <div className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-bounce [animation-delay:150ms]" />
                      <div className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-bounce [animation-delay:300ms]" />
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Build error banner */}
          {buildError && !isGenerating && (
            <div className="shrink-0 mx-4 mb-3 p-3 bg-red-50 border border-red-200 rounded-xl">
              <div className="flex items-start gap-2">
                <span className="text-red-500 text-xs mt-0.5 shrink-0">Error</span>
                <pre className="text-xs text-red-700 font-mono flex-1 whitespace-pre-wrap break-words line-clamp-4">
                  {buildError}
                </pre>
              </div>
              <button
                onClick={() => {
                  setPendingPrompt(`Fix this build error:\n\n${buildError}`);
                  setBuildError(null);
                }}
                className="mt-2 px-3 py-1.5 text-xs font-medium bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
              >
                Fix with AI
              </button>
            </div>
          )}
        </div>
      ) : (
        /* Code: File tree + editor */
        <div className="flex-1 flex min-h-0">
          {/* File tree sidebar */}
          <div className="w-48 shrink-0 border-r border-border-light bg-bg-white overflow-y-auto py-2">
            {files.map(([path]) => {
              const fileName = path.split("/").pop() || path;
              const dir = path.includes("/") ? path.substring(0, path.lastIndexOf("/")) : null;
              return (
                <button
                  key={path}
                  onClick={() => setSelectedFile(path)}
                  className={`w-full text-left px-3 py-1.5 text-xs transition-colors ${
                    activeFile === path
                      ? "bg-gray-50 text-text-body"
                      : "text-text-secondary hover:bg-gray-50"
                  }`}
                >
                  {dir && (
                    <span className="text-text-tertiary">{dir}/</span>
                  )}
                  <span className="font-medium">{fileName}</span>
                </button>
              );
            })}
          </div>

          {/* Code editor */}
          <div className="flex-1 flex flex-col min-w-0 min-h-0">
            {activeFile && (
              <div className="h-9 bg-gray-50 border-b border-border-light flex items-center px-3 shrink-0">
                <span className="text-xs text-text-secondary font-mono truncate">{activeFile}</span>
              </div>
            )}
            <div className="flex-1 overflow-auto min-h-0 bg-white">
              {activeContent != null ? (
                <pre className="text-[13px] leading-relaxed font-mono text-text-body p-4 whitespace-pre-wrap break-words">
                  <code>{activeContent}</code>
                </pre>
              ) : (
                <div className="flex items-center justify-center h-full text-sm text-text-tertiary">
                  Select a file to view
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
