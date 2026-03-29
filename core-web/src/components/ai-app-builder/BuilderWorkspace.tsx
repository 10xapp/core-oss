import { useNavigate } from "react-router-dom";
import { useBuilderStore } from "../../stores/builderStore";
import { ArrowLeft, Download } from "lucide-react";
import { Icon } from "../ui/Icon";
import BuilderChat from "./BuilderChat";
import BuilderPreview from "./BuilderPreview";
import VersionSelector from "./VersionSelector";
import { downloadProjectAsZip } from "./downloadProject";

export default function BuilderWorkspace() {
  const navigate = useNavigate();
  const {
    activeProject,
    fileTree,
    isGenerating,
    generationStatus,
  } = useBuilderStore();

  const hasFiles = Object.keys(fileTree).length > 0;

  return (
    <div className="h-screen w-screen bg-bg-white flex flex-col">
      {/* Top bar */}
      <div className="h-12 border-b border-border-light flex items-center px-3 gap-2 shrink-0">
        {/* Left: Back + Project name */}
        <button
          onClick={() => navigate("/builder")}
          className="p-1.5 rounded-lg hover:bg-gray-50 text-text-secondary"
        >
          <Icon icon={ArrowLeft} size={16} />
        </button>
        <span className="text-sm font-medium text-text-body max-w-[200px] truncate">
          {activeProject?.name || "Untitled"}
        </span>

        {/* Center: Version selector */}
        <div className="flex-1 flex justify-center">
          <VersionSelector />
        </div>

        {/* Download button */}
        <button
          onClick={() => void downloadProjectAsZip(fileTree, activeProject?.name || "project")}
          disabled={!hasFiles || isGenerating}
          className="p-1.5 rounded-lg hover:bg-gray-50 text-text-secondary disabled:opacity-30 disabled:pointer-events-none transition-colors"
          title="Download as ZIP"
        >
          <Icon icon={Download} size={16} />
        </button>

        {/* Generation indicator */}
        {isGenerating && (
          <div className="flex items-center gap-1.5 text-xs text-text-secondary">
            <div className="w-1.5 h-1.5 rounded-full bg-green-400 animate-pulse" />
            <span className="truncate max-w-[160px]">
              {generationStatus || "Generating..."}
            </span>
          </div>
        )}
      </div>

      {/* Main content: Chat + Code */}
      <div className="flex-1 flex min-h-0">
        {/* Chat panel (left) */}
        <div className="w-[420px] shrink-0 border-r border-border-light flex flex-col">
          <BuilderChat />
        </div>

        {/* Code panel (right) */}
        <div className="flex-1 flex flex-col min-w-0 bg-gray-50">
          <BuilderPreview />
        </div>
      </div>
    </div>
  );
}
