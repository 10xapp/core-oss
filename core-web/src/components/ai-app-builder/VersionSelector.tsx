import { useState, useRef, useEffect } from "react";
import { useBuilderStore } from "../../stores/builderStore";
import { ChevronDown } from "lucide-react";
import { Icon } from "../ui/Icon";

export default function VersionSelector() {
  const { versions, activeVersion, setActiveVersion } = useBuilderStore();
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  if (versions.length === 0) return null;

  return (
    <div className="relative" ref={dropdownRef}>
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-1 px-2.5 py-1 rounded-lg hover:bg-gray-50 text-xs text-text-secondary font-medium transition-colors"
      >
        v{activeVersion?.version_number || 1}
        <Icon icon={ChevronDown} size={12} className="text-text-tertiary" />
      </button>

      {isOpen && (
        <div className="absolute top-full left-1/2 -translate-x-1/2 mt-1 w-72 bg-white rounded-xl border border-border-light shadow-lg z-50 py-1 max-h-64 overflow-y-auto">
          {versions.map((version) => (
            <button
              key={version.id}
              onClick={() => {
                setActiveVersion(version);
                setIsOpen(false);
              }}
              className={`w-full text-left px-3 py-2 text-xs hover:bg-gray-50 flex items-center gap-2 transition-colors ${
                activeVersion?.id === version.id ? "bg-gray-50" : ""
              }`}
            >
              <span className="font-medium text-text-body shrink-0 font-mono">
                v{version.version_number}
              </span>
              <span className="text-text-secondary truncate flex-1">
                {version.prompt || "Initial version"}
              </span>
              {activeVersion?.id === version.id && (
                <span className="text-text-success shrink-0 text-[10px]">current</span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
