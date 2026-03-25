import { useState } from "react";
import { HugeiconsIcon } from "@hugeicons/react";
import {
  Download04Icon,
  Clock01Icon,
  SquareArrowExpand01Icon,
  SquareArrowShrink02Icon,
  Share01Icon,
  Delete02Icon,
  PencilEdit01Icon,
  Copy01Icon,
  FolderExportIcon,
} from "@hugeicons-pro/core-stroke-standard";
import Dropdown from "../Dropdown/Dropdown";
import FilesSharingModal from "./FilesSharingModal";

interface FilesSettingsDropdownProps {
  isOpen: boolean;
  onClose: () => void;
  trigger: React.RefObject<HTMLElement | null>;
  fileId?: string;
  onDelete?: () => void;
  isFullWidth?: boolean;
  onToggleFullWidth?: () => void;
  onDownload?: () => void;
  onOpenVersionHistory?: () => void;
  onRename?: () => void;
  onDuplicate?: () => void;
  onMove?: () => void;
}

export default function FilesSettingsDropdown({
  isOpen,
  onClose,
  trigger,
  fileId,
  onDelete,
  isFullWidth,
  onToggleFullWidth,
  onDownload,
  onOpenVersionHistory,
  onRename,
  onDuplicate,
  onMove,
}: FilesSettingsDropdownProps) {
  const [showSharingModal, setShowSharingModal] = useState(false);

  const itemClass =
    "w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm text-text-body hover:bg-bg-gray transition-colors text-left";

  return (
    <>
      <Dropdown isOpen={isOpen} onClose={onClose} trigger={trigger} align="right">
        <div className="w-[220px] px-1.5 py-1">
          {/* Rename */}
          {onRename && (
            <button
              onClick={() => {
                onRename();
                onClose();
              }}
              className={itemClass}
            >
              <HugeiconsIcon icon={PencilEdit01Icon} size={16} className="text-text-secondary" />
              <span>Rename</span>
            </button>
          )}

          {/* Duplicate */}
          {onDuplicate && (
            <button
              onClick={() => {
                onDuplicate();
                onClose();
              }}
              className={itemClass}
            >
              <HugeiconsIcon icon={Copy01Icon} size={16} className="text-text-secondary" />
              <span>Duplicate</span>
            </button>
          )}

          {/* Move to */}
          {onMove && (
            <button
              onClick={() => {
                onMove();
                onClose();
              }}
              className={itemClass}
            >
              <HugeiconsIcon icon={FolderExportIcon} size={16} className="text-text-secondary" />
              <span>Move to</span>
            </button>
          )}

          {/* Divider between actions and view options */}
          {(onRename || onDuplicate || onMove) && (onToggleFullWidth || onDownload || onOpenVersionHistory) && (
            <div className="my-1 border-t border-border-gray mx-1" />
          )}

          {/* Full width toggle */}
          {onToggleFullWidth && (
            <button
              onClick={onToggleFullWidth}
              className={itemClass + " justify-between"}
            >
              <div className="flex items-center gap-3">
                <HugeiconsIcon
                  icon={isFullWidth ? SquareArrowShrink02Icon : SquareArrowExpand01Icon}
                  size={16}
                  className="text-text-secondary"
                />
                <span>Full width</span>
              </div>
              <div
                className={`relative w-8 h-[18px] rounded-full transition-colors shrink-0 ${
                  isFullWidth ? "bg-black" : "bg-gray-200"
                }`}
              >
                <span
                  className={`absolute top-[3px] w-3 h-3 bg-white rounded-full shadow transition-transform ${
                    isFullWidth ? "left-[17px]" : "left-[3px]"
                  }`}
                />
              </div>
            </button>
          )}

          {/* Download */}
          {onDownload && (
            <button
              onClick={() => {
                onDownload();
                onClose();
              }}
              className={itemClass}
            >
              <HugeiconsIcon icon={Download04Icon} size={16} className="text-text-secondary" />
              <span>Download as Markdown</span>
            </button>
          )}

          {/* Version history */}
          {onOpenVersionHistory && (
            <button
              onClick={() => {
                onOpenVersionHistory();
                onClose();
              }}
              className={itemClass}
            >
              <HugeiconsIcon icon={Clock01Icon} size={16} className="text-text-secondary" />
              <span>Version history</span>
            </button>
          )}

          {/* Divider + Sharing */}
          {fileId && (
            <>
              <div className="my-1 border-t border-border-gray mx-1" />
              <button
                onClick={() => {
                  setShowSharingModal(true);
                  onClose();
                }}
                className={itemClass}
              >
                <HugeiconsIcon icon={Share01Icon} size={16} className="text-text-secondary" />
                <span>Sharing</span>
              </button>
            </>
          )}

          {/* Divider + Delete */}
          {onDelete && (
            <>
              <div className="my-1 border-t border-border-gray mx-1" />
              <button
                onClick={() => {
                  onClose();
                  onDelete();
                }}
                className="w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm text-red-600 hover:bg-red-50 transition-colors text-left"
              >
                <HugeiconsIcon icon={Delete02Icon} size={16} className="text-red-500" />
                <span>Delete file</span>
              </button>
            </>
          )}
        </div>
      </Dropdown>

      {/* Sharing modal */}
      {fileId && (
        <FilesSharingModal
          isOpen={showSharingModal}
          onClose={() => setShowSharingModal(false)}
          fileId={fileId}
        />
      )}
    </>
  );
}
