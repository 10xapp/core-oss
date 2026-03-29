import { Settings, Trash2, Archive, Undo2 } from "lucide-react";
import { Icon } from "../ui/Icon";
import Dropdown from "../Dropdown/Dropdown";

interface EmailSettingsDropdownProps {
  isOpen: boolean;
  onClose: () => void;
  trigger: React.RefObject<HTMLElement | null>;
  onArchive?: () => void;
  onDelete?: () => void;
  onRestore?: () => void;
  onAppSettings?: () => void;
}

export default function EmailSettingsDropdown({
  isOpen,
  onClose,
  trigger,
  onArchive,
  onDelete,
  onRestore,
  onAppSettings,
}: EmailSettingsDropdownProps) {
  const itemClass =
    "w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm text-text-body hover:bg-bg-gray transition-colors text-left";

  return (
    <Dropdown isOpen={isOpen} onClose={onClose} trigger={trigger} align="right">
      <div className="w-[220px] px-1.5 py-1">
        {/* Restore (trash folder) */}
        {onRestore && (
          <button
            onClick={() => {
              onRestore();
              onClose();
            }}
            className={itemClass}
          >
            <Icon icon={Undo2} size={16} className="text-text-secondary" />
            <span>Restore</span>
          </button>
        )}

        {/* Archive */}
        {onArchive && (
          <button
            onClick={() => {
              onArchive();
              onClose();
            }}
            className={itemClass}
          >
            <Icon icon={Archive} size={16} className="text-text-secondary" />
            <span>Archive</span>
          </button>
        )}

        {/* Delete */}
        {onDelete && (
          <button
            onClick={() => {
              onClose();
              onDelete();
            }}
            className="w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm text-red-600 hover:bg-red-50 transition-colors text-left"
          >
            <Icon icon={Trash2} size={16} className="text-red-500" />
            <span>Delete</span>
          </button>
        )}

        {/* App Settings */}
        {onAppSettings && (
          <>
            <div className="my-1 border-t border-border-gray mx-1" />
            <button
              onClick={() => {
                onAppSettings();
                onClose();
              }}
              className={itemClass}
            >
              <Icon icon={Settings} size={16} className="text-text-secondary" />
              <span>App settings</span>
            </button>
          </>
        )}
      </div>
    </Dropdown>
  );
}
