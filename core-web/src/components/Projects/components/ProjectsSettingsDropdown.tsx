import { Pencil, Settings, Trash2 } from "lucide-react";
import { Icon } from "../../ui/Icon";
import Dropdown from "../../Dropdown/Dropdown";

interface ProjectsSettingsDropdownProps {
  isOpen: boolean;
  onClose: () => void;
  trigger: React.RefObject<HTMLElement | null>;
  onBoardSettings?: () => void;
  onAppSettings?: () => void;
  onDelete?: () => void;
}

export default function ProjectsSettingsDropdown({
  isOpen,
  onClose,
  trigger,
  onBoardSettings,
  onAppSettings,
  onDelete,
}: ProjectsSettingsDropdownProps) {
  const itemClass =
    "w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm text-text-body hover:bg-bg-gray transition-colors text-left";

  return (
    <Dropdown isOpen={isOpen} onClose={onClose} trigger={trigger} align="right">
      <div className="w-[220px] px-1.5 py-1">
        {/* Board Settings */}
        {onBoardSettings && (
          <button
            onClick={() => {
              onBoardSettings();
              onClose();
            }}
            className={itemClass}
          >
            <Icon icon={Pencil} size={16} className="text-text-secondary" />
            <span>Board settings</span>
          </button>
        )}

        {/* App Settings */}
        {onAppSettings && (
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
        )}

        {/* Delete board */}
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
              <Icon icon={Trash2} size={16} className="text-red-500" />
              <span>Delete board</span>
            </button>
          </>
        )}
      </div>
    </Dropdown>
  );
}
