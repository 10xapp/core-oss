import { Pencil, Users, Trash2, LogOut, Settings } from "lucide-react";
import { Icon } from "../ui/Icon";
import Dropdown from "../Dropdown/Dropdown";

interface MessagesSettingsDropdownProps {
  isOpen: boolean;
  onClose: () => void;
  trigger: React.RefObject<HTMLElement | null>;
  onRename?: () => void;
  onMembers?: () => void;
  memberCount?: number;
  onAppSettings?: () => void;
  onLeave?: () => void;
  onDelete?: () => void;
}

export default function MessagesSettingsDropdown({
  isOpen,
  onClose,
  trigger,
  onRename,
  onMembers,
  memberCount,
  onAppSettings,
  onLeave,
  onDelete,
}: MessagesSettingsDropdownProps) {
  const itemClass =
    "w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm text-text-body hover:bg-bg-gray transition-colors text-left";

  return (
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
            <Icon icon={Pencil} size={16} className="text-text-secondary" />
            <span>Rename channel</span>
          </button>
        )}

        {/* Members */}
        {onMembers && (
          <button
            onClick={() => {
              onMembers();
              onClose();
            }}
            className={itemClass}
          >
            <Icon icon={Users} size={16} className="text-text-secondary" />
            <span className="flex-1">Members</span>
            {memberCount != null && memberCount > 0 && (
              <span className="text-xs text-text-tertiary">{memberCount > 99 ? '99+' : memberCount}</span>
            )}
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

        {/* Leave channel */}
        {onLeave && (
          <>
            <div className="my-1 border-t border-border-gray mx-1" />
            <button
              onClick={() => {
                onClose();
                onLeave();
              }}
              className={itemClass}
            >
              <Icon icon={LogOut} size={16} className="text-text-secondary" />
              <span>Leave channel</span>
            </button>
          </>
        )}

        {/* Delete channel */}
        {onDelete && (
          <>
            <button
              onClick={() => {
                onClose();
                onDelete();
              }}
              className="w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm text-red-600 hover:bg-red-50 transition-colors text-left"
            >
              <Icon icon={Trash2} size={16} className="text-red-500" />
              <span>Delete channel</span>
            </button>
          </>
        )}
      </div>
    </Dropdown>
  );
}
