import { useDroppable } from "@dnd-kit/core";
import {
  SortableContext,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { useState, useRef, useEffect, memo } from "react";
import {
  PlusIcon,
  TrashIcon,
  EllipsisHorizontalIcon,
  PencilIcon,
  ChevronDownIcon,
  ChevronUpIcon,
} from "@heroicons/react/24/outline";
import { SwatchIcon } from "@heroicons/react/24/solid";
import ConfirmModal from "./ConfirmModal";
import Dropdown from "../../Dropdown/Dropdown";

const COLUMN_COLORS = [
  { value: "#94A3B8", name: "Slate" },
  { value: "#64748B", name: "Gray" },
  { value: "#3B82F6", name: "Blue" },
  { value: "#06B6D4", name: "Cyan" },
  { value: "#10B981", name: "Green" },
  { value: "#FBBF24", name: "Yellow" },
  { value: "#F59E0B", name: "Amber" },
  { value: "#F97316", name: "Orange" },
  { value: "#EF4444", name: "Red" },
];
import KanbanCard from "./KanbanCard";
import {
  useCreateIssue,
  useDeleteState,
  useUpdateState,
  type ProjectIssue,
  type ProjectState,
} from "../../../hooks/queries/useProjects";

interface KanbanColumnProps {
  column: ProjectState;
  boardId: string;
  cardsOverride: ProjectIssue[];
  onCardClick?: (cardId: string) => void;
  previewIndex?: number | null;
  activeCardId?: string | null;
  isDragActive?: boolean;
  isCollapsed?: boolean;
}

const KanbanColumn = memo(function KanbanColumn({
  column,
  boardId,
  cardsOverride,
  onCardClick,
  previewIndex,
  activeCardId,
  isDragActive = false,
  isCollapsed = false,
}: KanbanColumnProps) {
  const { setNodeRef, isOver } = useDroppable({
    id: column.id,
    data: {
      type: "column",
      columnId: column.id,
    },
  });

  // React Query mutations
  const createIssue = useCreateIssue(boardId);
  const deleteState = useDeleteState(boardId);
  const updateState = useUpdateState(boardId);

  const cards = cardsOverride;
  const [isExpanded, setIsExpanded] = useState(false);
  const [isAddingCard, setIsAddingCard] = useState(false);

  const VISIBLE_CARD_LIMIT = 10;
  const hasMoreCards = cards.length > VISIBLE_CARD_LIMIT;
  const visibleCards = isExpanded ? cards : cards.slice(0, VISIBLE_CARD_LIMIT);
  const hiddenCount = cards.length - VISIBLE_CARD_LIMIT;
  const [newCardTitle, setNewCardTitle] = useState("");
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [showColorPicker, setShowColorPicker] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [showColumnMenu, setShowColumnMenu] = useState(false);
  const [isEditingName, setIsEditingName] = useState(false);
  const [editingName, setEditingName] = useState(column.name);
  const colorPickerRef = useRef<HTMLDivElement>(null);
  const menuButtonRef = useRef<HTMLButtonElement>(null);

  // Close color picker on outside click
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        colorPickerRef.current &&
        !colorPickerRef.current.contains(e.target as Node)
      ) {
        setShowColorPicker(false);
      }
    };
    if (showColorPicker) {
      document.addEventListener("mousedown", handleClickOutside);
      return () =>
        document.removeEventListener("mousedown", handleClickOutside);
    }
  }, [showColorPicker]);

  const normalizeTodoColor = (color?: string) => {
    const name = column.name?.trim().toLowerCase();
    if (name === "to do" || name === "todo" || name === "to-do") {
      if (!color || color === "#94A3B8" || color === "#3B82F6") {
        return "#EF4444";
      }
    }
    return color || "#94A3B8";
  };

  const displayColor = normalizeTodoColor(column.color);

  const handleColorChange = (color: string) => {
    updateState.mutate({ stateId: column.id, updates: { color } });
    setShowColorPicker(false);
  };

  const handleAddCard = () => {
    const title = newCardTitle.trim();
    if (!title) {
      setIsAddingCard(false);
      return;
    }

    // Close form and clear input immediately
    setNewCardTitle("");
    setIsAddingCard(false);

    // React Query handles optimistic update
    createIssue.mutate({ state_id: column.id, title });
  };

  const handleDeleteColumn = () => {
    setShowColumnMenu(false);
    setShowDeleteConfirm(true);
  };

  const handleRenameClick = () => {
    setShowColumnMenu(false);
    setEditingName(column.name);
    setIsEditingName(true);
  };

  const handleRenameSubmit = () => {
    const trimmed = editingName.trim();
    if (trimmed && trimmed !== column.name) {
      updateState.mutate({ stateId: column.id, updates: { name: trimmed } });
    }
    setIsEditingName(false);
  };

  const handleRenameKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleRenameSubmit();
    } else if (e.key === "Escape") {
      setIsEditingName(false);
      setEditingName(column.name);
    }
  };

  const handleColorPickerFromMenu = () => {
    setShowColumnMenu(false);
    setShowColorPicker(true);
  };

  const handleDeleteConfirm = () => {
    deleteState.mutate(column.id);
    setShowDeleteConfirm(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleAddCard();
    } else if (e.key === "Escape") {
      setIsAddingCard(false);
      setNewCardTitle("");
    }
  };

  const columnClasses = [
    "w-80 h-full flex-shrink-0 flex flex-col transition-all duration-300",
    isOver && "scale-[1.01]",
  ]
    .filter(Boolean)
    .join(" ");

  // Collapsed view for Done column in Active mode
  if (isCollapsed) {
    return (
      <div
        ref={setNodeRef}
        className={`w-80 flex-shrink-0 rounded-md transition-colors duration-200 ${
          isOver ? "bg-bg-hover ring-2 ring-green-300" : "bg-bg-panel"
        }`}
      >
        <div className="flex items-center justify-between px-4 py-3">
          <div className="flex items-center gap-2">
            <div
              className="w-2 h-2 rounded-full shrink-0"
              style={{ backgroundColor: displayColor }}
            />
            <span className="font-medium text-[13px] text-gray-900">{column.name}</span>
          </div>
          <span className="text-[12px] text-gray-400">{cards.length}</span>
        </div>
      </div>
    );
  }

  return (
    <div className={columnClasses}>
      {/* Column wrapper with background */}
      <div
        className={`flex-1 flex flex-col rounded-md px-2 pt-2 pb-2 transition-colors duration-200 ${
          isOver ? "bg-bg-hover" : "bg-bg-panel"
        }`}
      >
        {/* Column Header */}
        <div
          className="shrink-0 pb-4 flex items-center justify-between group"
          onMouseEnter={() => setHoveredId(column.id)}
          onMouseLeave={() => setHoveredId(null)}
        >
          <div className="flex items-center gap-2 flex-1 min-w-0 px-2">
            <div className="relative" ref={colorPickerRef}>
              <button
                onClick={() => setShowColorPicker(!showColorPicker)}
                className="w-2 h-2 rounded-full shrink-0 hover:scale-125 transition-transform"
                style={{ backgroundColor: displayColor }}
                title="Change color"
              />
              {showColorPicker && (
                <div className="absolute top-full left-0 mt-2 p-2.5 bg-white rounded-lg shadow-lg border border-gray-100 z-10">
                  <div className="flex gap-2">
                    {COLUMN_COLORS.map((color) => (
                      <button
                        key={color.value}
                        onClick={() => handleColorChange(color.value)}
                        className={`w-5 h-5 rounded-full transition-all ${
                          column.color === color.value
                            ? "ring-2 ring-offset-1 ring-gray-400 scale-110"
                            : "hover:scale-110"
                        }`}
                        style={{ backgroundColor: color.value }}
                        title={color.name}
                      />
                    ))}
                  </div>
                </div>
              )}
            </div>
            {isEditingName ? (
              <input
                type="text"
                value={editingName}
                onChange={(e) => setEditingName(e.target.value)}
                onKeyDown={handleRenameKeyDown}
                onBlur={handleRenameSubmit}
                autoFocus
                className="font-medium text-[13px] text-gray-900 bg-white border border-gray-200 rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-gray-400 min-w-0 flex-1"
              />
            ) : (
              <h3 className="font-medium text-[13px] text-gray-900 truncate">
                {column.name}
              </h3>
            )}
            <span className="text-[12px] text-gray-400 font-normal shrink-0">
              {cards.length}
            </span>
          </div>

          {/* Column Actions (Hover) - Three-dots menu */}
          <div className="relative shrink-0">
            <button
              ref={menuButtonRef}
              onClick={() => setShowColumnMenu(!showColumnMenu)}
              className={`p-1.5 rounded-lg text-gray-300 hover:text-gray-500 hover:bg-gray-100 transition-colors duration-200 ${
                hoveredId === column.id || showColumnMenu
                  ? "opacity-100"
                  : "opacity-0"
              }`}
              title="Column options"
            >
              <EllipsisHorizontalIcon className="w-3.5 h-3.5" />
            </button>
            <Dropdown
              isOpen={showColumnMenu}
              onClose={() => setShowColumnMenu(false)}
              trigger={menuButtonRef}
            >
              <button
                onClick={handleRenameClick}
                className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
              >
                <PencilIcon className="w-3.5 h-3.5" />
                Rename
              </button>
              <button
                onClick={handleColorPickerFromMenu}
                className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
              >
                <SwatchIcon className="w-3.5 h-3.5" />
                Change color
              </button>
              <button
                onClick={handleDeleteColumn}
                className="w-full px-3 py-1.5 text-left text-sm text-red-500 hover:bg-bg-gray flex items-center gap-2"
              >
                <TrashIcon className="w-3.5 h-3.5" />
                Delete
              </button>
            </Dropdown>
          </div>
        </div>

        {/* Column Content */}
        <div ref={setNodeRef}>
          <SortableContext
            items={cards.map((c) => c.id)}
            strategy={verticalListSortingStrategy}
          >
            {visibleCards.map((card, index) => (
              <div key={card.id}>
                {/* Drop indicator before this card */}
                {previewIndex === index && activeCardId !== card.id && (
                  <div className="h-0.5 bg-gray-300 rounded-full my-2 transition-colors duration-200" />
                )}
                <KanbanCard
                  card={card}
                  onCardClick={onCardClick}
                  isDragging={activeCardId === card.id}
                  isDragActive={isDragActive}
                />
              </div>
            ))}
            {/* Drop indicator at end of column */}
            {previewIndex === cards.length && cards.length > 0 && (
              <div className="h-0.5 bg-gray-300 rounded-full my-2 transition-colors duration-200" />
            )}
            {/* Indicator for empty column */}
            {cards.length === 0 && isOver && (
              <div className="h-0.5 bg-gray-300 rounded-full my-2 transition-colors duration-200" />
            )}
          </SortableContext>

          {/* Add Card Input - appears directly below cards when adding */}
          {isAddingCard && (
            <div className="mt-2 p-4 bg-white rounded-lg border border-gray-100">
              <input
                type="text"
                autoFocus
                value={newCardTitle}
                onChange={(e) => setNewCardTitle(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Enter a title..."
                className="w-full px-0 py-1 font-medium text-[13px] text-gray-900 placeholder:text-gray-400 placeholder:font-normal bg-transparent border-0 focus:outline-none focus:ring-0"
              />
              <div className="flex items-center justify-end gap-2 mt-4">
                <button
                  onClick={() => {
                    setIsAddingCard(false);
                    setNewCardTitle("");
                  }}
                  className="px-3 py-2 text-[12px] text-gray-500 hover:text-gray-700 font-medium transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={handleAddCard}
                  disabled={!newCardTitle.trim()}
                  className="px-3 py-2 text-[12px] bg-gray-900 text-white rounded-lg font-medium hover:bg-gray-800 disabled:opacity-40 disabled:cursor-not-allowed transition-all"
                >
                  Add card
                </button>
              </div>
            </div>
          )}

          {/* See all / Show less toggle */}
          {hasMoreCards && (
            <button
              onClick={() => setIsExpanded(!isExpanded)}
              className="w-full mt-2 py-2 text-[12px] text-gray-500 hover:text-gray-700 rounded-lg flex items-center justify-center gap-1 transition-colors duration-200 hover:bg-white/60"
            >
              {isExpanded ? (
                <>
                  <ChevronUpIcon className="w-3.5 h-3.5" />
                  <span>Show less</span>
                </>
              ) : (
                <>
                  <ChevronDownIcon className="w-3.5 h-3.5" />
                  <span>See all ({hiddenCount} more)</span>
                </>
              )}
            </button>
          )}

          {/* Add Card Button - stays at bottom */}
          {!isAddingCard && (
            <button
              onClick={() => setIsAddingCard(true)}
              className="w-full mt-2 py-3 text-[13px] text-gray-400 hover:text-gray-600 rounded-lg flex items-center justify-center gap-1.5 transition-colors duration-200 hover:bg-white/60"
            >
              <PlusIcon className="w-4 h-4 stroke-2" />
              <span>Add card</span>
            </button>
          )}
        </div>
      </div>

      {/* Delete Confirmation Modal */}
      <ConfirmModal
        isOpen={showDeleteConfirm}
        title="Delete column"
        message={`Are you sure you want to delete "${column.name}"? All cards in this column will also be deleted.`}
        confirmLabel="Delete"
        onConfirm={handleDeleteConfirm}
        onCancel={() => setShowDeleteConfirm(false)}
      />
    </div>
  );
});

export default KanbanColumn;
