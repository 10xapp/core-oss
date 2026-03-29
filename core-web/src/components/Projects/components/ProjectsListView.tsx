import { useMemo, useState, useRef, useEffect } from "react";
import { PlusIcon } from "@heroicons/react/24/outline";
import { User, Tag, Calendar } from "lucide-react";
import { Icon } from "../../ui/Icon";
import { Flag } from "@phosphor-icons/react";
import { useProjectsStore } from "../../../stores/projectsStore";
import {
  useProjectBoard,
  useCreateIssue,
  useUpdateIssue,
} from "../../../hooks/queries/useProjects";
import CardDetailModal from "./CardDetailModal";
import DatePicker from "../../ui/DatePicker";
import StatusPicker from "./StatusPicker";
import AssigneePicker from "./AssigneePicker";
import PriorityPicker from "./PriorityPicker";
import LabelPicker from "./LabelPicker";

export default function ProjectsListView() {
  // UI state from store
  const activeProjectId = useProjectsStore((state) => state.activeProjectId);
  const filters = useProjectsStore((state) => state.filters);
  const selectedCardId = useProjectsStore((state) => state.selectedCardId);
  const setSelectedCard = useProjectsStore((state) => state.setSelectedCard);

  // React Query data
  const { data: boardData } = useProjectBoard(activeProjectId);
  const issues = boardData?.issues ?? [];
  const columns = useMemo(() => {
    if (!boardData?.states) return [];
    return [...boardData.states].sort((a, b) => a.position - b.position);
  }, [boardData?.states]);

  // React Query mutations
  const createIssue = useCreateIssue(activeProjectId);
  const updateIssue = useUpdateIssue(activeProjectId);

  const [newItemTitle, setNewItemTitle] = useState("");
  const [isCreating, setIsCreating] = useState(false);
  const [isEditing, setIsEditing] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus();
    }
  }, [isEditing]);

  const getStatusDotColor = (
    status: { name?: string; color?: string } | undefined,
  ) => {
    const name = status?.name?.trim().toLowerCase();
    if (name === "to do" || name === "todo" || name === "to-do") {
      if (
        !status?.color ||
        status.color === "#94A3B8" ||
        status.color === "#3B82F6"
      ) {
        return "#EF4444";
      }
    }
    return status?.color || "#94A3B8";
  };

  // Get done column IDs for filtering
  const doneColumnIds = useMemo(() => {
    return new Set(
      columns
        .filter((col) => {
          const name = col.name.toLowerCase();
          return name === 'done' || name === 'completed';
        })
        .map((col) => col.id)
    );
  }, [columns]);

  // Filter issues locally
  const filteredIssues = useMemo(() => {
    if (!issues.length) return [];
    const searchLower = filters.search.trim().toLowerCase();
    const hasStatusFilter = filters.statusIds.length > 0;
    const hasAssigneeFilter = filters.assigneeIds.length > 0;
    const hasLabelFilter = filters.labelIds.length > 0;
    const hasPriorityFilter = filters.priorities.length > 0;
    const dueFrom = filters.dueFrom ? new Date(filters.dueFrom) : null;
    const dueTo = filters.dueTo ? new Date(filters.dueTo) : null;
    if (dueFrom) dueFrom.setHours(0, 0, 0, 0);
    if (dueTo) dueTo.setHours(23, 59, 59, 999);

    const filtered = issues.filter((issue) => {
      // Filter out done items when showActiveOnly is true
      if (filters.showActiveOnly && doneColumnIds.has(issue.state_id)) {
        return false;
      }
      if (searchLower) {
        const title = issue.title?.toLowerCase() || '';
        const description = issue.description?.toLowerCase() || '';
        if (!title.includes(searchLower) && !description.includes(searchLower)) {
          return false;
        }
      }
      if (hasStatusFilter && !filters.statusIds.includes(issue.state_id)) {
        return false;
      }
      if (hasAssigneeFilter) {
        const assignees = issue.assignees || [];
        const match = assignees.some((a) => filters.assigneeIds.includes(a.user_id));
        if (!match) return false;
      }
      if (hasLabelFilter) {
        const labels = issue.label_objects || [];
        const match = labels.some((l) => filters.labelIds.includes(l.id));
        if (!match) return false;
      }
      if (hasPriorityFilter && !filters.priorities.includes(issue.priority || 0)) {
        return false;
      }
      if (dueFrom || dueTo) {
        if (!issue.due_at) return false;
        const dueDate = new Date(issue.due_at);
        if (dueFrom && dueDate < dueFrom) return false;
        if (dueTo && dueDate > dueTo) return false;
      }
      return true;
    });

    // Sort by created_at descending (newest first) for list view
    return [...filtered].sort((a, b) =>
      new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime()
    );
  }, [issues, filters, doneColumnIds]);

  const hasActiveFilters =
    filters.search.trim().length > 0 ||
    filters.statusIds.length > 0 ||
    filters.assigneeIds.length > 0 ||
    filters.labelIds.length > 0 ||
    filters.priorities.length > 0 ||
    !!filters.dueFrom ||
    !!filters.dueTo;
  const selectedCard = selectedCardId
    ? (issues.find((issue) => issue.id === selectedCardId) ?? null)
    : null;

  const handleStartEditing = () => {
    if (!isEditing) {
      setIsEditing(true);
      setNewItemTitle("");
    }
  };

  const handleCancelEditing = () => {
    setIsEditing(false);
    setNewItemTitle("");
  };

  const handleCreateItem = () => {
    if (!newItemTitle.trim() || !activeProjectId || columns.length === 0) {
      handleCancelEditing();
      return;
    }

    // Default to first column (usually To Do)
    const firstColumnId = columns[0].id;
    const title = newItemTitle.trim();

    // Hide the editing row immediately to avoid duplicate appearance
    setNewItemTitle("");
    setIsEditing(false);
    setIsCreating(true);

    createIssue.mutate(
      { state_id: firstColumnId, title },
      { onSettled: () => setIsCreating(false) }
    );
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleCreateItem();
    } else if (e.key === "Escape") {
      e.preventDefault();
      handleCancelEditing();
    }
  };

  if (!activeProjectId) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-text-tertiary">Select a project to view the list</p>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden h-full">
      <div className="flex-1 overflow-auto">
        <table className="w-full text-[13px] text-left table-fixed">
          <colgroup>
            <col className="w-[40%]" />
            <col className="w-[12%]" />
            <col className="w-[14%]" />
            <col className="w-[12%]" />
            <col className="w-[11%]" />
            <col className="w-[11%]" />
          </colgroup>
          <thead className="sticky top-0 z-10 bg-white border-b border-gray-100">
            <tr className="text-xs text-gray-500 uppercase tracking-wide">
              <th className="pl-4 pr-6 py-3.5 font-medium">Title</th>
              <th className="px-4 py-3.5 font-medium">Status</th>
              <th className="px-4 py-3.5 font-medium">Assignees</th>
              <th className="px-4 py-3.5 font-medium">Due</th>
              <th className="px-4 py-3.5 font-medium">Priority</th>
              <th className="px-4 py-3.5 font-medium">Labels</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {/* Quick Add Row - Always visible */}
            <tr
              className="hover:bg-gray-50 transition-colors group cursor-pointer"
              onClick={handleStartEditing}
            >
              <td className="pl-4 pr-6 py-3.5">
                <div className="flex items-center gap-2 text-gray-400">
                  <PlusIcon className="w-4 h-4" />
                  <span className="text-[13px]">Add new item...</span>
                </div>
              </td>
              <td className="px-4 py-3.5" />
              <td className="px-4 py-3.5" />
              <td className="px-4 py-3.5" />
              <td className="px-4 py-3.5" />
              <td className="px-4 py-3.5" />
            </tr>

            {/* New Item Row - Appears when editing */}
            {isEditing && (
              <tr className="bg-white">
                <td className="pl-4 pr-6 py-3.5 h-[52px]">
                  <div className="font-medium text-gray-900">
                    <input
                      ref={inputRef}
                      type="text"
                      value={newItemTitle}
                      onChange={(e) => setNewItemTitle(e.target.value)}
                      onKeyDown={handleKeyDown}
                      onBlur={() => {
                        if (!newItemTitle.trim()) {
                          handleCancelEditing();
                        }
                      }}
                      placeholder="Enter item title..."
                      disabled={isCreating}
                      className="w-full bg-transparent border-0 p-0 focus:ring-0 focus:outline-none outline-none text-[13px] text-gray-900 placeholder:text-gray-400 font-medium"
                    />
                  </div>
                </td>
                <td className="px-4 py-3.5 h-[52px]">
                  {columns.length > 0 && (
                    <div className="flex items-center gap-2 text-gray-500 -ml-2 pl-2">
                      <span
                        className="w-2 h-2 rounded-full"
                        style={{ backgroundColor: getStatusDotColor(columns[0]) }}
                      />
                      <span>{columns[0]?.name || "To Do"}</span>
                    </div>
                  )}
                </td>
                <td className="px-4 py-3.5 h-[52px]">
                  <div className="flex items-center gap-2.5 text-gray-400 -ml-2 pl-3">
                    <Icon icon={User} size={14} />
                    <span>—</span>
                  </div>
                </td>
                <td className="px-4 py-3.5 h-[52px]">
                  <div className="flex items-center gap-2 text-gray-400 -ml-2 pl-2">
                    <Icon icon={Calendar} size={14} />
                    <span>—</span>
                  </div>
                </td>
                <td className="px-4 py-3.5 h-[52px]">
                  <div className="flex items-center gap-1.5 text-gray-400 -ml-2 pl-2">
                    <Flag size={14} weight="fill" />
                    <span>—</span>
                  </div>
                </td>
                <td className="px-4 py-3.5 h-[52px]">
                  <div className="flex items-center gap-2 text-gray-400 -ml-2 pl-3">
                    <Icon icon={Tag} size={14} />
                    <span>—</span>
                  </div>
                </td>
              </tr>
            )}

            {filteredIssues.map((issue) => (
              <tr
                key={issue.id}
                className="hover:bg-gray-50 cursor-pointer transition-colors"
                onClick={() => setSelectedCard(issue.id)}
              >
                <td className="pl-4 pr-6 py-3.5">
                  <div className="font-medium text-gray-900">{issue.title}</div>
                  {issue.description && (
                    <div className="text-xs text-gray-400 truncate max-w-[360px] mt-0.5">
                      {issue.description}
                    </div>
                  )}
                </td>
                <td
                  className="px-4 py-3.5"
                  onClick={(e) => e.stopPropagation()}
                >
                  <StatusPicker
                    issueId={issue.id}
                    boardId={issue.board_id}
                    currentStatusId={issue.state_id}
                    buttonClassName="hover:bg-gray-100 -ml-2 w-full text-gray-500 hover:text-gray-700"
                  />
                </td>
                <td
                  className="px-4 py-3.5"
                  onClick={(e) => e.stopPropagation()}
                >
                  <AssigneePicker
                    issueId={issue.id}
                    boardId={issue.board_id}
                    currentAssignees={issue.assignees || []}
                    buttonClassName="hover:bg-gray-100 -ml-2 w-full text-gray-500 hover:text-gray-700"
                    emptyState="icon-dash"
                  />
                </td>
                <td
                  className="px-4 py-3.5"
                  onClick={(e) => e.stopPropagation()}
                >
                  <DatePicker
                    value={issue.due_at ? issue.due_at.slice(0, 10) : null}
                    onChange={(date) =>
                      updateIssue.mutate({
                        issueId: issue.id,
                        updates: {
                          due_at: date || undefined,
                          clear_due_at: !date,
                        },
                      })
                    }
                    placeholder="—"
                    showIcon={true}
                    showRelativeDate={false}
                    showClearButton={false}
                    buttonClassName="!px-2 !py-1.5 hover:bg-gray-100 -ml-2 !text-[13px] whitespace-nowrap"
                    className="w-max"
                  />
                </td>
                <td
                  className="px-4 py-3.5"
                  onClick={(e) => e.stopPropagation()}
                >
                  <PriorityPicker
                    issueId={issue.id}
                    boardId={issue.board_id}
                    priority={issue.priority}
                    buttonClassName="hover:bg-gray-100 -ml-2 w-full text-gray-500 hover:text-gray-700"
                  />
                </td>
                <td
                  className="px-4 py-3.5"
                  onClick={(e) => e.stopPropagation()}
                >
                  <LabelPicker
                    issueId={issue.id}
                    boardId={issue.board_id}
                    currentLabels={issue.label_objects || []}
                    buttonClassName="hover:bg-gray-100 -ml-2 w-full text-gray-500 hover:text-gray-700"
                    emptyState="icon-dash"
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {filteredIssues.length === 0 && (
          <div className="flex items-center justify-center py-12 text-[13px] text-gray-400">
            {hasActiveFilters ? "No matching results" : "No items yet"}
          </div>
        )}
      </div>

      {selectedCard && (
        <CardDetailModal
          card={selectedCard}
          initialEdit={true}
          onClose={() => setSelectedCard(null)}
        />
      )}
    </div>
  );
}
