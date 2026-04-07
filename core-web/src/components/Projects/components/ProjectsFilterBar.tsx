import { useMemo, useRef, useEffect, useState } from "react";
import { motion, AnimatePresence } from "motion/react";
import { XMarkIcon } from "@heroicons/react/24/outline";
import { Filter, Columns3, List } from "lucide-react";
import { Icon } from "../../ui/Icon";
import { Flag } from "@phosphor-icons/react";
import { useProjectsStore } from "../../../stores/projectsStore";
import { useProjectBoard, useProjectMembers } from "../../../hooks/queries/useProjects";
import { PROJECT_PRIORITY_OPTIONS } from "./priorityOptions";

interface ProjectsFilterBarProps {
  viewMode: "kanban" | "list";
  setViewMode: (mode: "kanban" | "list") => void;
}

export default function ProjectsFilterBar({
  viewMode,
  setViewMode,
}: ProjectsFilterBarProps) {
  const [isOpen, setIsOpen] = useState(false);
  const panelRef = useRef<HTMLDivElement>(null);

  // UI state from store
  const filters = useProjectsStore((state) => state.filters);
  const setFilters = useProjectsStore((state) => state.setFilters);
  const clearFilters = useProjectsStore((state) => state.clearFilters);
  const activeProjectId = useProjectsStore((state) => state.activeProjectId);
  const workspaceId = useProjectsStore((state) => state.workspaceId);

  // React Query data
  const { data: boardData } = useProjectBoard(activeProjectId);
  const { data: members = [] } = useProjectMembers(workspaceId);
  const boardStates = boardData?.states;
  const boardLabels = boardData?.labels;

  const columns = useMemo(() => {
    if (!boardStates) return [];
    return [...boardStates].sort((a, b) => a.position - b.position);
  }, [boardStates]);

  useEffect(() => {
    if (!isOpen) return;
    const handleClickOutside = (event: MouseEvent) => {
      if (
        panelRef.current &&
        !panelRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [isOpen]);

  const statusOptions = useMemo(() => {
    return columns.map((state) => ({
      id: state.id,
      label: state.name,
    }));
  }, [columns]);

  const assigneeOptions = useMemo(() => {
    return members.map((member) => ({
      id: member.user_id,
      label: member.name || member.email || "Unknown",
      avatar_url: member.avatar_url,
      initials: member.name
        ? member.name
            .split(" ")
            .map((n) => n[0])
            .join("")
            .toUpperCase()
            .slice(0, 2)
        : (member.email?.[0] || "?").toUpperCase(),
    }));
  }, [members]);

  const labelOptions = useMemo(() => {
    if (!boardLabels) return [];
    return boardLabels.map((label) => ({
      id: label.id,
      label: label.name,
      color: label.color,
    }));
  }, [boardLabels]);

  const toggleArray = (values: string[], id: string) =>
    values.includes(id)
      ? values.filter((value) => value !== id)
      : [...values, id];

  const toggleNumberArray = (values: number[], id: number) =>
    values.includes(id)
      ? values.filter((value) => value !== id)
      : [...values, id];

  const hasActiveFilters =
    filters.search.trim().length > 0 ||
    filters.statusIds.length > 0 ||
    filters.assigneeIds.length > 0 ||
    filters.labelIds.length > 0 ||
    filters.priorities.length > 0 ||
    !!filters.dueFrom ||
    !!filters.dueTo;

  return (
    <div className="h-11 flex items-center justify-between pl-2.5 pr-3 border-b border-border-gray">
      <div className="flex items-center gap-3">
        {/* Filter Button */}
        <div className="relative" ref={panelRef}>
          <button
            type="button"
            onClick={() => setIsOpen((prev) => !prev)}
            className={`inline-flex items-center gap-1.5 h-8 px-2.5 text-sm rounded-lg transition-colors ${
              isOpen || hasActiveFilters
                ? "bg-gray-100 text-gray-900"
                : "text-gray-500 hover:bg-gray-100 hover:text-gray-700"
            }`}
            aria-label="Filters"
          >
            <Icon icon={Filter} size={16} />
            {!hasActiveFilters && (
              <span className="text-[13px] font-medium">Filter</span>
            )}
          </button>

        {isOpen && (
          <div className="absolute left-0 mt-2 w-[340px] bg-white rounded-xl shadow-xl border border-gray-100 p-5 z-30">
            <div className="space-y-6">
              {/* Assignee Section */}
              <div>
                <div className="text-[13px] font-medium text-gray-900 mb-3">
                  Assignee
                </div>
                <div className="flex flex-wrap gap-2 -m-0.5 p-0.5">
                  {assigneeOptions.length === 0 ? (
                    <div className="text-xs text-gray-400 italic">
                      No members found
                    </div>
                  ) : (
                    assigneeOptions.map((member) => {
                      const isSelected = filters.assigneeIds.includes(
                        member.id,
                      );
                      return (
                        <button
                          key={member.id}
                          onClick={() =>
                            setFilters({
                              assigneeIds: toggleArray(
                                filters.assigneeIds,
                                member.id,
                              ),
                            })
                          }
                          className={`w-8 h-8 rounded-full flex items-center justify-center text-[11px] font-medium transition-all ${
                            isSelected
                              ? "ring-2 ring-offset-1 ring-gray-900"
                              : "hover:opacity-80"
                          }`}
                          title={member.label}
                        >
                          {member.avatar_url ? (
                            <img
                              src={member.avatar_url}
                              alt={member.label}
                              className="w-full h-full rounded-full object-cover"
                            />
                          ) : (
                            <div
                              className={`w-full h-full rounded-full flex items-center justify-center border-2 ${
                                isSelected
                                  ? "border-gray-900 bg-gray-900 text-white"
                                  : "border-transparent bg-gray-100 text-gray-600"
                              }`}
                            >
                              {member.initials}
                            </div>
                          )}
                        </button>
                      );
                    })
                  )}
                </div>
              </div>

              {/* Priority Section */}
              <div>
                <div className="text-[13px] font-medium text-gray-900 mb-3">
                  Priority
                </div>
                <div className="flex flex-wrap gap-2 -m-0.5 p-0.5">
                  {PROJECT_PRIORITY_OPTIONS.map((option) => {
                    const isSelected = filters.priorities.includes(
                      option.value,
                    );
                    return (
                      <button
                        key={option.value}
                        onClick={() =>
                          setFilters({
                            priorities: toggleNumberArray(
                              filters.priorities,
                              option.value,
                            ),
                          })
                        }
                        className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[12px] font-medium transition-all ${
                          isSelected
                            ? `${option.activeBg} text-gray-900`
                            : "ring-1 ring-inset ring-gray-200 bg-white text-gray-500 hover:ring-gray-300 hover:bg-gray-50"
                        }`}
                      >
                        {option.value > 0 ? (
                          <Flag
                            size={12}
                            weight="fill"
                            className={option.color}
                          />
                        ) : (
                          <span className="text-gray-400">–</span>
                        )}
                        <span>{option.shortLabel}</span>
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Labels Section */}
              <div>
                <div className="text-[13px] font-medium text-gray-900 mb-3">
                  Labels
                </div>
                <div className="flex flex-wrap gap-2 -m-0.5 p-0.5">
                  {labelOptions.length === 0 ? (
                    <div className="text-xs text-gray-400 italic">
                      No labels found
                    </div>
                  ) : (
                    labelOptions.map((label) => {
                      const isSelected = filters.labelIds.includes(label.id);
                      return (
                        <button
                          key={label.id}
                          onClick={() =>
                            setFilters({
                              labelIds: toggleArray(filters.labelIds, label.id),
                            })
                          }
                          className={`px-3 py-1 rounded-lg text-[12px] font-medium transition-all ${
                            isSelected
                              ? "ring-1 ring-inset ring-black/10"
                              : "ring-1 ring-inset ring-gray-200 hover:ring-gray-300"
                          }`}
                          style={{
                            backgroundColor: isSelected
                              ? label.color + "25"
                              : "white",
                            color: isSelected ? label.color : "#6B7280",
                          }}
                        >
                          {label.label}
                        </button>
                      );
                    })
                  )}
                </div>
              </div>

              {/* Status Section - keeping simple pills */}
              <div>
                <div className="text-[13px] font-medium text-gray-900 mb-3">
                  Status
                </div>
                <div className="flex flex-wrap gap-2 -m-0.5 p-0.5">
                  {statusOptions.length === 0 ? (
                    <div className="text-xs text-gray-400 italic">
                      No statuses found
                    </div>
                  ) : (
                    statusOptions.map((status) => {
                      const isSelected = filters.statusIds.includes(status.id);
                      return (
                        <button
                          key={status.id}
                          onClick={() =>
                            setFilters({
                              statusIds: toggleArray(
                                filters.statusIds,
                                status.id,
                              ),
                            })
                          }
                          className={`px-3 py-1 rounded-lg text-[12px] font-medium transition-all ${
                            isSelected
                              ? "bg-gray-900 text-white"
                              : "bg-white text-gray-600 ring-1 ring-inset ring-gray-200 hover:ring-gray-300 hover:bg-gray-50"
                          }`}
                        >
                          {status.label}
                        </button>
                      );
                    })
                  )}
                </div>
              </div>
            </div>

            <div className="mt-6 pt-4 border-t border-gray-100 flex items-center justify-between">
              <button
                type="button"
                onClick={clearFilters}
                className="text-xs font-medium text-gray-500 hover:text-gray-900 transition-colors"
              >
                Clear all
              </button>
              <button
                type="button"
                onClick={() => setIsOpen(false)}
                className="px-3 py-1.5 text-xs font-medium bg-gray-900 text-white rounded-lg hover:bg-gray-800 transition-colors"
              >
                Done
              </button>
            </div>
          </div>
        )}
        </div>

        <div className="flex items-center gap-2 overflow-x-auto no-scrollbar -m-1 p-1">
        <AnimatePresence mode="popLayout">
          {/* Active Priority Filters */}
          {filters.priorities.map((priority) => {
            const config = PROJECT_PRIORITY_OPTIONS.find((p) => p.value === priority);
            if (!config) return null;
            return (
              <motion.div
                key={`priority-${priority}`}
                layout
                initial={{ opacity: 0, x: -12, scale: 0.9 }}
                animate={{ opacity: 1, x: 0, scale: 1 }}
                exit={{ opacity: 0, x: -12, scale: 0.9 }}
                transition={{ duration: 0.2 }}
                onClick={() =>
                  setFilters({
                    priorities: toggleNumberArray(filters.priorities, priority),
                  })
                }
                className={`group flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg ${config.activeBg} text-[12px] font-medium text-gray-900 flex-shrink-0 cursor-pointer`}
              >
                <span className="relative w-3 h-3 flex items-center justify-center">
                  <Flag
                    size={12}
                    weight="fill"
                    className={`${config.color} group-hover:opacity-0 transition-opacity`}
                  />
                  <XMarkIcon className="w-3 h-3 text-gray-600 absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity" />
                </span>
                <span>{config.shortLabel}</span>
              </motion.div>
            );
          })}

          {/* Active Assignee Filters (Stacked) */}
          {filters.assigneeIds.length > 0 && (
            <motion.div
              layout
              key="assignees-group"
              initial={{ opacity: 0, x: -12 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -12 }}
              transition={{ duration: 0.15, ease: "easeOut" }}
              className="flex flex-shrink-0"
            >
              <AnimatePresence mode="popLayout">
                {filters.assigneeIds.map((assigneeId, index) => {
                  const member = assigneeOptions.find(
                    (m) => m.id === assigneeId,
                  );
                  if (!member) return null;
                  return (
                    <motion.div
                      key={`assignee-${assigneeId}`}
                      layout
                      initial={{ opacity: 0, scale: 0.8 }}
                      animate={{ opacity: 1, scale: 1 }}
                      exit={{ opacity: 0, scale: 0.8 }}
                      transition={{
                        duration: 0.15,
                        ease: "easeOut",
                        layout: { duration: 0.15, ease: "easeOut" },
                      }}
                      onClick={() =>
                        setFilters({
                          assigneeIds: toggleArray(
                            filters.assigneeIds,
                            assigneeId,
                          ),
                        })
                      }
                      className={`relative z-0 hover:z-10 cursor-pointer group ${index > 0 ? "-ml-2" : ""}`}
                    >
                      <div className="w-7 h-7 rounded-full ring-2 ring-white overflow-hidden relative bg-white">
                        {member.avatar_url ? (
                          <img
                            src={member.avatar_url}
                            alt={member.label}
                            className="w-full h-full object-cover group-hover:opacity-50 transition-opacity"
                            title={member.label}
                          />
                        ) : (
                          <div
                            className="w-full h-full flex items-center justify-center bg-gray-100 text-gray-600 text-[10px] font-medium group-hover:opacity-50 transition-opacity"
                            title={member.label}
                          >
                            {member.initials}
                          </div>
                        )}
                        <div className="absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity">
                          <XMarkIcon className="w-3.5 h-3.5 text-gray-900" />
                        </div>
                      </div>
                    </motion.div>
                  );
                })}
              </AnimatePresence>
            </motion.div>
          )}

          {/* Active Label Filters */}
          {filters.labelIds.map((labelId) => {
            const label = labelOptions.find((l) => l.id === labelId);
            if (!label) return null;
            return (
              <motion.div
                key={`label-${labelId}`}
                layout
                initial={{ opacity: 0, x: -12, scale: 0.9 }}
                animate={{ opacity: 1, x: 0, scale: 1 }}
                exit={{ opacity: 0, x: -12, scale: 0.9 }}
                transition={{ duration: 0.2 }}
                className="px-2.5 py-1 rounded-lg text-[12px] font-medium border border-transparent flex-shrink-0"
                style={{
                  backgroundColor: label.color + "25",
                  color: label.color,
                }}
              >
                {label.label}
              </motion.div>
            );
          })}

          {/* Active Status Filters */}
          {filters.statusIds.map((statusId) => {
            const status = statusOptions.find((s) => s.id === statusId);
            if (!status) return null;
            return (
              <motion.div
                key={`status-${statusId}`}
                layout
                initial={{ opacity: 0, scale: 0.8, width: 0 }}
                animate={{ opacity: 1, scale: 1, width: "auto" }}
                exit={{ opacity: 0, scale: 0.8, width: 0 }}
                transition={{ duration: 0.2, ease: "easeInOut" }}
                onClick={() =>
                  setFilters({
                    statusIds: toggleArray(filters.statusIds, statusId),
                  })
                }
                className="group relative px-2.5 py-1 rounded-lg text-[12px] font-medium bg-gray-900 text-white flex-shrink-0 cursor-pointer overflow-hidden"
              >
                <span className="group-hover:opacity-0 transition-opacity duration-200 whitespace-nowrap">
                  {status.label}
                </span>
                <div className="absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity duration-200 bg-white/10">
                  <XMarkIcon className="w-3.5 h-3.5 text-white" />
                </div>
              </motion.div>
            );
          })}
        </AnimatePresence>
        </div>
      </div>

      {/* Right side: All/Active + View Toggle */}
      <div className="flex items-center gap-1">
        {/* All / Active Buttons */}
        <button
          type="button"
          onClick={() => setFilters({ showActiveOnly: false })}
          className={`h-7 px-2.5 text-[13px] font-medium rounded-md transition-colors ${
            !filters.showActiveOnly
              ? "bg-black/6 text-text-body"
              : "text-text-tertiary hover:text-text-secondary"
          }`}
        >
          All
        </button>
        <button
          type="button"
          onClick={() => setFilters({ showActiveOnly: true })}
          className={`h-7 px-2.5 text-[13px] font-medium rounded-md transition-colors ${
            filters.showActiveOnly
              ? "bg-black/6 text-text-body"
              : "text-text-tertiary hover:text-text-secondary"
          }`}
        >
          Active
        </button>

        {/* Separator */}
        <div className="w-px h-4 bg-border-gray mx-1" />

        {/* View Toggle */}
        <button
          type="button"
          onClick={() => setViewMode("kanban")}
          className={`h-7 w-7 flex items-center justify-center rounded-md transition-colors ${
            viewMode === "kanban"
              ? "bg-black/6 text-text-body"
              : "text-text-tertiary hover:text-text-secondary"
          }`}
          aria-label="Kanban view"
        >
          <Icon icon={Columns3} size={14} />
        </button>
        <button
          type="button"
          onClick={() => setViewMode("list")}
          className={`h-7 w-7 flex items-center justify-center rounded-md transition-colors ${
            viewMode === "list"
              ? "bg-black/6 text-text-body"
              : "text-text-tertiary hover:text-text-secondary"
          }`}
          aria-label="List view"
        >
          <Icon icon={List} size={14} />
        </button>
      </div>
    </div>
  );
}
