import { useState, useRef, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { motion, AnimatePresence } from 'motion/react';
import { XMarkIcon, TrashIcon, CalendarIcon, PhotoIcon, PlusIcon, EllipsisHorizontalIcon } from '@heroicons/react/24/outline';
import { Flag } from '@phosphor-icons/react';
import { useProjectsStore } from '../../../stores/projectsStore';
import { useAuthStore } from '../../../stores/authStore';
import {
  useProjectBoard,
  useProjectMembers,
  useUpdateIssue,
  useDeleteIssue,
  type ProjectIssue,
} from '../../../hooks/queries/useProjects';
import { getRelativeDate, formatDateFull } from '../../../utils/dateUtils';
import { getPresignedUploadUrl, confirmFileUpload } from '../../../api/client';
import { resolveUploadMimeType } from '../../../utils/uploadMime';
import DatePicker from '../../ui/DatePicker';
import LabelPicker from './LabelPicker';
import AssigneePicker from './AssigneePicker';
import StackedAvatars from './StackedAvatars';
import IssueComments from './IssueComments';
import { PROJECT_PRIORITY_OPTIONS } from './priorityOptions';

interface CardDetailModalProps {
  card: ProjectIssue;
  onClose: () => void;
  initialEdit?: boolean;
}

export default function CardDetailModal({ card, onClose, initialEdit = false }: CardDetailModalProps) {
  const [isEditing, setIsEditing] = useState(initialEdit);
  const [isVisible, setIsVisible] = useState(true);
  const [showMenu, setShowMenu] = useState(false);

  // React Query mutations
  const updateIssue = useUpdateIssue(card.board_id);
  const deleteIssue = useDeleteIssue(card.board_id);

  // React Query data
  const workspaceId = useProjectsStore((state) => state.workspaceId);
  const { data: boardData } = useProjectBoard(card.board_id);
  const { data: members = [] } = useProjectMembers(workspaceId);
  const states = boardData?.states ?? [];
  const currentUserId = useAuthStore((s) => s.user?.id);

  // Helper to get member by user_id
  const getMemberByUserId = useCallback(
    (userId: string) => members.find((m) => m.user_id === userId),
    [members]
  );

  const creator = card.created_by ? getMemberByUserId(card.created_by) : null;
  const currentMember = currentUserId ? members.find((m) => m.user_id === currentUserId) : null;
  const canDelete = card.created_by === currentUserId || currentMember?.role === 'owner' || currentMember?.role === 'admin';
  const mouseDownTargetRef = useRef<EventTarget | null>(null);

  const closeModal = () => {
    setIsVisible(false);
  };

  const handleClose = () => {
    // If editing and has a title, save changes before closing
    if (isEditing && title.trim()) {
      handleSave();
    } else {
      closeModal();
    }
  };

  // Normalize ISO timestamp to YYYY-MM-DD for date input
  const toDateInputValue = (iso: string | undefined | null): string => {
    if (!iso) return '';
    return iso.slice(0, 10);
  };

  const [title, setTitle] = useState(card.title);
  const [description, setDescription] = useState(card.description || '');
  const [priority, setPriority] = useState<number>(card.priority);
  const [showPriorityPicker, setShowPriorityPicker] = useState(false);
  const [dueDate, setDueDate] = useState(toDateInputValue(card.due_at));

  // Image state - track r2_keys for saving, urls for display
  const [imageR2Keys, setImageR2Keys] = useState<string[]>(card.image_r2_keys || []);
  const [imageUrls, setImageUrls] = useState<string[]>(card.image_urls || []);
  const [isUploadingImage, setIsUploadingImage] = useState(false);
  const imageInputRef = useRef<HTMLInputElement>(null);

  const column = states.find((s) => s.id === card.state_id);
  const priorityConfig = PROJECT_PRIORITY_OPTIONS.find((p) => p.value === priority);

  // Track which images were added/removed for atomic operations
  const [addedImageKeys, setAddedImageKeys] = useState<string[]>([]);
  const [removedImageKeys, setRemovedImageKeys] = useState<string[]>([]);

  const handleImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    await uploadImageFile(file);
    if (imageInputRef.current) imageInputRef.current.value = '';
  };

  const uploadImageFile = async (file: File) => {
    const mimeType = resolveUploadMimeType(file);
    if (!mimeType.startsWith('image/')) return;

    setIsUploadingImage(true);
    try {
      const uploadInfo = await getPresignedUploadUrl({
        workspaceId: card.workspace_id,
        filename: file.name,
        contentType: mimeType,
        fileSize: file.size,
        createDocument: false,
      });

      await fetch(uploadInfo.upload_url, {
        method: 'PUT',
        body: file,
        headers: { 'Content-Type': mimeType },
      });

      const confirmResult = await confirmFileUpload(uploadInfo.file_id, {
        createDocument: false,
      });

      const newR2Key = confirmResult.file.r2_key || uploadInfo.r2_key;
      const newUrl = confirmResult.file.public_url || uploadInfo.public_url;

      setImageR2Keys((prev) => [...prev, newR2Key]);
      setImageUrls((prev) => [...prev, newUrl]);
      setAddedImageKeys((prev) => [...prev, newR2Key]);
    } catch (error) {
      console.error('Failed to upload image:', error);
    } finally {
      setIsUploadingImage(false);
    }
  };


  const handlePaste = (e: React.ClipboardEvent) => {
    const imageFile = Array.from(e.clipboardData.files).find((f) =>
      f.type.startsWith('image/')
    );
    if (imageFile) {
      e.preventDefault();
      void uploadImageFile(imageFile);
    }
  };

  const handleRemoveImage = (index: number) => {
    const keyToRemove = imageR2Keys[index];
    setImageR2Keys((prev) => prev.filter((_, i) => i !== index));
    setImageUrls((prev) => prev.filter((_, i) => i !== index));
    // Only track removal if it was an original image (not newly added)
    if (!addedImageKeys.includes(keyToRemove)) {
      setRemovedImageKeys((prev) => [...prev, keyToRemove]);
    } else {
      // Remove from added list if it was just added
      setAddedImageKeys((prev) => prev.filter((k) => k !== keyToRemove));
    }
  };

  const handleSave = async () => {
    if (!title.trim()) return;

    // Optimistically close for faster perceived performance
    closeModal();

    // Build update object
    const updates: Parameters<typeof updateIssue.mutate>[0]['updates'] = {
      title: title.trim(),
      description: description.trim() || undefined,
      priority,
      due_at: dueDate || undefined,
      clear_due_at: !dueDate && !!card.due_at,
    };

    // Handle image changes - use atomic add/remove if possible
    if (addedImageKeys.length > 0 && removedImageKeys.length === 0) {
      // Only additions
      updates.add_image_r2_keys = addedImageKeys;
    } else if (removedImageKeys.length > 0 && addedImageKeys.length === 0) {
      // Only removals
      updates.remove_image_r2_keys = removedImageKeys;
    } else if (addedImageKeys.length > 0 || removedImageKeys.length > 0) {
      // Both add and remove - send full replacement
      updates.image_r2_keys = imageR2Keys;
    }

    updateIssue.mutate({ issueId: card.id, updates });
  };

  const handleDelete = () => {
    deleteIssue.mutate(card.id);
    closeModal();
  };

  return createPortal(
    <AnimatePresence onExitComplete={onClose}>
      {isVisible && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.15 }}
          className="fixed inset-0 bg-black/30 flex items-center justify-center z-9999"
          onMouseDown={(e) => { mouseDownTargetRef.current = e.target; }}
          onClick={(e) => e.target === e.currentTarget && mouseDownTargetRef.current === e.currentTarget && handleClose()}
        >
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: 10 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: 10 }}
            transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
            className="bg-white rounded-lg shadow-[0_24px_48px_-12px_rgba(0,0,0,0.15)] overflow-hidden flex flex-col"
            style={{ height: '720px', maxHeight: '90vh', width: '720px', maxWidth: 'calc(100% - 2rem)' }}
            onClick={(e) => e.stopPropagation()}
          >
        {/* Header */}
        <div className="flex items-start justify-between px-6 pt-6 pb-0">
          <div className="flex-1 pr-4">
            {column && (
              <div className="flex items-center gap-2 mb-3">
                <div
                  className="w-1.5 h-1.5 rounded-full"
                  style={{
                    backgroundColor:
                      column.name?.trim().toLowerCase() === 'to do' ||
                      column.name?.trim().toLowerCase() === 'todo' ||
                      column.name?.trim().toLowerCase() === 'to-do'
                        ? '#EF4444'
                        : (column.color || '#94a3b8'),
                  }}
                />
                <span className="text-[11px] font-medium text-gray-400 uppercase tracking-wide">
                  {column.name}
                </span>
              </div>
            )}
            {isEditing ? (
              <input
                type="text"
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                className="w-full text-lg font-medium text-gray-900 bg-transparent border-0 p-0 focus:outline-none focus:ring-0 placeholder:text-gray-300"
                placeholder="Card title"
                autoFocus
              />
            ) : (
              <h2 className="text-lg font-medium text-gray-900 leading-snug">
                {card.title}
              </h2>
            )}
          </div>
          <div className="flex items-center gap-1">
            <div className="relative">
              <button
                onClick={() => setShowMenu(!showMenu)}
                className="p-1.5 text-gray-300 hover:text-gray-500 rounded-lg transition-colors"
              >
                <EllipsisHorizontalIcon className="w-4.5 h-4.5 stroke-2" />
              </button>
              {showMenu && (
                <>
                  <div
                    className="fixed inset-0 z-10"
                    onClick={() => setShowMenu(false)}
                  />
                  <div className="absolute top-full right-0 z-20 mt-1 w-36 bg-white rounded-lg shadow-lg border border-gray-100 overflow-hidden py-1">
                    {canDelete ? (
                      <button
                        onClick={() => {
                          setShowMenu(false);
                          handleDelete();
                        }}
                        className="w-[calc(100%-8px)] mx-1 flex items-center gap-2 px-2 py-1.5 text-[12px] text-red-500 hover:bg-red-50 rounded-md transition-colors"
                      >
                        <TrashIcon className="w-4 h-4" />
                        Delete
                      </button>
                    ) : (
                      <button
                        disabled
                        title="Only the card creator or workspace admins can delete"
                        className="w-[calc(100%-8px)] mx-1 flex items-center gap-2 px-2 py-1.5 text-[12px] text-gray-300 cursor-not-allowed rounded-md"
                      >
                        <TrashIcon className="w-4 h-4" />
                        Delete
                      </button>
                    )}
                  </div>
                </>
              )}
            </div>
            <button
              onClick={handleClose}
              className="p-1.5 -mr-1 text-gray-300 hover:text-gray-500 rounded-lg transition-colors"
            >
              <XMarkIcon className="w-4.5 h-4.5 stroke-2" />
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="px-6 pb-6 pt-6 overflow-y-auto flex-1">
          {isEditing ? (
            <div className="space-y-5">
              {/* Row with Priority, Due Date, Labels, Assignees */}
              <div className="flex flex-wrap items-center gap-4">
                {/* Priority Picker */}
                <div className="relative">
                  <button
                    type="button"
                    onClick={() => setShowPriorityPicker(!showPriorityPicker)}
                    className="flex items-center gap-2 px-3 py-2 text-[13px] bg-white rounded-xl transition-all text-left hover:bg-gray-50/60"
                  >
                    {priority > 0 && priorityConfig ? (
                      <div className="flex items-center gap-1.5 flex-1 min-w-0">
                        <Flag size={14} weight="fill" className={priorityConfig.color} />
                        <span className="text-gray-700 font-medium">P{priority}</span>
                      </div>
                    ) : (
                      <div className="flex items-center gap-1.5 text-gray-400 flex-1">
                        <Flag size={14} weight="fill" />
                        <span>No priority</span>
                      </div>
                    )}
                  </button>

                  {showPriorityPicker && (
                    <>
                      <div
                        className="fixed inset-0 z-10"
                        onClick={() => setShowPriorityPicker(false)}
                      />
                      <div className="absolute top-full left-0 z-20 mt-1 w-40 bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden py-1">
                        <button
                          onClick={() => {
                            setPriority(0);
                            setShowPriorityPicker(false);
                          }}
                          className="w-full flex items-center gap-2 px-3 py-2 text-[12px] hover:bg-gray-50 transition-colors"
                        >
                          <span className="text-gray-400">No priority</span>
                          {priority === 0 && <span className="ml-auto text-blue-600">✓</span>}
                        </button>
                        {PROJECT_PRIORITY_OPTIONS.filter((o) => o.value > 0).map((option) => (
                          <button
                            key={option.value}
                            onClick={() => {
                              setPriority(option.value);
                              setShowPriorityPicker(false);
                            }}
                            className="w-full flex items-center gap-2 px-3 py-2 text-[12px] hover:bg-gray-50 transition-colors"
                          >
                            <Flag size={14} weight="fill" className={option.color} />
                            <span className="text-gray-700">Priority {option.value}</span>
                            {priority === option.value && <span className="ml-auto text-blue-600">✓</span>}
                          </button>
                        ))}
                      </div>
                    </>
                  )}
                </div>

                {/* Due Date */}
                <div>
                  <DatePicker
                    value={dueDate}
                    onChange={setDueDate}
                    placeholder="No due date"
                    buttonClassName="!px-3 hover:bg-gray-50/60"
                  />
                </div>

                {/* Labels */}
                <div>
                  <LabelPicker
                    issueId={card.id}
                    boardId={card.board_id}
                    currentLabels={card.label_objects || []}
                    buttonClassName="!px-3 !py-2 hover:bg-gray-50/60"
                  />
                </div>

                {/* Assignees */}
                <div>
                  <AssigneePicker
                    issueId={card.id}
                    boardId={card.board_id}
                    currentAssignees={card.assignees || []}
                    buttonClassName="!px-3 !py-2 hover:bg-gray-50/60"
                  />
                </div>
              </div>

              {/* Description */}
              <div className="border-t border-gray-100 pt-5">
                <textarea
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  onPaste={handlePaste}
                  rows={6}
                  className="w-full py-1.5 text-[13px] text-gray-700 bg-transparent border-0 focus:outline-none placeholder:text-gray-400 resize-none transition-all"
                  placeholder="Add a description..."
                />
              </div>

              {/* Attachments */}
              <div className="border-t border-gray-100 pt-5">
                <div className="flex items-center gap-2 mb-4">
                  <PhotoIcon className="w-4 h-4 text-gray-400" />
                  <span className="text-[12px] font-medium text-gray-500">Images</span>
                </div>
                <input
                  ref={imageInputRef}
                  type="file"
                  accept="image/*"
                  onChange={handleImageUpload}
                  className="hidden"
                />
                <div className="flex flex-wrap gap-3">
                  {imageUrls.map((url, index) => (
                    <div key={index} className="relative group w-16 h-16">
                      <img
                        src={url}
                        alt=""
                        className="w-full h-full object-cover rounded-lg"
                      />
                      <button
                        type="button"
                        onClick={() => handleRemoveImage(index)}
                        className="absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 text-white rounded-md opacity-0 group-hover:opacity-100 transition-opacity"
                      >
                        <XMarkIcon className="w-3 h-3" />
                      </button>
                    </div>
                  ))}
                  <button
                    type="button"
                    onClick={() => imageInputRef.current?.click()}
                    disabled={isUploadingImage}
                    className="w-16 h-16 flex items-center justify-center border border-border-gray hover:border-gray-300 rounded-lg text-gray-400 hover:text-gray-500 transition-colors disabled:opacity-50"
                  >
                    {isUploadingImage ? (
                      <span className="text-[10px]">...</span>
                    ) : (
                      <PlusIcon className="w-5 h-5" />
                    )}
                  </button>
                </div>
              </div>

              {/* Comments section */}
              <div className="border-t border-gray-100 pt-5">
                <IssueComments issueId={card.id} />
              </div>
            </div>
          ) : (
            <div className="space-y-5">
              {/* Priority and Due Date row */}
              <div className="flex items-center gap-4">
                {card.priority > 0 && (
                  <div className="flex items-center gap-1.5">
                    <Flag size={14} weight="fill" className={priorityConfig?.color} />
                    <span className="text-[12px] text-gray-500">Priority {card.priority}</span>
                  </div>
                )}
                {card.due_at && (() => {
                  const dateInfo = getRelativeDate(card.due_at);
                  return (
                    <>
                      {card.priority > 0 && <div className="w-px h-3 bg-gray-200" />}
                      <div className={`flex items-center gap-1.5 text-[12px] ${
                        dateInfo.isOverdue ? 'text-red-500' : dateInfo.isSoon ? 'text-amber-600' : 'text-gray-500'
                      }`}>
                        <CalendarIcon className="w-3.5 h-3.5" />
                        {dateInfo.text}
                      </div>
                    </>
                  );
                })()}
              </div>

              {/* Description */}
              {card.description ? (
                <p className="text-[13px] text-gray-600 leading-relaxed whitespace-pre-wrap">
                  {card.description}
                </p>
              ) : (
                <p className="text-[13px] text-gray-400 italic">No description</p>
              )}

              {/* Labels */}
              {card.label_objects && card.label_objects.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {card.label_objects.map((label) => (
                    <span
                      key={label.id}
                      className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-[11px] font-medium"
                      style={{
                        backgroundColor: label.color + '1F',
                        color: label.color,
                      }}
                    >
                        <span
                          className="w-1.5 h-1.5 rounded-full shrink-0"
                        style={{ backgroundColor: label.color }}
                      />
                      {label.name}
                    </span>
                  ))}
                </div>
              )}

              {/* Assignees */}
              {card.assignees && card.assignees.length > 0 && (
                <div className="flex items-center gap-2.5">
                  <StackedAvatars
                    userIds={card.assignees.map((a) => a.user_id)}
                    maxVisible={6}
                    size="md"
                  />
                </div>
              )}

              {/* Comments section */}
              <div className="border-t border-gray-100 pt-5">
                <IssueComments issueId={card.id} />
              </div>

              {/* Attachments/Images */}
              {card.image_urls && card.image_urls.length > 0 && (
                <div className="border-t border-gray-100 pt-5">
                  <div className="flex items-center gap-2 mb-4">
                    <PhotoIcon className="w-4 h-4 text-gray-400" />
                    <span className="text-[12px] font-medium text-gray-500">Attachments</span>
                  </div>
                  <div className="grid grid-cols-3 gap-3">
                    {card.image_urls.map((url, index) => (
                      <div key={index} className="aspect-video">
                        <img
                          src={url}
                          alt=""
                          className="w-full h-full object-cover rounded-lg"
                        />
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 pb-6 pt-4 border-t border-gray-100">
          <div className="flex items-center gap-2 text-[11px] text-gray-400">
            {creator && (
              creator.avatar_url ? (
                <img
                  src={creator.avatar_url}
                  alt={creator.name || creator.email || 'Creator'}
                  className="w-5 h-5 rounded-full object-cover"
                  title={creator.name || creator.email}
                />
              ) : (
                <div
                  className="w-5 h-5 rounded-full bg-gray-200 flex items-center justify-center text-[9px] font-medium text-gray-600"
                  title={creator.name || creator.email}
                >
                  {(creator.name || creator.email || '?').charAt(0).toUpperCase()}
                </div>
              )
            )}
            {card.created_at && `Created ${formatDateFull(card.created_at)}`}
          </div>
          <div className="flex items-center gap-2">
            {isEditing ? (
              <button
                onClick={handleSave}
                disabled={!title.trim()}
                className="px-3 py-2 text-[12px] font-medium bg-gray-900 text-white rounded-lg hover:bg-gray-800 disabled:opacity-40 transition-all"
              >
                Save
              </button>
            ) : (
              <button
                onClick={() => setIsEditing(true)}
                className="px-3 py-1.5 text-[12px] font-medium text-gray-500 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors"
              >
                Edit
              </button>
            )}
          </div>
        </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>,
    document.body
  );
}
