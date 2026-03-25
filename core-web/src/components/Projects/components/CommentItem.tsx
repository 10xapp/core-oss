import { useState } from 'react';
import { EllipsisHorizontalIcon, PencilIcon, TrashIcon } from '@heroicons/react/24/outline';
import { avatarGradient } from '../../../utils/avatarGradient';
import type { IssueComment, ContentBlock } from '../../../api/client';
import { formatDistanceToNow } from 'date-fns';

interface CommentItemProps {
  comment: IssueComment;
  currentUserId?: string;
  onEdit: (commentId: string, blocks: ContentBlock[]) => void;
  onDelete: (commentId: string) => void;
  onReact: (commentId: string, emoji: string) => void;
  onRemoveReact: (commentId: string, emoji: string) => void;
}

const QUICK_EMOJIS = ['👍', '❤️', '🎉', '👀', '🚀', '👎'];

export default function CommentItem({
  comment,
  currentUserId,
  onEdit,
  onDelete,
  onReact,
  onRemoveReact,
}: CommentItemProps) {
  const [showMenu, setShowMenu] = useState(false);
  const [isEditing, setIsEditing] = useState(false);
  const [editText, setEditText] = useState('');
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);

  const isOwn = comment.user_id === currentUserId;

  // Extract text content from blocks (with fallback to content field)
  const renderContent = () => {
    // Fallback to plain content if blocks is empty
    if (!comment.blocks || comment.blocks.length === 0) {
      return comment.content ? (
        <span className="whitespace-pre-wrap">{comment.content}</span>
      ) : (
        <span className="text-gray-400 italic">No content</span>
      );
    }

    return comment.blocks.map((block, idx) => {
      if (block.type === 'text') {
        return (
          <span key={idx} className="whitespace-pre-wrap">
            {(block.data as { content?: string }).content || ''}
          </span>
        );
      }
      if (block.type === 'mention') {
        return (
          <span key={idx} className="text-blue-600 font-medium">
            @{(block.data as { display_name?: string }).display_name || 'someone'}
          </span>
        );
      }
      if (block.type === 'code') {
        return (
          <code key={idx} className="px-1.5 py-0.5 bg-gray-100 rounded text-[12px] font-mono">
            {(block.data as { content?: string }).content || ''}
          </code>
        );
      }
      if (block.type === 'quote') {
        return (
          <blockquote key={idx} className="border-l-2 border-gray-300 pl-2 italic text-gray-600">
            {(block.data as { preview?: string }).preview || ''}
          </blockquote>
        );
      }
      return null;
    });
  };

  const startEdit = () => {
    // Extract plain text for editing
    const text = comment.blocks
      .filter((b) => b.type === 'text')
      .map((b) => (b.data as { content?: string }).content || '')
      .join('\n');
    setEditText(text);
    setIsEditing(true);
    setShowMenu(false);
  };

  const saveEdit = () => {
    if (!editText.trim()) return;
    const blocks: ContentBlock[] = [
      { type: 'text', data: { content: editText.trim() } },
    ];
    onEdit(comment.id, blocks);
    setIsEditing(false);
  };

  const cancelEdit = () => {
    setIsEditing(false);
    setEditText('');
  };

  // Group reactions by emoji
  const reactionGroups = (comment.reactions || []).reduce<Record<string, { count: number; userIds: string[] }>>(
    (acc, r) => {
      if (!acc[r.emoji]) acc[r.emoji] = { count: 0, userIds: [] };
      acc[r.emoji].count++;
      acc[r.emoji].userIds.push(r.user_id);
      return acc;
    },
    {}
  );

  const handleReactionClick = (emoji: string) => {
    const group = reactionGroups[emoji];
    if (group && currentUserId && group.userIds.includes(currentUserId)) {
      onRemoveReact(comment.id, emoji);
    } else {
      onReact(comment.id, emoji);
    }
    setShowEmojiPicker(false);
  };

  const getInitials = () => {
    if (comment.user?.name) {
      return comment.user.name
        .split(' ')
        .map((n) => n[0])
        .join('')
        .toUpperCase()
        .slice(0, 2);
    }
    return (comment.user?.email?.[0] || '?').toUpperCase();
  };

  const timeAgo = comment.created_at
    ? formatDistanceToNow(new Date(comment.created_at), { addSuffix: true })
    : '';

  return (
    <div className="group flex gap-3 py-3 hover:bg-gray-50/50 px-4 -mx-4 rounded-lg transition-colors">
      {/* Avatar */}
      {comment.user?.avatar_url ? (
        <img
          src={comment.user.avatar_url}
          alt={comment.user.name || comment.user.email || 'User'}
          className="w-7 h-7 rounded-full object-cover flex-shrink-0"
        />
      ) : (
        <div className="w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0" style={{ background: avatarGradient(comment.user?.name || comment.user?.email || '?') }}>
          <span className="text-[10px] font-medium text-white">{getInitials()}</span>
        </div>
      )}

      <div className="flex-1 min-w-0">
        {/* Header */}
        <div className="flex items-center gap-2 mb-1">
          <span className="text-[13px] font-medium text-gray-900">
            {comment.user?.name || comment.user?.email || 'Unknown'}
          </span>
          <span className="text-[11px] text-gray-400">{timeAgo}</span>
          {comment.is_edited && (
            <span className="text-[10px] text-gray-400 italic">(edited)</span>
          )}
        </div>

        {/* Content */}
        {isEditing ? (
          <div className="space-y-2">
            <textarea
              value={editText}
              onChange={(e) => setEditText(e.target.value)}
              className="w-full px-3 py-2 text-[13px] border border-gray-200 rounded-lg focus:outline-none focus:ring-1 focus:ring-gray-300 resize-none"
              rows={3}
              autoFocus
            />
            <div className="flex gap-2">
              <button
                onClick={saveEdit}
                className="px-3 py-1.5 text-[12px] font-medium bg-gray-900 text-white rounded-lg hover:bg-gray-800"
              >
                Save
              </button>
              <button
                onClick={cancelEdit}
                className="px-3 py-1.5 text-[12px] font-medium text-gray-500 hover:text-gray-700"
              >
                Cancel
              </button>
            </div>
          </div>
        ) : (
          <div className="text-[13px] text-gray-700 leading-relaxed">{renderContent()}</div>
        )}

        {/* Reactions */}
        {!isEditing && (
          <div className="flex items-center gap-1.5 mt-2 flex-wrap">
            {Object.entries(reactionGroups).map(([emoji, { count, userIds }]) => {
              const isOwn = currentUserId && userIds.includes(currentUserId);
              return (
                <button
                  key={emoji}
                  onClick={() => handleReactionClick(emoji)}
                  className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[12px] transition-colors ${
                    isOwn
                      ? 'bg-blue-100 text-blue-700 hover:bg-blue-200'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  <span>{emoji}</span>
                  <span className="font-medium">{count}</span>
                </button>
              );
            })}

            {/* Add reaction button */}
            <div className="relative">
              <button
                onClick={() => setShowEmojiPicker(!showEmojiPicker)}
                className="p-1 text-gray-300 hover:text-gray-500 hover:bg-gray-100 rounded transition-colors"
                title="Add reaction"
              >
                <svg
                  className="w-4 h-4"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M14.828 14.828a4 4 0 01-5.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
              </button>

              {showEmojiPicker && (
                <>
                  <div className="fixed inset-0 z-10" onClick={() => setShowEmojiPicker(false)} />
                  <div className="absolute left-0 top-full z-20 mt-1 bg-white rounded-lg shadow-lg border border-gray-200 p-2 flex gap-1">
                    {QUICK_EMOJIS.map((emoji) => (
                      <button
                        key={emoji}
                        onClick={() => handleReactionClick(emoji)}
                        className="w-8 h-8 flex items-center justify-center hover:bg-gray-100 rounded transition-colors text-base"
                      >
                        {emoji}
                      </button>
                    ))}
                  </div>
                </>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Actions menu */}
      {isOwn && !isEditing && (
        <div className="relative flex-shrink-0">
          <button
            onClick={() => setShowMenu(!showMenu)}
            className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
          >
            <EllipsisHorizontalIcon className="w-5 h-5" />
          </button>

          {showMenu && (
            <>
              <div className="fixed inset-0 z-10" onClick={() => setShowMenu(false)} />
              <div className="absolute right-0 top-full z-20 mt-1 w-32 bg-white rounded-lg shadow-lg border border-gray-200 py-1">
                <button
                  onClick={startEdit}
                  className="w-full flex items-center gap-2 px-3 py-2 text-[12px] text-gray-700 hover:bg-gray-50 transition-colors"
                >
                  <PencilIcon className="w-3.5 h-3.5" />
                  Edit
                </button>
                <button
                  onClick={() => {
                    onDelete(comment.id);
                    setShowMenu(false);
                  }}
                  className="w-full flex items-center gap-2 px-3 py-2 text-[12px] text-red-600 hover:bg-red-50 transition-colors"
                >
                  <TrashIcon className="w-3.5 h-3.5" />
                  Delete
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
