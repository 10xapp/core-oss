import { useState } from 'react';
import { useProjectsStore } from '../../../stores/projectsStore';
import { useCreateBoard } from '../../../hooks/queries/useProjects';
import Modal from '../../Modal/Modal';

interface CreateProjectModalProps {
  isOpen: boolean;
  onClose: () => void;
  onCreated?: (boardId: string) => void;
}

export default function CreateProjectModal({
  isOpen,
  onClose,
  onCreated,
}: CreateProjectModalProps) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');

  const workspaceAppId = useProjectsStore((state) => state.workspaceAppId);
  const createBoard = useCreateBoard(workspaceAppId);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!name.trim()) return;

    try {
      const result = await createBoard.mutateAsync({
        name: name.trim(),
        description: description.trim() || undefined,
      });
      if (result.board && onCreated) {
        onCreated(result.board.id);
      }
      setName('');
      setDescription('');
      onClose();
    } catch (error) {
      console.error('Failed to create project:', error);
    }
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} title="New Project">
      <form onSubmit={handleSubmit} className="space-y-5">
        {/* Name Input */}
        <div>
          <label className="block text-[12px] font-medium text-gray-500 mb-2">
            Name
          </label>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g., Q1 Roadmap"
            className="w-full px-3 py-2 text-[14px] text-gray-900 bg-transparent border border-gray-200 rounded-lg focus:outline-none focus:border-gray-300 placeholder:text-gray-400"
            autoFocus
          />
        </div>

        {/* Description Input */}
        <div>
          <label className="block text-[12px] font-medium text-gray-500 mb-2">
            Description <span className="text-gray-400 font-normal">(optional)</span>
          </label>
          <textarea
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="What is this project about?"
            rows={3}
            className="w-full px-3 py-2 text-[14px] text-gray-900 bg-transparent border border-gray-200 rounded-lg focus:outline-none focus:border-gray-300 placeholder:text-gray-400 resize-none"
          />
        </div>

        {/* Actions */}
        <div className="flex items-center justify-end gap-2 pt-4 border-t border-gray-100">
          <button
            type="button"
            onClick={onClose}
            className="px-3 py-2 text-[12px] font-medium text-gray-500 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors"
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={!name.trim() || createBoard.isPending}
            className="px-3 py-2 text-[12px] font-medium bg-gray-900 text-white rounded-lg hover:bg-gray-800 disabled:opacity-40 disabled:cursor-not-allowed transition-all"
          >
            {createBoard.isPending ? 'Creating...' : 'Create'}
          </button>
        </div>
      </form>
    </Modal>
  );
}
