import { useState } from 'react';
import { PlusIcon, HashtagIcon, XMarkIcon, ChevronRightIcon, ChevronDownIcon } from '@heroicons/react/24/outline';
import { SIDEBAR } from '../../lib/sidebar';

interface Channel {
  id: string;
  name: string;
  type: 'public' | 'private';
  unreadCount: number;
}

interface DirectMessage {
  id: string;
  userId: string;
  userName: string;
  status: 'online' | 'away' | 'offline';
  unreadCount: number;
}

interface Message {
  id: string;
  author: string;
  content: string;
  timestamp: string;
  avatar?: string;
}

const defaultChannels: Channel[] = [
  { id: '1', name: 'general', type: 'public', unreadCount: 0 },
  { id: '2', name: 'announcements', type: 'public', unreadCount: 2 },
  { id: '3', name: 'random', type: 'public', unreadCount: 0 },
];

const defaultDirectMessages: DirectMessage[] = [
  { id: 'dm1', userId: 'user1', userName: 'John Doe', status: 'online', unreadCount: 1 },
  { id: 'dm2', userId: 'user2', userName: 'Jane Smith', status: 'away', unreadCount: 0 },
  { id: 'dm3', userId: 'user3', userName: 'Mike Johnson', status: 'offline', unreadCount: 0 },
];

export default function TeamView() {
  const [channels, setChannels] = useState<Channel[]>(defaultChannels);
  const [directMessages] = useState<DirectMessage[]>(defaultDirectMessages);
  const [selectedChannel, setSelectedChannel] = useState<string | null>(channels[0].id);
  const [selectedDM, setSelectedDM] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([
    {
      id: '1',
      author: 'John Doe',
      content: 'Hey everyone! Welcome to the general channel.',
      timestamp: '10:30 AM',
    },
    {
      id: '2',
      author: 'Jane Smith',
      content: 'Thanks for the welcome! Excited to be here.',
      timestamp: '10:35 AM',
    },
  ]);
  const [messageInput, setMessageInput] = useState('');
  const [searchQuery] = useState('');
  const [showChannelsExpanded, setShowChannelsExpanded] = useState(true);
  const [showDMsExpanded, setShowDMsExpanded] = useState(true);
  const [showCreateChannel, setShowCreateChannel] = useState(false);
  const [newChannelName, setNewChannelName] = useState('');
  const [newChannelType, setNewChannelType] = useState<'public' | 'private'>('public');

  const handleSendMessage = (e: React.FormEvent) => {
    e.preventDefault();
    if (!messageInput.trim()) return;

    const newMessage: Message = {
      id: `msg-${Date.now()}`,
      author: 'You',
      content: messageInput,
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    };

    setMessages([...messages, newMessage]);
    setMessageInput('');
  };

  const handleCreateChannel = (e: React.FormEvent) => {
    e.preventDefault();
    if (!newChannelName.trim()) return;

    const newChannel: Channel = {
      id: `ch-${Date.now()}`,
      name: newChannelName.toLowerCase(),
      type: newChannelType,
      unreadCount: 0,
    };

    setChannels([...channels, newChannel]);
    setNewChannelName('');
    setShowCreateChannel(false);
    setSelectedChannel(newChannel.id);
  };

  const currentChannel = selectedChannel
    ? channels.find(ch => ch.id === selectedChannel)
    : null;

  const currentDM = selectedDM
    ? directMessages.find(dm => dm.id === selectedDM)
    : null;

  const filteredChannels = channels.filter(ch =>
    ch.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const filteredDMs = directMessages.filter(dm =>
    dm.userName.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="flex-1 flex h-full overflow-hidden">
      {/* Main content container - light bg with rounded corners */}
      <div className="flex-1 flex overflow-hidden bg-bg-mini-app">
        {/* Sidebar */}
        <div className={`w-[212px] shrink-0 flex flex-col overflow-hidden ${SIDEBAR.bg} border-r border-black/5`}>
          {/* Header */}
          <div className="h-12 flex items-center px-4 shrink-0">
            <h2 className="text-base font-semibold text-text-body">Team</h2>
          </div>

          {/* Channels Section */}
          <div className="flex-1 overflow-y-auto">
            <div className="px-3">
            {/* Channels Header */}
            <div className="flex items-center justify-between mb-2">
              <button
                onClick={() => setShowChannelsExpanded(!showChannelsExpanded)}
                className="flex items-center gap-1 text-text-secondary hover:text-text-body transition-colors"
              >
                {showChannelsExpanded ? (
                  <ChevronDownIcon className="w-3.5 h-3.5" />
                ) : (
                  <ChevronRightIcon className="w-3.5 h-3.5" />
                )}
                <span className="text-xs font-semibold uppercase">Channels</span>
              </button>
              <button
                onClick={() => setShowCreateChannel(true)}
                className="p-1 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded transition-colors"
              >
                <PlusIcon className="w-3.5 h-3.5" />
              </button>
            </div>

            {/* Channels List */}
            {showChannelsExpanded && (
              <div className="space-y-1">
                {filteredChannels.map(channel => (
                  <button
                    key={channel.id}
                    onClick={() => {
                      setSelectedChannel(channel.id);
                      setSelectedDM(null);
                    }}
                    className={`w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors ${
                      selectedChannel === channel.id
                        ? SIDEBAR.selected
                        : `${SIDEBAR.item} hover:bg-black/5`
                    }`}
                  >
                    <HashtagIcon
                      className="w-3.5 h-3.5"
                    />
                    <span className="flex-1 text-left truncate">{channel.name}</span>
                    {channel.unreadCount > 0 && (
                      <span className="px-1.5 py-0.5 bg-brand-primary text-white text-xs rounded-full font-semibold">
                        {channel.unreadCount}
                      </span>
                    )}
                  </button>
                ))}
              </div>
            )}

            {/* Direct Messages Header */}
            <div className="flex items-center justify-between mb-2 mt-6">
              <button
                onClick={() => setShowDMsExpanded(!showDMsExpanded)}
                className="flex items-center gap-1 text-text-secondary hover:text-text-body transition-colors"
              >
                {showDMsExpanded ? (
                  <ChevronDownIcon className="w-3.5 h-3.5" />
                ) : (
                  <ChevronRightIcon className="w-3.5 h-3.5" />
                )}
                <span className="text-xs font-semibold uppercase">Direct Messages</span>
              </button>
            </div>

            {/* Direct Messages List */}
            {showDMsExpanded && (
              <div className="space-y-1">
                {filteredDMs.map(dm => (
                  <button
                    key={dm.id}
                    onClick={() => {
                      setSelectedDM(dm.id);
                      setSelectedChannel(null);
                    }}
                    className={`w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors ${
                      selectedDM === dm.id
                        ? SIDEBAR.selected
                        : `${SIDEBAR.item} hover:bg-black/5`
                    }`}
                  >
                    <div className="relative">
                      <div className="w-6 h-6 rounded-full flex items-center justify-center text-xs font-semibold text-white" style={{ background: `linear-gradient(135deg, #5A7864 0%, #607E98 100%)` }}>
                        {dm.userName.charAt(0)}
                      </div>
                      {/* Status indicator */}
                      <div
                        className={`absolute bottom-0 right-0 w-2 h-2 rounded-full border border-bg-gray-dark ${
                          dm.status === 'online'
                            ? 'bg-green-500'
                            : dm.status === 'away'
                            ? 'bg-yellow-500'
                            : 'bg-gray-400'
                        }`}
                      />
                    </div>
                    <span className="flex-1 text-left truncate">{dm.userName}</span>
                    {dm.unreadCount > 0 && (
                      <span className="px-1.5 py-0.5 bg-brand-primary text-white text-xs rounded-full font-semibold">
                        {dm.unreadCount}
                      </span>
                    )}
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>
        </div>

        {/* Main Content + Chat */}
      <div className="flex-1 flex min-w-0 overflow-hidden relative">
        <div className="flex-1 flex min-w-0 overflow-hidden bg-white rounded-lg">
        {selectedChannel || selectedDM ? (
          <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
          {/* Header */}
          <div className="h-12 border-b border-border-gray px-6 flex items-center justify-between bg-bg-white">
            <div className="flex items-center gap-2">
              {currentChannel && (
                <>
                  <HashtagIcon className="w-4.5 h-4.5 text-text-secondary" />
                  <h2 className="text-lg font-semibold text-text-body">
                    {currentChannel.name}
                  </h2>
                </>
              )}
              {currentDM && (
                <>
                  <div className="w-8 h-8 rounded-full flex items-center justify-center text-sm font-semibold text-white" style={{ background: `linear-gradient(135deg, #5A7864 0%, #607E98 100%)` }}>
                    {currentDM.userName.charAt(0)}
                  </div>
                  <div>
                    <h2 className="text-lg font-semibold text-text-body">
                      {currentDM.userName}
                    </h2>
                    <p className="text-xs text-text-tertiary">
                      {currentDM.status === 'online'
                        ? 'Active now'
                        : currentDM.status === 'away'
                        ? 'Away'
                        : 'Offline'}
                    </p>
                  </div>
                </>
              )}
            </div>
          </div>

          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-6 space-y-4">
            {messages.map(message => (
              <div key={message.id} className="flex gap-3">
                <div className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold text-white flex-shrink-0" style={{ background: `linear-gradient(135deg, #5A7864 0%, #607E98 100%)` }}>
                  {message.author.charAt(0)}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-baseline gap-2 mb-1">
                    <p className="font-semibold text-text-body">{message.author}</p>
                    <p className="text-xs text-text-tertiary">{message.timestamp}</p>
                  </div>
                  <p className="text-text-body break-words">{message.content}</p>
                </div>
              </div>
            ))}
          </div>

          {/* Input */}
          <div className="border-t border-border-gray p-4 bg-bg-white">
            <form onSubmit={handleSendMessage} className="flex gap-2">
              <input
                type="text"
                value={messageInput}
                onChange={(e) => setMessageInput(e.target.value)}
                placeholder={
                  currentChannel
                    ? `Message #${currentChannel.name}`
                    : `Message @${currentDM?.userName}`
                }
                className="flex-1 px-4 py-2 bg-bg-gray rounded-lg text-text-body placeholder:text-text-tertiary focus:outline-none focus:ring-1 focus:ring-brand-primary"
              />
              <button
                type="submit"
                disabled={!messageInput.trim()}
                className="px-4 py-2 bg-brand-primary text-white rounded-lg font-medium hover:bg-brand-primary/90 disabled:opacity-50 transition-colors"
              >
                Send
              </button>
            </form>
          </div>
        </div>
      ) : (
        <div className="flex-1 flex items-center justify-center text-text-tertiary">
          <p>Select a channel or direct message to start chatting</p>
        </div>
      )}
        </div>
      </div>

      {/* Create Channel Modal */}
      {showCreateChannel && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-bg-gray-dark border border-border-gray rounded-xl w-full max-w-md mx-4">
            <div className="flex items-center justify-between p-4 border-b border-border-gray">
              <h2 className="text-lg font-semibold text-text-body">Create Channel</h2>
              <button
                onClick={() => setShowCreateChannel(false)}
                className="p-1 text-text-secondary hover:text-text-body"
              >
                <XMarkIcon className="w-5 h-5" />
              </button>
            </div>

            <form onSubmit={handleCreateChannel} className="p-4 space-y-4">
              <div>
                <label className="block text-sm text-text-secondary mb-1">
                  Channel Name
                </label>
                <input
                  type="text"
                  value={newChannelName}
                  onChange={(e) => setNewChannelName(e.target.value)}
                  className="w-full px-3 py-2 bg-bg-gray-dark/50 border border-border-gray rounded-lg text-sm text-text-body placeholder:text-text-tertiary focus:outline-none focus:border-brand-primary"
                  placeholder="e.g. project-updates"
                  required
                />
              </div>

              <div>
                <label className="block text-sm text-text-secondary mb-2">
                  Channel Type
                </label>
                <div className="flex gap-3">
                  <label className="flex items-center gap-2">
                    <input
                      type="radio"
                      value="public"
                      checked={newChannelType === 'public'}
                      onChange={(e) => setNewChannelType(e.target.value as 'public' | 'private')}
                      className="rounded border-border-gray"
                    />
                    <span className="text-sm text-text-body">Public</span>
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="radio"
                      value="private"
                      checked={newChannelType === 'private'}
                      onChange={(e) => setNewChannelType(e.target.value as 'public' | 'private')}
                      className="rounded border-border-gray"
                    />
                    <span className="text-sm text-text-body">Private</span>
                  </label>
                </div>
              </div>

              <div className="flex justify-end gap-2 pt-2">
                <button
                  type="button"
                  onClick={() => setShowCreateChannel(false)}
                  className="px-4 py-2 text-sm text-text-secondary hover:text-text-body"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={!newChannelName.trim()}
                  className="px-4 py-2 bg-brand-primary text-white rounded-lg text-sm font-medium hover:bg-brand-primary/90 disabled:opacity-50 transition-colors"
                >
                  Create Channel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
      </div>
    </div>
  );
}
