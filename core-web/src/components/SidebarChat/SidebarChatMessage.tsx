import { useState, memo } from 'react';
import { DocumentDuplicateIcon, CheckIcon } from '@heroicons/react/24/outline';
import StreamingText from '../Chat/StreamingText';

interface ChatMessageProps {
  role: 'user' | 'assistant';
  content: string;
  isStreaming?: boolean;
}

// User message - right aligned bubble (compact)
function UserMessage({ content }: { content: string }) {
  const isShort = content.length <= 60 && !content.includes('\n');
  return (
    <div className="py-1.5 px-3">
      <div className="flex justify-end">
        <div className={`bg-[#F7F8FA] px-4 py-2 max-w-[85%] ${isShort ? 'rounded-full' : 'rounded-2xl'}`}>
          <p className="text-text-body whitespace-pre-wrap text-sm leading-relaxed">
            {content}
          </p>
        </div>
      </div>
    </div>
  );
}

// Assistant message - left aligned (compact)
function AssistantMessage({ content, isStreaming }: { content: string; isStreaming?: boolean }) {
  const [copied, setCopied] = useState(false);

  const handleCopyMessage = async () => {
    await navigator.clipboard.writeText(content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="group py-1.5 px-4">
      <div className="max-w-[95%]">
        {/* Message content - use StreamingText for animation */}
        <StreamingText
          content={content}
          isStreaming={isStreaming ?? false}
          variant="compact"
        />

        {/* Copy button - only show on hover when not streaming */}
        {!isStreaming && (
          <div className="opacity-0 group-hover:opacity-100 transition-opacity mt-1">
            <button
              onClick={handleCopyMessage}
              className="p-1 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded transition-colors"
              title="Copy"
            >
              {copied ? (
                <CheckIcon className="w-3.5 h-3.5 stroke-2" />
              ) : (
                <DocumentDuplicateIcon className="w-3.5 h-3.5" />
              )}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

function SidebarChatMessage({ role, content, isStreaming }: ChatMessageProps) {
  if (role === 'user') {
    return <UserMessage content={content} />;
  }
  return <AssistantMessage content={content} isStreaming={isStreaming} />;
}

export default memo(SidebarChatMessage);
