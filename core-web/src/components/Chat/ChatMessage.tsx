import { useState, memo } from 'react';
import { DocumentDuplicateIcon, CheckIcon, HandThumbUpIcon, HandThumbDownIcon, ArrowPathIcon } from '@heroicons/react/24/outline';
import type { ContentPart } from '../../api/client';
import ContentPartsRenderer, { CodeBlock } from './ContentPartsRenderer';
import ChatAttachmentImage from './ChatAttachmentImage';
import StreamingText from './StreamingText';

// Re-export CodeBlock so it's still available if anyone imported it from here
export { CodeBlock };

interface ChatMessageProps {
  role: 'user' | 'assistant';
  content: string;
  contentParts?: ContentPart[];
  messageId?: string;
  isStreaming?: boolean;
  onRegenerate?: (messageId: string) => void;
}

// User message - right aligned bubble
function UserMessage({ content, contentParts }: { content: string; contentParts?: ContentPart[] }) {
  // Check for attachment parts to show image thumbnails above text
  const attachmentParts = contentParts?.filter(p => p.type === 'attachment') || [];
  const isMultiline = content.includes('\n');

  return (
    <div className="py-3">
      <div className="max-w-3xl mx-auto px-4">
        <div className="flex flex-col items-end gap-2">
          {/* Attachment thumbnails */}
          {attachmentParts.length > 0 && (
            <div className="flex gap-2 flex-wrap justify-end max-w-[70%]">
              {attachmentParts.map((part) => {
                // Use local preview URL if available, otherwise show from server
                const previewUrl = part.data.preview_url as string | undefined;
                const attachmentId = part.data.attachment_id as string | undefined;
                const width = part.data.width as number | undefined;
                const height = part.data.height as number | undefined;

                if (previewUrl) {
                  return (
                    <img
                      key={part.id}
                      src={previewUrl}
                      alt="Attachment"
                      className="rounded-xl max-w-[200px] max-h-[200px] object-cover"
                    />
                  );
                }

                if (attachmentId) {
                  return <ChatAttachmentImage key={part.id} attachmentId={attachmentId} width={width} height={height} />;
                }

                return null;
              })}
            </div>
          )}

          {/* Bubble */}
          {content && (
            <div
              className={`bg-[#F4F4F4] rounded-[18px] px-4 max-w-[70%] ${isMultiline ? 'py-3' : 'py-1.5'}`}
            >
              <div className="whitespace-pre-wrap text-text-body text-[16px] leading-relaxed">
                {content}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// Assistant message - left aligned, no bubble
function AssistantMessage({ content, contentParts, messageId, isStreaming, onRegenerate }: { content: string; contentParts?: ContentPart[]; messageId?: string; isStreaming?: boolean; onRegenerate?: (messageId: string) => void }) {
  const [copied, setCopied] = useState(false);

  const handleCopyMessage = async () => {
    await navigator.clipboard.writeText(content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const hasContentParts = contentParts && contentParts.length > 0;

  return (
    <div className="group py-4">
      <div className="max-w-3xl mx-auto px-4">
        <div>
          {/* Content */}
          <div className="max-w-[85%]">
            {hasContentParts ? (
              <ContentPartsRenderer parts={contentParts} messageId={messageId} isStreaming={isStreaming} />
            ) : (
              <StreamingText content={content} isStreaming={isStreaming ?? false} />
            )}

            {/* Message actions */}
            {!isStreaming && content && (
              <div className="flex items-center gap-1 mt-2">
                <button
                  onClick={handleCopyMessage}
                  className="p-1.5 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded transition-colors"
                  title="Copy"
                >
                  {copied ? (
                    <CheckIcon className="w-4 h-4 stroke-2" />
                  ) : (
                    <DocumentDuplicateIcon className="w-4 h-4" />
                  )}
                </button>
                {onRegenerate && messageId && (
                  <button
                    onClick={() => onRegenerate(messageId)}
                    className="p-1.5 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded transition-colors"
                    title="Regenerate response"
                  >
                    <ArrowPathIcon className="w-4 h-4" />
                  </button>
                )}
                <button
                  className="p-1.5 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded transition-colors"
                  title="Good response"
                >
                  <HandThumbUpIcon className="w-4 h-4" />
                </button>
                <button
                  className="p-1.5 text-text-tertiary hover:text-text-body hover:bg-bg-gray rounded transition-colors"
                  title="Bad response"
                >
                  <HandThumbDownIcon className="w-4 h-4" />
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function ChatMessage({ role, content, contentParts, messageId, isStreaming, onRegenerate }: ChatMessageProps) {
  if (role === 'user') {
    return <UserMessage content={content} contentParts={contentParts} />;
  }
  return <AssistantMessage content={content} contentParts={contentParts} messageId={messageId} isStreaming={isStreaming} onRegenerate={onRegenerate} />;
}

export default memo(ChatMessage);
