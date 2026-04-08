import { memo, useEffect, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { Components } from 'react-markdown';
import { AnimatedMarkdown } from 'flowtoken';
import 'flowtoken/dist/styles.css';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnimateText = (children: any) => any;

interface StreamingTextProps {
  content: string;
  isStreaming: boolean;
  variant?: 'default' | 'compact';
}

// ---------------------------------------------------------------------------
// Build animated component wrappers for flowtoken
// ---------------------------------------------------------------------------
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const makeAnimatedComponents = (base: Record<string, any>): Record<string, any> => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const animated: Record<string, any> = {};
  for (const [tag, Component] of Object.entries(base)) {
    if (tag === 'code') {
      animated[tag] = ({ animateText, ...props }: { animateText: AnimateText } & Record<string, any>) => {
        return Component({ ...props, children: animateText(props.children) });
      };
    } else if (['ul', 'ol', 'blockquote'].includes(tag)) {
      animated[tag] = ({ animateText: _animateText, ...props }: { animateText: AnimateText } & Record<string, any>) => {
        return Component(props);
      };
    } else {
      animated[tag] = ({ animateText, ...props }: { animateText: AnimateText } & Record<string, any>) => {
        return Component({ ...props, children: animateText(props.children) });
      };
    }
  }
  return animated;
};

// ---------------------------------------------------------------------------
// Base markdown components (styling only)
// ---------------------------------------------------------------------------
const defaultMarkdownComponents: Components = {
  p({ children }) {
    return <p className="mb-4 last:mb-0 leading-7">{children}</p>;
  },
  ul({ children }) {
    return <ul className="mb-4 last:mb-0 pl-6 list-disc space-y-2">{children}</ul>;
  },
  ol({ children }) {
    return <ol className="mb-4 last:mb-0 pl-6 list-decimal space-y-2">{children}</ol>;
  },
  li({ children }) {
    return <li className="leading-7">{children}</li>;
  },
  h1({ children }) {
    return <h1 className="text-2xl font-semibold mb-4 mt-6 first:mt-0">{children}</h1>;
  },
  h2({ children }) {
    return <h2 className="text-xl font-semibold mb-3 mt-5 first:mt-0">{children}</h2>;
  },
  h3({ children }) {
    return <h3 className="text-lg font-semibold mb-2 mt-4 first:mt-0">{children}</h3>;
  },
  blockquote({ children }) {
    return (
      <blockquote className="border-l-4 border-border-gray pl-4 my-4 text-text-secondary italic">
        {children}
      </blockquote>
    );
  },
  a({ href, children }) {
    return (
      <a href={href} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
        {children}
      </a>
    );
  },
  code({ className, children, ...props }) {
    const match = /language-(\w+)/.exec(className || '');
    const codeString = String(children).replace(/\n$/, '');
    const isInline = !match && !codeString.includes('\n');
    if (isInline) {
      return (
        <code className="bg-border-light px-1.5 py-0.5 rounded text-sm font-mono text-text-body" {...props}>
          {children}
        </code>
      );
    }
    return (
      <pre className="bg-bg-code rounded-xl p-4 overflow-x-auto my-4">
        <code className="text-sm font-mono text-gray-100">{codeString}</code>
      </pre>
    );
  },
};

const compactMarkdownComponents: Components = {
  p({ children }) {
    return <p className="mb-2 last:mb-0 leading-relaxed">{children}</p>;
  },
  ul({ children }) {
    return <ul className="mb-2 last:mb-0 pl-4 list-disc space-y-1">{children}</ul>;
  },
  ol({ children }) {
    return <ol className="mb-2 last:mb-0 pl-4 list-decimal space-y-1">{children}</ol>;
  },
  li({ children }) {
    return <li className="leading-relaxed">{children}</li>;
  },
  h1({ children }) {
    return <h1 className="text-base font-semibold mb-2 mt-3 first:mt-0">{children}</h1>;
  },
  h2({ children }) {
    return <h2 className="text-sm font-semibold mb-2 mt-3 first:mt-0">{children}</h2>;
  },
  h3({ children }) {
    return <h3 className="text-sm font-semibold mb-1.5 mt-2 first:mt-0">{children}</h3>;
  },
  blockquote({ children }) {
    return (
      <blockquote className="border-l-2 border-border-gray pl-3 my-2 text-text-secondary italic text-sm">
        {children}
      </blockquote>
    );
  },
  a({ href, children }) {
    return (
      <a href={href} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
        {children}
      </a>
    );
  },
  code({ className, children, ...props }) {
    const match = /language-(\w+)/.exec(className || '');
    const codeString = String(children).replace(/\n$/, '');
    const isInline = !match && !codeString.includes('\n');
    if (isInline) {
      return (
        <code className="bg-border-light px-1 py-0.5 rounded text-xs font-mono text-text-body" {...props}>
          {children}
        </code>
      );
    }
    return (
      <pre className="bg-bg-code rounded-lg p-3 overflow-x-auto my-2">
        <code className="text-xs font-mono text-gray-100">{codeString}</code>
      </pre>
    );
  },
};

// Pre-build animated component variants
const defaultAnimated = makeAnimatedComponents(defaultMarkdownComponents);
const compactAnimated = makeAnimatedComponents(compactMarkdownComponents);

// ---------------------------------------------------------------------------
// StreamingText
// ---------------------------------------------------------------------------
function StreamingText({ content, isStreaming, variant = 'default' }: StreamingTextProps) {
  const isCompact = variant === 'compact';
  const animatedComponents = isCompact ? compactAnimated : defaultAnimated;
  const plainComponents = isCompact ? compactMarkdownComponents : defaultMarkdownComponents;

  // Track whether this message ever streamed (to avoid animating historical msgs)
  const hasStreamed = useRef(isStreaming);
  useEffect(() => {
    if (isStreaming) hasStreamed.current = true;
  }, [isStreaming]);

  const containerClass = isCompact
    ? "prose prose-sm prose-gray max-w-none text-text-body text-sm"
    : "prose prose-gray max-w-none text-text-body text-[16px]";

  // Historical message — plain ReactMarkdown, no animation, no spans
  if (!hasStreamed.current) {
    return (
      <div className={containerClass}>
        <ReactMarkdown remarkPlugins={[remarkGfm]} components={plainComponents}>
          {content}
        </ReactMarkdown>
      </div>
    );
  }

  // Streamed message — always use AnimatedMarkdown so spans stay stable (no format jump).
  // "streaming-text" class activates the CSS mask sweep on inner spans.
  return (
    <div className={`${containerClass} streaming-text`}>
      <AnimatedMarkdown
        content={content}
        sep="diff"
        animation="fadeIn"
        animationDuration="0.6s"
        animationTimingFunction="ease-in-out"
        customComponents={animatedComponents}
      />
    </div>
  );
}

export default memo(StreamingText);

export { defaultMarkdownComponents, compactMarkdownComponents };
