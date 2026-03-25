import DOMPurify, { type Config } from 'dompurify';

const BASE_CONFIG: Config = {
  USE_PROFILES: { html: true },
  ALLOW_DATA_ATTR: false,
  // TODO(security): Add RETURN_TRUSTED_TYPE when we adopt Trusted Types.
};

const EMAIL_AND_DOCUMENT_URI_REGEXP = /^(?:(?:https?|mailto|tel|cid|data|blob):|[^a-z]|[a-z+.-]+(?:[^a-z+.-:]|$))/i;
const STRICT_URI_REGEXP = /^(?:(?:https?|mailto|tel):|[^a-z]|[a-z+.-]+(?:[^a-z+.-:]|$))/i;

const RICH_DOCUMENT_CONFIG: Config = {
  ...BASE_CONFIG,
  ADD_TAGS: ['style'],
  ADD_ATTR: ['style', 'target'],
  FORBID_TAGS: ['script', 'noscript', 'iframe', 'object', 'embed', 'form', 'input', 'button', 'textarea', 'select', 'meta', 'base'],
  ALLOWED_URI_REGEXP: EMAIL_AND_DOCUMENT_URI_REGEXP,
};

const STRICT_CONFIG: Config = {
  ...BASE_CONFIG,
  ALLOWED_TAGS: [
    'a',
    'b',
    'blockquote',
    'br',
    'code',
    'em',
    'i',
    'li',
    'ol',
    'p',
    'pre',
    's',
    'span',
    'strong',
    'u',
    'ul',
  ],
  ALLOWED_ATTR: ['href', 'target', 'rel'],
  FORBID_TAGS: [
    'style',
    'img',
    'picture',
    'source',
    'video',
    'audio',
    'iframe',
    'object',
    'embed',
    'form',
    'input',
    'button',
    'textarea',
    'select',
    'svg',
    'math',
    'meta',
    'base',
  ],
  ALLOWED_URI_REGEXP: STRICT_URI_REGEXP,
};

function sanitize(html: string, config: Config): string {
  if (!html) return '';
  return DOMPurify.sanitize(html, config);
}

/** Sanitize HTML and force all links to open in a new tab. */
function sanitizeWithExternalLinks(html: string, config: Config): string {
  if (!html) return '';
  DOMPurify.addHook('afterSanitizeAttributes', (node) => {
    if (node.tagName === 'A') {
      node.setAttribute('target', '_blank');
      node.setAttribute('rel', 'noopener noreferrer');
    }
  });
  const result = DOMPurify.sanitize(html, config);
  DOMPurify.removeHook('afterSanitizeAttributes');
  return result;
}

export function sanitizeEmailHtml(html: string): string {
  return sanitize(html, RICH_DOCUMENT_CONFIG);
}

export function sanitizeStrictHtml(html: string): string {
  return sanitizeWithExternalLinks(html, STRICT_CONFIG);
}

export function sanitizeRichDocumentHtml(html: string): string {
  return sanitize(html, RICH_DOCUMENT_CONFIG);
}

export function sanitizeRichDocumentElementInPlace(element: Element): void {
  DOMPurify.sanitize(element, {
    ...RICH_DOCUMENT_CONFIG,
    IN_PLACE: true,
  } as Config & { IN_PLACE: true });
}
