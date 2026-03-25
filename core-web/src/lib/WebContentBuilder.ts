/**
 * WebContentBuilder - Mirrors the backend's ContentBuilder for accumulating
 * stream events into ContentPart arrays during streaming.
 *
 * The key pattern: when a non-text event arrives (display, action, sources),
 * any accumulated text is first flushed to a text part before the new part is added.
 */
import type { ContentPart, StreamEvent, Source } from '../api/client';

let partCounter = 0;
function nextPartId(): string {
  return `part-${++partCounter}-${Date.now()}`;
}

/**
 * Parse text with [N] source citations, {EN} email references, and {CN} calendar
 * references into content parts. Mirrors backend parse_text_to_parts() logic.
 */
const ALL_REF_PATTERN = /(\[(\d+)\]|\{E(\d+)\}|\{C(\d+)\})/g;

function parseTextToParts(text: string): ContentPart[] {
  if (!text) return [];

  const parts: ContentPart[] = [];
  let lastEnd = 0;

  for (const match of text.matchAll(ALL_REF_PATTERN)) {
    // Text before the reference
    if (match.index! > lastEnd) {
      const before = text.slice(lastEnd, match.index!);
      if (before) {
        parts.push({ id: nextPartId(), type: 'text', data: { content: before } });
      }
    }

    if (match[2] !== undefined) {
      // Source citation: [N]
      parts.push({ id: nextPartId(), type: 'source_ref', data: { source_index: parseInt(match[2]) } });
    } else if (match[3] !== undefined) {
      // Email reference: {EN}
      parts.push({ id: nextPartId(), type: 'email_ref', data: { email_index: parseInt(match[3]) } });
    } else if (match[4] !== undefined) {
      // Calendar reference: {CN}
      parts.push({ id: nextPartId(), type: 'cal_ref', data: { cal_index: parseInt(match[4]) } });
    }

    lastEnd = match.index! + match[0].length;
  }

  // Remaining text
  if (lastEnd < text.length) {
    parts.push({ id: nextPartId(), type: 'text', data: { content: text.slice(lastEnd) } });
  }

  // No refs found — return single text part
  if (parts.length === 0 && text) {
    parts.push({ id: nextPartId(), type: 'text', data: { content: text } });
  }

  return parts;
}

export class WebContentBuilder {
  private parts: ContentPart[] = [];
  private textBuffer = '';

  /** Append a text delta (from 'content' stream events). */
  appendText(delta: string): void {
    this.textBuffer += delta;
  }

  /** Flush any buffered text into content parts (parses [N] and {EN} references). */
  flushText(): void {
    if (this.textBuffer) {
      this.parts.push(...parseTextToParts(this.textBuffer));
      this.textBuffer = '';
    }
  }

  /** Add a display part from a 'display' stream event. */
  addDisplay(event: StreamEvent): void {
    this.flushText();
    this.parts.push({
      id: event.id || nextPartId(),
      type: 'display',
      data: {
        display_type: event.display_type,
        items: event.items || [],
        total_count: event.total_count ?? (event.items?.length || 0),
      },
    });
  }

  /** Add an action part from an 'action' stream event. */
  addAction(event: StreamEvent): void {
    this.flushText();
    this.parts.push({
      id: event.id || nextPartId(),
      type: 'action',
      data: {
        action: event.action,
        status: event.status || 'staged',
        data: event.data || {},
        description: event.description || '',
      },
    });
  }

  /** Add a sources part from a 'sources' stream event. */
  addSources(sources: Source[]): void {
    this.flushText();
    this.parts.push({
      id: nextPartId(),
      type: 'sources',
      data: { sources },
    });
  }

  /** Add a tool_call part from a 'tool_call' stream event. */
  addToolCallStart(name: string, args?: Record<string, unknown>): void {
    this.flushText();
    this.parts.push({
      id: `tool-${name}-${Date.now()}`,
      type: 'tool_call',
      data: { name, args: args || {}, phase: 'running' },
    });
  }

  /** Update an existing tool_call part to mark it as done. */
  updateToolCallEnd(name: string, durationMs?: number, status?: string): void {
    // Find the last running tool_call with this name and update it
    for (let i = this.parts.length - 1; i >= 0; i--) {
      const part = this.parts[i];
      if (part.type === 'tool_call' && part.data.name === name && part.data.phase === 'running') {
        part.data.phase = 'done';
        part.data.duration_ms = durationMs;
        part.data.status = status || 'success';
        break;
      }
    }
  }

  /** Add a nutrition display part from a 'nutrition' stream event. */
  addNutrition(event: StreamEvent): void {
    this.flushText();
    this.parts.push({
      id: event.id || nextPartId(),
      type: 'display',
      data: {
        display_type: 'nutrition',
        items: event.nutrition_items || [{
          food_name: event.food_name,
          calories: event.calories,
          protein_g: event.protein_g,
          carbs_g: event.carbs_g,
          fat_g: event.fat_g,
          fiber_g: event.fiber_g,
          serving_size: event.serving_size,
        }],
        total_count: event.nutrition_items?.length || 1,
      },
    });
  }

  /**
   * Get a snapshot of current parts + any buffered text (for live rendering).
   * Does NOT consume the buffer — safe to call repeatedly during streaming.
   * Parses [N] and {EN} references in the buffer for inline rendering.
   */
  getSnapshot(): ContentPart[] {
    if (!this.textBuffer) {
      return [...this.parts];
    }
    // Parse references in the streaming buffer so they render inline during streaming
    const bufferParts = parseTextToParts(this.textBuffer);
    // Mark the last text part with the streaming ID for animation continuity
    for (let i = bufferParts.length - 1; i >= 0; i--) {
      if (bufferParts[i].type === 'text') {
        bufferParts[i].id = 'streaming-text';
        break;
      }
    }
    return [...this.parts, ...bufferParts];
  }

  /** Flush remaining text and return the final parts array. */
  finalize(): ContentPart[] {
    this.flushText();
    return [...this.parts];
  }

  /** Get the full accumulated plain text (for the content field fallback). */
  getFullText(): string {
    let text = '';
    for (const part of this.parts) {
      if (part.type === 'text') {
        text += (part.data.content as string) || '';
      }
    }
    text += this.textBuffer;
    return text;
  }
}
