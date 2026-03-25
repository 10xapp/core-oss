# Content Parts Schema

## Overview

The Content Parts Schema is a structured approach to message content that enables proper interleaving of text, tool outputs, and actions during streaming. Instead of storing raw text with inline markers, messages are stored as an ordered array of typed content parts.

**Key Benefits:**
- Identical rendering during streaming and after DB reload
- No regex parsing on iOS - backend does all parsing
- Proper interleaving of text and tool outputs (emails, calendar, todos)
- Easy extensibility for new tool types
- Full-text search preserved via `content` column

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         STREAMING FLOW                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Backend (Python)                    iOS (Swift)                     │
│  ┌──────────────────┐               ┌──────────────────┐            │
│  │  ContentBuilder  │   NDJSON      │ IOSContentBuilder │            │
│  │                  │ ──────────►   │                  │            │
│  │  - appendText()  │   events      │  - appendText()  │            │
│  │  - addDisplay()  │               │  - addDisplay()  │            │
│  │  - addAction()   │               │  - addAction()   │            │
│  │  - finalize()    │               │  - finalize()    │            │
│  └──────────────────┘               └──────────────────┘            │
│         │                                    │                       │
│         ▼                                    ▼                       │
│  ┌──────────────────┐               ┌──────────────────┐            │
│  │   content_parts  │               │   contentParts   │            │
│  │   (JSONB)        │               │   [ContentPart]  │            │
│  └──────────────────┘               └──────────────────┘            │
│         │                                    │                       │
│         │                                    ▼                       │
│         │                           ┌──────────────────┐            │
│         └──────────────────────────►│ContentPartsRenderer│           │
│              DB reload              │   (SwiftUI)      │            │
│                                     └──────────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
```

## The Flush Pattern

The key to proper interleaving is the **flush pattern**. When a non-text event arrives (display, action), any accumulated text is first flushed to a text part before the new part is added.

```python
# Backend: api/services/chat/content_builder.py
class ContentBuilder:
    def add_display(self, display_type, items, total_count):
        self.flush_text()  # CRITICAL: Flush accumulated text first!
        self.content_parts.append(create_display_part(...))
```

```swift
// iOS: Core/Features/Chat/IOSContentBuilder.swift
class IOSContentBuilder {
    func addDisplay(_ content: DisplayContent) {
        flushText()  // CRITICAL: Flush accumulated text first!
        contentParts.append(...)
    }
}
```

### Example Flow

User asks: "Show me my emails and todos for today"

```
Stream Event Timeline:
──────────────────────────────────────────────────────────────────────

1. content("Let me check your emails.\n\n")
   → builder.appendText("Let me check your emails.\n\n")

2. display(emails)
   → builder.flushText()  ← Creates text part
   → builder.addDisplay(emails)

3. content("You have 5 emails.\n\nNow checking todos...\n\n")
   → builder.appendText("You have 5 emails...")

4. display(todos)
   → builder.flushText()  ← Creates text part
   → builder.addDisplay(todos)

5. content("Here are your 3 todos.")
   → builder.appendText("Here are your 3 todos.")

6. done
   → builder.finalize()  ← Flushes remaining text, adds sources

──────────────────────────────────────────────────────────────────────

Resulting content_parts array:
[
  {"type": "text", "data": {"content": "Let me check your emails.\n\n"}},
  {"type": "display", "data": {"display_type": "emails", ...}},
  {"type": "text", "data": {"content": "You have 5 emails.\n\nNow checking todos...\n\n"}},
  {"type": "display", "data": {"display_type": "todos", ...}},
  {"type": "text", "data": {"content": "Here are your 3 todos."}}
]
```

## Content Part Types

### Text Part
Plain or markdown text content.

```json
{
  "id": "uuid",
  "type": "text",
  "data": {
    "content": "Markdown text here..."
  }
}
```

### Source Reference Part
Reference to a web search source (1-based index).

```json
{
  "id": "uuid",
  "type": "source_ref",
  "data": {
    "source_index": 1
  }
}
```

### Display Part
Embedded UI cards (calendar events, emails, todos).

```json
{
  "id": "uuid",
  "type": "display",
  "data": {
    "display_type": "calendar_events" | "emails" | "todos",
    "items": [...],
    "total_count": 5
  }
}
```

### Action Part
Staged operations requiring user confirmation.

```json
{
  "id": "uuid",
  "type": "action",
  "data": {
    "action": "create_calendar_event" | "send_email" | "create_todo",
    "status": "staged" | "confirmed" | "executed",
    "data": { /* action-specific payload */ },
    "description": "Human-readable description"
  }
}
```

### Sources Part
Footer with web search sources for citations.

```json
{
  "id": "uuid",
  "type": "sources",
  "data": {
    "sources": [
      {
        "url": "https://example.com",
        "title": "Example Article",
        "domain": "example.com",
        "favicon": "https://example.com/favicon.ico"
      }
    ]
  }
}
```

## Database Schema

```sql
-- messages table
ALTER TABLE public.messages ADD COLUMN content_parts JSONB;

-- GIN index for efficient queries
CREATE INDEX idx_messages_content_parts ON public.messages USING GIN (content_parts);

-- The 'content' column is kept for:
-- 1. Full-text search
-- 2. Fallback for old messages without content_parts
-- 3. Quick preview without parsing
```

## Adding New Tool Types

### Step 1: Define the Display Type

In `api/services/chat/content_builder.py`, no changes needed - the display part schema is generic.

### Step 2: Emit Display Event from Agent

In your tool handler (e.g., `api/services/chat/tools/workouts.py`):

```python
from api.services.chat.events import display_event

async def search_workouts(user_id: str, ...):
    workouts = await fetch_workouts(...)

    # Emit display event
    yield display_event(
        display_type="workouts",
        items=[w.to_dict() for w in workouts[:3]],
        total_count=len(workouts)
    )
```

### Step 3: Add iOS Renderer Case

In `Core/Features/Chat/ContentPartsRenderer.swift`:

```swift
case .display(let data):
    if let displayContent = data.data.toDisplayContent() {
        EmbeddedDisplayContainer(...)
    }
```

In `Core/ChatModels.swift`, update `DisplayPartDataPayload.toDisplayContent()`:

```swift
func toDisplayContent() -> DisplayContent? {
    switch displayType {
    case "calendar_events":
        // existing...
    case "emails":
        // existing...
    case "todos":
        // existing...
    case "workouts":  // NEW
        let workouts = items.map { EmbeddedWorkout(from: $0) }
        return .workouts(workouts, totalCount: totalCount)
    default:
        return nil
    }
}
```

### Step 4: Add Display Content Enum Case

In `Core/ChatModels.swift`:

```swift
enum DisplayContent {
    case calendarEvents([EmbeddedCalendarEvent], totalCount: Int)
    case emails([EmbeddedEmail], totalCount: Int)
    case todos([EmbeddedTodo], totalCount: Int)
    case workouts([EmbeddedWorkout], totalCount: Int)  // NEW
}
```

### Step 5: Add Embedded Model

```swift
struct EmbeddedWorkout: Identifiable {
    let id: String
    let type: String
    let duration: Int
    let calories: Int
    // ...

    init(from dict: [String: AnyCodableValue]) {
        self.id = dict["id"]?.stringValue ?? UUID().uuidString
        // ...
    }

    func toDictionary() -> [String: AnyCodableValue] {
        // For IOSContentBuilder
    }
}
```

### Step 6: Add Card View

In `Core/Features/Chat/EmbeddedCards.swift`:

```swift
struct EmbeddedWorkoutCard: View {
    let workout: EmbeddedWorkout
    // ...
}
```

Update `EmbeddedDisplayContainer` to handle the new case.

## File Structure

```
core-api-block-schema/
├── api/
│   ├── routers/
│   │   └── chat.py                    # Uses ContentBuilder for streaming
│   └── services/
│       └── chat/
│           ├── content_builder.py     # ContentBuilder class + part helpers
│           └── events.py              # NDJSON event emitters
├── supabase/
│   └── migrations/
│       └── 20260104000000_rename_blocks_to_content_parts.sql
└── docs/
    └── CONTENT_PARTS_SCHEMA.md        # This file

core-ios-block-schema/
└── Core/
    ├── ChatModels.swift               # ContentPart enum, data types
    ├── ContentView.swift              # Uses IOSContentBuilder for streaming
    └── Features/
        └── Chat/
            ├── IOSContentBuilder.swift      # Mirrors backend's ContentBuilder
            ├── ContentPartsRenderer.swift   # Renders content_parts array
            ├── EmbeddedCards.swift          # Card views for display parts
            └── CitedTextView.swift          # Legacy citation rendering
```

## Migration Notes

### From Legacy Format

Old messages without `content_parts` are handled gracefully:

```swift
// iOS: ContentView.swift
if message.hasContentParts, let contentParts = message.contentParts {
    ContentPartsRenderer(contentParts: contentParts, ...)
} else {
    // Legacy text-based rendering
    CitedTextView(text: message.text, sources: message.sources, ...)
}
```

### Dual-Write Strategy

Backend writes both `content` (plain text) and `content_parts` (structured):

```python
# chat.py
message_data = {
    "content": text_content,        # For search/fallback
    "content_parts": content_parts  # Source of truth
}
```

## Testing Checklist

- [ ] Single tool call (emails only)
- [ ] Multiple tool calls (emails + todos + calendar)
- [ ] Tool call with no surrounding text
- [ ] Tool calls with text between each
- [ ] Citations in text with web search
- [ ] Action + display in same message
- [ ] Stream interruption (verify partial content preserved)
- [ ] DB reload renders identical to streaming
- [ ] Legacy messages without content_parts still render

## Troubleshooting

### Display cards appear in wrong order

Ensure `flushText()` is called before `addDisplay()` in both:
- Backend: `content_builder.py`
- iOS: `IOSContentBuilder.swift`

### Citations not rendering as links

Check that:
1. Backend parses `[N]` into `source_ref` parts
2. Sources are passed to `ContentPartsRenderer`
3. `InlinePartsView` maps source indices correctly (1-based)

### New display type not rendering

1. Add case to `DisplayPartDataPayload.toDisplayContent()`
2. Add enum case to `DisplayContent`
3. Add card view to `EmbeddedCards.swift`
4. Update `EmbeddedDisplayContainer` switch statement

## Action Persistence

Action parts (todos, calendar events, emails) support persistent execution state. When a user executes an action, the status changes from `"staged"` to `"executed"` and is saved to Supabase.

### Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    ACTION EXECUTION FLOW                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. User taps action card                                                │
│     └─► ActionExecutor.execute() runs the action                         │
│                                                                          │
│  2. Action succeeds                                                      │
│     └─► UI shows checkmark (local state: completedActions.insert)        │
│                                                                          │
│  3. Persist to server                                                    │
│     └─► PATCH /api/chat/messages/{message_id}/actions/{action_id}/execute│
│         └─► Updates content_parts JSONB: status "staged" → "executed"    │
│                                                                          │
│  4. Update local state                                                   │
│     └─► updateLocalActionStatus() syncs messages array                   │
│                                                                          │
│  5. User navigates away and returns                                      │
│     └─► Messages reload from Supabase                                    │
│     └─► content_parts includes status: "executed"                        │
│     └─► ActionCardContainer.onAppear reads isExecuted, shows checkmark   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### API Endpoint

```
PATCH /api/chat/messages/{message_id}/actions/{action_id}/execute
Authorization: Bearer <access_token>

Response: {"success": true, "action_id": "...", "status": "executed"}
```

The endpoint:
1. Validates `message_id` is a UUID
2. Verifies message exists and user owns the conversation
3. Finds the action part by `action_id` in `content_parts`
4. Updates `data.status` from `"staged"` to `"executed"`
5. Saves the updated `content_parts` JSONB

### Action ID Consistency

Action IDs must match between streaming and database storage:

```python
# Backend: Generate ID upfront in agent.py
action_id = str(uuid.uuid4()).upper()

# Include in streaming event
yield action_event(action_id=action_id, action="create_todo", ...)

# Include in content_parts via ContentBuilder
builder.add_action("create_todo", data, description, action_id=action_id)
```

```swift
// iOS: Parse server-provided ID from streaming event
struct ActionEvent: Codable {
    let partId: String  // Parsed from "id" in streaming JSON
    // ...
}

// Use same ID when calling persistence API
chatService.markActionExecuted(messageId: messageId, actionId: event.partId)
```

### Race Condition Handling

Actions may auto-execute during streaming before the `done` event provides the `message_id`. iOS handles this with retry logic:

```swift
// ContentView.swift: markActionExecuted()
var messageId = event.messageId
var retryCount = 0
let maxRetries = 10  // 5 seconds max wait

while messageId.isEmpty && retryCount < maxRetries {
    // Search messages for the action by partId to get serverId
    if let msg = messages.first(where: { /* find by partId */ }) {
        messageId = msg.serverId
        if !messageId.isEmpty { break }
    }
    retryCount += 1
    try? await Task.sleep(nanoseconds: 500_000_000)  // 500ms
}
```

### Email Actions

Email actions work differently - the email is sent via `EmailService` in the modal before `ActionExecutor.execute()` is called. The executor just returns `true` to acknowledge success:

```swift
// ActionExecutor.swift
case "send_email":
    // Email already sent by modal's EmailService.sendEmail()
    return true
```

The `EmailComposeModal.onSend` callback triggers persistence:

```swift
// ContentView.swift
onSend: { event in
    let success = await executeAction(event)
    if success {
        Task { await markActionExecuted(event) }
    }
    return success
}
```

### UI States

| Action Type | Staged | Executed |
|-------------|--------|----------|
| Todo | Plus icon, tappable | Green checkmark, disabled |
| Calendar | Plus icon, tappable | Green checkmark, disabled |
| Email | "DRAFT" label | "SENT" label (green) |

The UI checks both local state (`completedActions` set) and server state (`event.isExecuted`):

```swift
// EmailActionCard.swift
private var showCompleted: Bool { isCompleted || event.isExecuted }
```
