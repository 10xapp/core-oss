"""
Entry creation orchestrator for app drawer.
Classifies input and creates the appropriate entry type.
"""
import logging
from typing import Dict, Any
from fastapi import HTTPException

from .classify_intent import classify_intent
from api.services.calendar.create_event import create_event
from api.services.email.send_email import send_email

logger = logging.getLogger(__name__)


async def classify_and_create_entry(
    content: str,
    user_id: str,
    user_jwt: str
) -> Dict[str, Any]:
    """
    Classify user input using AI and create the appropriate entry.

    Args:
        content: User's natural language input
        user_id: User's ID
        user_jwt: User's Supabase JWT

    Returns:
        Dict with:
        - tool: "calendar" or "email"
        - created: Created entry details
        - message: User-facing success message
    """
    try:
        # Step 1: Classify the input
        logger.info(f"Processing app drawer input for user {user_id}")
        classification = await classify_intent(content)

        tool = classification["tool"]
        title = classification["title"]

        logger.info(f"Classified as '{tool}': '{title}'")

        # Step 2: Create the appropriate entry
        if tool == "calendar":
            return await _create_calendar_entry(
                classification=classification,
                user_id=user_id,
                user_jwt=user_jwt
            )

        elif tool == "email":
            return await _create_email_entry(
                classification=classification,
                user_id=user_id,
                user_jwt=user_jwt
            )

        else:
            # Fallback to calendar
            logger.warning(f"Unknown tool type '{tool}', falling back to calendar")
            classification["tool"] = "calendar"
            return await _create_calendar_entry(
                classification=classification,
                user_id=user_id,
                user_jwt=user_jwt
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in classify_and_create_entry: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create entry: {str(e)}"
        )


async def _create_calendar_entry(
    classification: Dict[str, Any],
    user_id: str,
    user_jwt: str
) -> Dict[str, Any]:
    """Create a calendar event from classification result."""
    try:
        # Build event data
        event_data = {
            "title": classification["title"],
            "description": classification.get("notes"),
            "start_time": classification["start_time"],
            "end_time": classification["end_time"],
            "is_all_day": False,
            "status": "confirmed"
        }

        # Create the event (sync function, not async)
        result = create_event(
            user_id=user_id,
            event_data=event_data,
            user_jwt=user_jwt
        )

        created_event = result["event"]

        logger.info(f"Created calendar event '{created_event['title']}' with ID {created_event['id']}")

        return {
            "tool": "calendar",
            "created": {
                "id": created_event["id"],
                "title": created_event["title"],
                "type": "calendar",
                "details": {
                    "start_time": created_event.get("start_time"),
                    "end_time": created_event.get("end_time"),
                    "synced_to_google": result.get("synced_to_google", False)
                }
            },
            "message": "Event added!"
        }

    except Exception as e:
        logger.exception(f"Error creating calendar event: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create calendar event: {str(e)}"
        )


async def _create_email_entry(
    classification: Dict[str, Any],
    user_id: str,
    user_jwt: str
) -> Dict[str, Any]:
    """Create and send an email from classification result."""
    try:
        email_to = classification.get("email_to", "")
        email_subject = classification.get("email_subject", classification["title"])
        email_body = classification.get("email_body", "")

        if not email_to:
            raise HTTPException(
                status_code=400,
                detail="Email recipient is required"
            )

        # Send the email (sync function)
        result = send_email(
            user_id=user_id,
            user_jwt=user_jwt,
            to=email_to,
            subject=email_subject,
            body=email_body
        )

        logger.info(f"Sent email to '{email_to}' with subject '{email_subject}'")

        return {
            "tool": "email",
            "created": {
                "id": result.get("id", ""),
                "title": email_subject,
                "type": "email",
                "details": {
                    "to": email_to,
                    "subject": email_subject,
                    # Note: email_body intentionally omitted to avoid response bloat.
                    # Add "body": email_body here if needed for debugging.
                }
            },
            "message": "Email sent!"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error sending email: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send email: {str(e)}"
        )
