"""
Create or update the Gmail Pub/Sub push subscription used for webhook ingress.

This script is migration-friendly: if the subscription already exists, it updates
the push endpoint and push auth config instead of forcing a delete/recreate.

Environment:
    GOOGLE_PUBSUB_TOPIC=projects/PROJECT_ID/topics/TOPIC_NAME
    WEBHOOK_BASE_URL=https://core-webhooks-production.up.railway.app
    PUBSUB_SUBSCRIPTION_NAME=<optional override>
    PUBSUB_ALLOW_ADDITIONAL_SUBSCRIPTION=<optional; defaults to false>
    PUBSUB_PUSH_SERVICE_ACCOUNT_EMAIL=<recommended>
    PUBSUB_PUSH_AUDIENCE=<optional; defaults to WEBHOOK_BASE_URL/api/webhooks/gmail>
    PUBSUB_DEAD_LETTER_TOPIC=projects/PROJECT_ID/topics/TOPIC_NAME
    PUBSUB_MAX_DELIVERY_ATTEMPTS=20
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import List


def _run(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True)


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _build_push_flags(push_endpoint: str) -> List[str]:
    flags = [f"--push-endpoint={push_endpoint}"]

    push_service_account = os.getenv("PUBSUB_PUSH_SERVICE_ACCOUNT_EMAIL", "").strip()
    if push_service_account:
        flags.append(f"--push-auth-service-account={push_service_account}")
        audience = os.getenv("PUBSUB_PUSH_AUDIENCE", "").strip() or push_endpoint
        flags.append(f"--push-auth-token-audience={audience}")

    return flags


def _build_dead_letter_flags() -> List[str]:
    dead_letter_topic = os.getenv("PUBSUB_DEAD_LETTER_TOPIC", "").strip()
    if not dead_letter_topic:
        return []

    max_delivery_attempts = os.getenv("PUBSUB_MAX_DELIVERY_ATTEMPTS", "20").strip() or "20"
    return [
        f"--dead-letter-topic={dead_letter_topic}",
        f"--max-delivery-attempts={max_delivery_attempts}",
    ]


def _list_topic_subscriptions(project_id: str, pubsub_topic: str) -> List[str]:
    """Return subscription names attached to the given topic."""
    cmd = [
        "gcloud",
        "pubsub",
        "subscriptions",
        "list",
        f"--project={project_id}",
        f"--filter=topic:{pubsub_topic}",
        "--format=value(name.basename())",
    ]
    result = _run(cmd)
    if result.returncode != 0:
        print("❌ Failed to list existing Pub/Sub subscriptions for the topic")
        print()
        print(result.stderr)
        sys.exit(1)

    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def setup_pubsub_push_subscription() -> None:
    """Create or update the Gmail Pub/Sub push subscription."""
    pubsub_topic = os.getenv(
        "GOOGLE_PUBSUB_TOPIC",
        "projects/core-478723/topics/gmail-sync-topic",
    ).strip()
    webhook_base_url = os.getenv(
        "WEBHOOK_BASE_URL",
        "",  # Set WEBHOOK_BASE_URL in your environment
    ).rstrip("/")

    parts = pubsub_topic.split("/")
    if len(parts) != 4 or parts[0] != "projects" or parts[2] != "topics":
        print(f"❌ Invalid GOOGLE_PUBSUB_TOPIC format: {pubsub_topic}")
        print("   Expected: projects/PROJECT_ID/topics/TOPIC_NAME")
        sys.exit(1)

    project_id = parts[1]
    topic_name = parts[3]
    explicit_subscription_name = os.getenv("PUBSUB_SUBSCRIPTION_NAME", "").strip()
    allow_additional_subscription = _is_truthy(
        os.getenv("PUBSUB_ALLOW_ADDITIONAL_SUBSCRIPTION", "")
    )
    existing_subscriptions = _list_topic_subscriptions(project_id, pubsub_topic)

    if explicit_subscription_name:
        subscription_name = explicit_subscription_name
        if (
            subscription_name not in existing_subscriptions
            and existing_subscriptions
            and not allow_additional_subscription
        ):
            print("❌ Refusing to create an additional subscription on the Gmail topic")
            print()
            print(
                "The topic already has existing subscriptions, and creating another one "
                "would deliver every Gmail notification to both endpoints."
            )
            print()
            print("Existing subscriptions:")
            for name in existing_subscriptions:
                print(f"   - {name}")
            print()
            print("Use one of these options:")
            print("   1. Set PUBSUB_SUBSCRIPTION_NAME to the existing subscription you want to update")
            print("   2. Delete the old subscription before re-running this script")
            print(
                "   3. If you intentionally want fanout, set "
                "PUBSUB_ALLOW_ADDITIONAL_SUBSCRIPTION=true"
            )
            sys.exit(1)
    elif len(existing_subscriptions) == 1:
        subscription_name = existing_subscriptions[0]
    elif len(existing_subscriptions) == 0:
        subscription_name = f"{topic_name}-push-subscription"
    else:
        print("❌ Multiple subscriptions already exist for the Gmail topic")
        print()
        print(
            "This script will not guess which one to update because that can leave "
            "duplicate webhook delivery in place."
        )
        print()
        print("Existing subscriptions:")
        for name in existing_subscriptions:
            print(f"   - {name}")
        print()
        print("Re-run with PUBSUB_SUBSCRIPTION_NAME set to the exact subscription you want to update.")
        sys.exit(1)

    push_endpoint = f"{webhook_base_url}/api/webhooks/gmail"
    push_flags = _build_push_flags(push_endpoint)
    dead_letter_flags = _build_dead_letter_flags()

    print("=" * 80)
    print("🔧 Configuring Google Cloud Pub/Sub push subscription")
    print("=" * 80)
    print(f"📢 Pub/Sub Topic: {pubsub_topic}")
    print(f"📬 Subscription Name: {subscription_name}")
    print(f"🌐 Push Endpoint: {push_endpoint}")
    print(f"🔑 Project ID: {project_id}")
    if existing_subscriptions:
        print(f"📚 Existing Topic Subscriptions: {', '.join(existing_subscriptions)}")
    else:
        print("📚 Existing Topic Subscriptions: none")
    if push_flags[1:]:
        print("🔐 Authenticated push: enabled")
    else:
        print("🔐 Authenticated push: disabled")
    if dead_letter_flags:
        print(f"💀 Dead-letter topic: {dead_letter_flags[0].split('=', 1)[1]}")
    print()

    check_cmd = [
        "gcloud",
        "pubsub",
        "subscriptions",
        "describe",
        subscription_name,
        f"--project={project_id}",
        "--format=json",
    ]
    exists = _run(check_cmd).returncode == 0

    if exists:
        print(f"♻️ Updating existing subscription '{subscription_name}'...")
        cmd = [
            "gcloud",
            "pubsub",
            "subscriptions",
            "update",
            subscription_name,
            f"--project={project_id}",
            "--ack-deadline=60",
            "--message-retention-duration=7d",
            *push_flags,
            *dead_letter_flags,
        ]
    else:
        print(f"📝 Creating subscription '{subscription_name}'...")
        cmd = [
            "gcloud",
            "pubsub",
            "subscriptions",
            "create",
            subscription_name,
            f"--topic={topic_name}",
            f"--project={project_id}",
            "--ack-deadline=60",
            "--message-retention-duration=7d",
            *push_flags,
            *dead_letter_flags,
        ]

    print(f"Running: {' '.join(cmd)}")
    print()

    result = _run(cmd)
    if result.returncode != 0:
        print("❌ Pub/Sub subscription command failed!")
        print()
        print(result.stderr)
        print()
        print("Common issues:")
        print("   1. Not authenticated: run 'gcloud auth login'")
        print("   2. Wrong project: run 'gcloud config set project YOUR_PROJECT_ID'")
        print("   3. Missing IAM on the push auth service account")
        print("   4. Dead-letter topic missing or missing Pub/Sub service account permissions")
        print("   5. Existing topic subscriptions caused duplicate delivery; set PUBSUB_SUBSCRIPTION_NAME explicitly")
        sys.exit(1)

    print("✅ Pub/Sub push subscription is configured")
    print()
    print("📋 Next Steps:")
    print("   1. Deploy the webhook ingress service on Railway")
    print(f"   2. Verify Gmail webhook health at {push_endpoint.replace('/gmail', '/gmail/verify')}")
    print("   3. Send a test email and confirm Railway logs show '📬 Gmail webhook received'")
    print("   4. If you changed WEBHOOK_BASE_URL, reset Google Calendar watches so their stored addresses move too")
    print()
    print("Google Cloud Console:")
    print(
        "   "
        f"https://console.cloud.google.com/cloudpubsub/subscription/detail/{subscription_name}"
        f"?project={project_id}"
    )
    print("=" * 80)


if __name__ == "__main__":
    try:
        subprocess.run(["gcloud", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Google Cloud SDK (gcloud) is not installed or not in PATH")
        print()
        print("Install from: https://cloud.google.com/sdk/docs/install")
        sys.exit(1)

    setup_pubsub_push_subscription()
