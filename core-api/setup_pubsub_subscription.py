"""
Setup Google Cloud Pub/Sub Push Subscription for Gmail Webhooks

This script creates a push subscription that forwards Gmail notifications
from your Pub/Sub topic to your webhook endpoint.

Prerequisites:
1. Google Cloud SDK installed (gcloud)
2. Authenticated with proper permissions
3. GOOGLE_PUBSUB_TOPIC environment variable set

Usage:
    python setup_pubsub_subscription.py
"""
import os
import subprocess
import sys

def setup_pubsub_push_subscription():
    """
    Create a Pub/Sub push subscription for Gmail webhooks
    """
    # Get configuration from environment
    pubsub_topic = os.getenv('GOOGLE_PUBSUB_TOPIC', 'projects/YOUR_PROJECT_ID/topics/gmail-sync-topic')
    webhook_url = os.getenv('WEBHOOK_BASE_URL', 'http://localhost:8000')
    
    # Extract project ID and topic name from full topic path
    # Format: projects/PROJECT_ID/topics/TOPIC_NAME
    parts = pubsub_topic.split('/')
    if len(parts) != 4 or parts[0] != 'projects' or parts[2] != 'topics':
        print(f"❌ Invalid GOOGLE_PUBSUB_TOPIC format: {pubsub_topic}")
        print("   Expected: projects/PROJECT_ID/topics/TOPIC_NAME")
        sys.exit(1)
    
    project_id = parts[1]
    topic_name = parts[3]
    
    # Subscription name
    subscription_name = f"{topic_name}-push-subscription"
    full_subscription = f"projects/{project_id}/subscriptions/{subscription_name}"
    
    # Push endpoint — include verification token
    webhook_token = os.getenv("GOOGLE_WEBHOOK_TOKEN", "").strip()
    if not webhook_token:
        print("GOOGLE_WEBHOOK_TOKEN is required for Gmail push endpoint")
        sys.exit(1)
    push_endpoint = f"{webhook_url}/api/webhooks/gmail?token={webhook_token}"
    
    print("=" * 80)
    print("🔧 Setting up Google Cloud Pub/Sub Push Subscription")
    print("=" * 80)
    print(f"📢 Pub/Sub Topic: {pubsub_topic}")
    print(f"📬 Subscription Name: {subscription_name}")
    print(f"🌐 Push Endpoint: {push_endpoint}")
    print(f"🔑 Project ID: {project_id}")
    print()
    
    # Check if subscription already exists
    print("🔍 Checking if subscription already exists...")
    check_cmd = [
        'gcloud', 'pubsub', 'subscriptions', 'describe',
        subscription_name,
        f'--project={project_id}',
        '--format=json'
    ]
    
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"⚠️ Subscription '{subscription_name}' already exists!")
        print()
        print("To update the push endpoint, delete and recreate:")
        print(f"  gcloud pubsub subscriptions delete {subscription_name} --project={project_id}")
        print()
        print("Then run this script again.")
        return
    
    # Create the push subscription
    print(f"📝 Creating push subscription...")
    create_cmd = [
        'gcloud', 'pubsub', 'subscriptions', 'create',
        subscription_name,
        f'--topic={topic_name}',
        f'--push-endpoint={push_endpoint}',
        f'--project={project_id}',
        '--ack-deadline=60',  # 60 second ack deadline
        '--message-retention-duration=7d',  # Keep messages for 7 days if undelivered
    ]
    
    print(f"Running: {' '.join(create_cmd)}")
    print()
    
    result = subprocess.run(create_cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ Push subscription created successfully!")
        print()
        print("📋 Next Steps:")
        print("   1. Your Gmail webhook should now receive notifications at:")
        print(f"      {push_endpoint}")
        print()
        print("   2. Test by sending yourself an email")
        print()
        print("   3. Check logs for: '📬 Gmail webhook received'")
        print()
        print("   4. Verify in Google Cloud Console:")
        print(f"      https://console.cloud.google.com/cloudpubsub/subscription/detail/{subscription_name}?project={project_id}")
        print()
    else:
        print("❌ Failed to create push subscription!")
        print()
        print("Error output:")
        print(result.stderr)
        print()
        print("Common issues:")
        print("   1. Not authenticated: Run 'gcloud auth login'")
        print("   2. Wrong project: Run 'gcloud config set project YOUR_PROJECT_ID'")
        print("   3. Missing permissions: Need 'pubsub.subscriptions.create' permission")
        print("   4. Topic doesn't exist: Create the topic first")
        sys.exit(1)
    
    print("=" * 80)


if __name__ == "__main__":
    # Check if gcloud is installed
    try:
        subprocess.run(['gcloud', '--version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Google Cloud SDK (gcloud) is not installed or not in PATH")
        print()
        print("Install from: https://cloud.google.com/sdk/docs/install")
        sys.exit(1)
    
    setup_pubsub_push_subscription()

