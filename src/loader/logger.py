from utils import secrets
from loguru import logger
from notifiers.logging import NotificationHandler

handler = NotificationHandler(
    "slack", defaults=dict(webhook_url=secrets(["SLACK_WEBHOOK"])),
)
logger.add(handler, level="ERROR")
