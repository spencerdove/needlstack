"""
Resend email wrapper for sending changelog notifications.
"""
import logging
import os
from pathlib import Path
from typing import List, Optional

import markdown
import resend

logger = logging.getLogger(__name__)


def _init_resend():
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        raise EnvironmentError("RESEND_API_KEY not set")
    resend.api_key = api_key


def parse_changelog(version: str) -> dict:
    """Read and parse a changelog markdown file. Returns dict with metadata and content."""
    changelog_dir = Path(__file__).resolve().parent.parent / "changelogs"
    filepath = changelog_dir / f"{version}.md"
    if not filepath.exists():
        raise FileNotFoundError(f"Changelog not found: {filepath}")

    text = filepath.read_text()

    # Parse YAML frontmatter
    meta = {}
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) >= 3:
            for line in parts[1].strip().splitlines():
                if ":" in line:
                    key, val = line.split(":", 1)
                    meta[key.strip()] = val.strip()
            text = parts[2].strip()

    html_body = markdown.markdown(text, extensions=["extra"])
    return {
        "version": meta.get("version", version),
        "date": meta.get("date", ""),
        "title": meta.get("title", ""),
        "markdown": text,
        "html": html_body,
    }


def send_changelog(
    version: str,
    recipients: List[dict],
    from_email: Optional[str] = None,
    dry_run: bool = False,
) -> List[dict]:
    """
    Send a changelog email to all recipients.

    Args:
        version: Version string (e.g. 'v4.0')
        recipients: List of dicts with 'email' and 'name' keys
        from_email: Override RESEND_FROM_EMAIL env var
        dry_run: If True, return email data without sending

    Returns:
        List of send results (or preview dicts in dry_run mode)
    """
    changelog = parse_changelog(version)
    from_addr = from_email or os.getenv("RESEND_FROM_EMAIL", "updates@needlstack.com")
    subject = f"Needlstack {changelog['version']} — {changelog['title']}"

    html_content = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h1 style="color: #e2e8f0;">Needlstack {changelog['version']}</h1>
        <p style="color: #94a3b8;">{changelog['date']}</p>
        <hr style="border-color: #334155;">
        <div style="color: #cbd5e1; line-height: 1.6;">
            {changelog['html']}
        </div>
        <hr style="border-color: #334155;">
        <p style="color: #64748b; font-size: 0.85em;">
            Reply to this email with feedback — it gets read and categorized automatically.
        </p>
    </div>
    """

    results = []
    for recipient in recipients:
        email_data = {
            "from": from_addr,
            "to": [recipient["email"]],
            "subject": subject,
            "html": html_content,
            "reply_to": os.getenv("RESEND_REPLY_TO", from_addr),
        }

        if dry_run:
            results.append({"status": "dry_run", "to": recipient["email"], "subject": subject})
            logger.info(f"[DRY RUN] Would send to {recipient['email']}")
            continue

        _init_resend()
        try:
            resp = resend.Emails.send(email_data)
            results.append({"status": "sent", "to": recipient["email"], "id": resp.get("id")})
            logger.info(f"Sent changelog to {recipient['email']}: {resp.get('id')}")
        except Exception as exc:
            results.append({"status": "error", "to": recipient["email"], "error": str(exc)})
            logger.error(f"Failed to send to {recipient['email']}: {exc}")

    return results
