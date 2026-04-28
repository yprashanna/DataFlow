"""Alert manager — sends notifications when pipelines fail or quality degrades.

Currently supports: log-based alerts (always on) and email via SMTP (optional).
Email needs SMTP creds in .env — if they're missing, we just log loudly.
# TODO: add Slack webhook support (it's just a POST request)
"""

import logging
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Quality score below this triggers an alert
QUALITY_ALERT_THRESHOLD = 80.0


class AlertManager:
    """Sends alerts through configured channels.

    Environment variables (all optional — alerts fall back to logs):
        ALERT_EMAIL_FROM    = sender@example.com
        ALERT_EMAIL_TO      = ops@example.com
        SMTP_HOST           = smtp.gmail.com
        SMTP_PORT           = 587
        SMTP_USER           = sender@example.com
        SMTP_PASSWORD       = app_password_here
    """

    def __init__(self):
        self.email_from = os.getenv("ALERT_EMAIL_FROM")
        self.email_to = os.getenv("ALERT_EMAIL_TO")
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.email_enabled = all([
            self.email_from, self.email_to, self.smtp_user, self.smtp_password
        ])

    def send_failure_alert(self, pipeline_name: str, error_message: str):
        """Alert when a pipeline fails."""
        subject = f"[DataFlow] Pipeline FAILED: {pipeline_name}"
        body = (
            f"Pipeline '{pipeline_name}' failed at "
            f"{datetime.now(timezone.utc).isoformat()}.\n\n"
            f"Error:\n{error_message}\n\n"
            f"Check the monitoring dashboard for details."
        )
        self._send(subject, body, level="ERROR")

    def send_quality_alert(self, pipeline_name: str, quality_score: float, failed_checks: list):
        """Alert when data quality score drops below threshold."""
        if quality_score >= QUALITY_ALERT_THRESHOLD:
            return  # all good, no alert needed

        subject = f"[DataFlow] Quality Alert: {pipeline_name} ({quality_score:.1f}%)"
        check_list = "\n".join(f"  - {c}" for c in failed_checks)
        body = (
            f"Pipeline '{pipeline_name}' quality score dropped to {quality_score:.1f}%\n"
            f"(threshold: {QUALITY_ALERT_THRESHOLD}%)\n\n"
            f"Failed checks:\n{check_list}\n\n"
            f"Check the monitoring dashboard for details."
        )
        self._send(subject, body, level="WARNING")

    def _send(self, subject: str, body: str, level: str = "INFO"):
        """Log the alert and optionally email it."""
        log_fn = logger.error if level == "ERROR" else logger.warning
        log_fn("ALERT — %s: %s", subject, body)

        if self.email_enabled:
            try:
                self._send_email(subject, body)
                logger.info("Alert email sent to %s", self.email_to)
            except Exception as exc:
                logger.error("Failed to send alert email: %s", exc)

    def _send_email(self, subject: str, body: str):
        msg = MIMEMultipart()
        msg["From"] = self.email_from
        msg["To"] = self.email_to
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            server.sendmail(self.email_from, self.email_to, msg.as_string())
