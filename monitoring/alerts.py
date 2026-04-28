"""Alert manager — sends notifications when pipelines fail or quality degrades."""

import logging
import os
import smtplib
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

QUALITY_ALERT_THRESHOLD = 80.0


class AlertManager:
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
        subject = f"[DataFlow] Pipeline FAILED: {pipeline_name}"
        body = (
            f"Pipeline '{pipeline_name}' failed at "
            f"{datetime.now(timezone.utc).isoformat()}.\n\n"
            f"Error:\n{error_message}\n\n"
            f"Check the monitoring dashboard for details."
        )
        self._send(subject, body)

    def send_quality_alert(self, pipeline_name: str, quality_score: float, failed_checks: list):
        if quality_score >= QUALITY_ALERT_THRESHOLD:
            return
        subject = f"[DataFlow] Quality Alert: {pipeline_name} ({quality_score:.1f}%)"
        check_list = "\n".join(f"  - {c}" for c in failed_checks)
        body = (
            f"Pipeline '{pipeline_name}' quality score dropped to {quality_score:.1f}%\n"
            f"(threshold: {QUALITY_ALERT_THRESHOLD}%)\n\n"
            f"Failed checks:\n{check_list}"
        )
        self._send(subject, body)

    def _send(self, subject: str, body: str):
        """Log + email. Swallows email errors so pipeline runs are never blocked."""
        try:
            logger.warning("ALERT: %s", subject)
        except Exception:
            pass  # logging must never crash the caller

        if self.email_enabled:
            try:
                self._send_email(subject, body)
            except Exception:
                pass  # silently swallow — caller uses send_email_direct for explicit testing

    def send_email_direct(self, subject: str, body: str) -> dict:
        """
        Send email and RETURN the result dict instead of swallowing errors.
        Used by the /test/email API endpoint so errors are visible.
        """
        if not self.email_enabled:
            return {
                "sent": False,
                "error": "Email not enabled — missing env vars",
                "missing": [
                    v for v in ["ALERT_EMAIL_FROM", "ALERT_EMAIL_TO", "SMTP_USER", "SMTP_PASSWORD"]
                    if not os.getenv(v)
                ],
            }
        try:
            self._send_email(subject, body)
            return {"sent": True, "error": None}
        except smtplib.SMTPAuthenticationError as e:
            return {
                "sent": False,
                "error": f"Authentication failed: {e}",
                "fix": "Your Gmail app password is wrong. Go to myaccount.google.com → Security → App passwords and regenerate it.",
            }
        except smtplib.SMTPException as e:
            return {
                "sent": False,
                "error": f"SMTP error: {e}",
                "fix": "Check SMTP_HOST=smtp.gmail.com and SMTP_PORT=587",
            }
        except OSError as e:
            return {
                "sent": False,
                "error": f"Network error: {e}",
                "fix": "Render free tier may be blocking outbound SMTP on port 587. Try port 465 with SMTP_PORT=465.",
            }
        except Exception as e:
            return {
                "sent": False,
                "error": f"Unexpected error: {e}",
                "traceback": traceback.format_exc(),
            }

    def _send_email(self, subject: str, body: str):
        msg = MIMEMultipart()
        msg["From"] = self.email_from
        msg["To"] = self.email_to
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(self.smtp_user, self.smtp_password)
            server.sendmail(self.email_from, self.email_to, msg.as_string())
