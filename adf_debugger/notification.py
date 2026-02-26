"""
ADF Pipeline Debugger - Notification Service
Sends rich HTML diagnostic emails via Gmail SMTP.
"""
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional

logger = logging.getLogger("adf_debugger.notification")


class NotificationService:
    """Send diagnostic reports via Gmail SMTP."""

    def __init__(
        self,
        smtp_host: str = "smtp.gmail.com",
        smtp_port: int = 587,
        username: str = "",
        password: str = "",
        from_address: str = "",
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_address = from_address or username
        logger.info(f"Notification service configured with {smtp_host}:{smtp_port}")

    def send_email(
        self,
        to_addresses: List[str],
        subject: str,
        html_body: str,
        plain_text_body: str = "",
        cc_addresses: List[str] = None,
        priority: str = "normal",
    ) -> bool:
        """
        Send a diagnostic email.

        Args:
            to_addresses: List of recipient email addresses
            subject: Email subject line
            html_body: Rich HTML email content
            plain_text_body: Plain text fallback
            cc_addresses: Optional CC recipients
            priority: 'high', 'normal', or 'low'

        Returns:
            True if email sent successfully, False otherwise
        """
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.from_address
            msg["To"] = ", ".join(to_addresses)

            if cc_addresses:
                msg["Cc"] = ", ".join(cc_addresses)

            # Set priority headers
            if priority == "high":
                msg["X-Priority"] = "1"
                msg["X-MSMail-Priority"] = "High"
                msg["Importance"] = "High"
            elif priority == "low":
                msg["X-Priority"] = "5"
                msg["X-MSMail-Priority"] = "Low"
                msg["Importance"] = "Low"

            # Attach plain text version first (fallback)
            if plain_text_body:
                msg.attach(MIMEText(plain_text_body, "plain", "utf-8"))

            # Attach HTML version (preferred)
            msg.attach(MIMEText(html_body, "html", "utf-8"))

            # All recipients
            all_recipients = list(to_addresses)
            if cc_addresses:
                all_recipients.extend(cc_addresses)

            # Send via SMTP
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(self.username, self.password)
                server.sendmail(
                    self.from_address,
                    all_recipients,
                    msg.as_string(),
                )

            logger.info(
                f"Email sent successfully to {', '.join(to_addresses)} "
                f"| Subject: {subject}"
            )
            return True

        except smtplib.SMTPAuthenticationError as e:
            logger.error(
                f"SMTP authentication failed: {e}. "
                "Please check your Gmail App Password. "
                "You need to generate an App Password at: "
                "https://myaccount.google.com/apppasswords"
            )
            return False
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def send_diagnostic_report(
        self,
        report: dict,
        to_addresses: List[str] = None,
        cc_addresses: List[str] = None,
    ) -> bool:
        """
        Send a formatted diagnostic report.

        Args:
            report: Dict from ReportBuilder.build_report()
            to_addresses: Override default recipients
            cc_addresses: CC recipients

        Returns:
            True if sent successfully
        """
        if not to_addresses:
            logger.error("No recipient addresses specified")
            return False

        # Determine priority from severity
        severity = report.get("severity", "medium")
        priority_map = {
            "critical": "high",
            "high": "high",
            "medium": "normal",
            "low": "low",
        }
        priority = priority_map.get(severity, "normal")

        return self.send_email(
            to_addresses=to_addresses,
            subject=report.get("subject", "ADF Pipeline Failure Alert"),
            html_body=report.get("html", ""),
            plain_text_body=report.get("plain_text", ""),
            cc_addresses=cc_addresses,
            priority=priority,
        )

    def send_test_email(self, to_address: str) -> bool:
        """Send a test email to verify SMTP configuration."""
        test_html = """
        <html>
        <body style="font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e4e6eb; padding: 40px;">
            <div style="max-width: 500px; margin: auto; background: #1a1d27; border-radius: 12px; padding: 32px; text-align: center;">
                <h1 style="color: #22c55e; margin-bottom: 16px;">✅ Connection Successful!</h1>
                <p style="font-size: 16px; color: #c4c7d4;">
                    Your ADF Pipeline Debugger email notifications are working correctly.
                </p>
                <p style="font-size: 13px; color: #8b8fa3; margin-top: 24px;">
                    You will receive detailed diagnostic reports here when pipeline failures occur.
                </p>
            </div>
        </body>
        </html>
        """

        return self.send_email(
            to_addresses=[to_address],
            subject="[TEST] ADF Pipeline Debugger - Email Test",
            html_body=test_html,
            plain_text_body="ADF Pipeline Debugger email test successful!",
        )
