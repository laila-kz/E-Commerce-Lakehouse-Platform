"""
Failure Alerting Callback Plugin
Provides slack_alert_on_failure callback for Airflow DAG default_args.
Supports Slack Webhook as primary and Airflow email as fallback.
"""

import os
import json
import logging
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)


def slack_alert_on_failure(context):
    """
    Airflow task failure callback hook.
    Fires on final task attempt failure after retries are exhausted.
    Sends notification to Slack webhook if configured, else falls back to email.
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id if context.get('dag') else 'unknown_dag'
    task_id = task_instance.task_id if task_instance else 'unknown_task'
    execution_date = context.get('execution_date') or context.get('ds')
    run_id = context.get('run_id') or 'unknown_run_id'
    log_url = getattr(task_instance, 'log_url', 'N/A')
    exception = context.get('exception') or 'Task failed without explicit exception message.'
    hostname = getattr(task_instance, 'hostname', 'airflow_worker')

    message_text = (
        f"🚨 *AIRFLOW TASK FAILURE ALERT*\n"
        f"• *DAG*: `{dag_id}`\n"
        f"• *Task*: `{task_id}`\n"
        f"• *Execution Date*: `{execution_date}`\n"
        f"• *Run ID*: `{run_id}`\n"
        f"• *Host*: `{hostname}`\n"
        f"• *Exception*: ```{str(exception)[:300]}```\n"
        f"• *Log URL*: {log_url}"
    )

    slack_webhook_url = os.environ.get('SLACK_WEBHOOK_URL')

    slack_sent = False
    if slack_webhook_url and slack_webhook_url.strip() and not slack_webhook_url.startswith("<"):
        try:
            payload = json.dumps({"text": message_text}).encode('utf-8')
            req = urllib.request.Request(
                slack_webhook_url,
                data=payload,
                headers={'Content-Type': 'application/json'}
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status in (200, 204):
                    logger.info("Successfully posted failure alert to Slack webhook.")
                    slack_sent = True
                else:
                    logger.warning(f"Slack webhook returned non-200 status: {response.status}")
        except Exception as e:
            logger.error(f"Failed to post failure alert to Slack: {e}")

    # Fallback to Airflow email if Slack was not sent
    if not slack_sent:
        logger.info("Attempting email fallback alert for task failure.")
        try:
            from airflow.utils.email import send_email
            subject = f"[Airflow Failure] Task {task_id} failed in DAG {dag_id}"
            body = message_text.replace("*", "").replace("`", "")
            recipients = [os.environ.get("ALERT_EMAIL", "data-alerts@company.com")]
            send_email(to=recipients, subject=subject, html_content=body)
            logger.info("Successfully dispatched failure notification via email.")
        except Exception as email_err:
            logger.error(f"Email notification fallback failed: {email_err}")
