import requests

def send_webhook_alert(title, message, webhook, logger):
    if not webhook or len(webhook) == 0:
        logger.info(f'Webhook was not specified.')
        return

    logger.info(f'ALERT: {title}.\n{message}')
    fmt_message = "## {}\n\n{}".format(title, message)
    try:
        requests.post(webhook, json={'text': fmt_message})
    except Exception as e:
        logger.info(e)