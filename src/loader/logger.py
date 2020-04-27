import json
import requests

# Set the webhook_url to the one provided by Slack when you create the webhook at https://my.slack.com/services/new/incoming-webhook/
def post_slack(error_args):

    webhook_url = os.getenv("SLACK_WEBHOOK")
    slack_data = {
        "text": "*Não!*\n\n*Origem:* {origin}\n*Tipo de erro:* {error_type}\n*Erro:* ```{error}```".format(
            **error_args
        )
    }

    response = requests.post(
        webhook_url,
        data=json.dumps(slack_data),
        headers={"Content-Type": "application/json"},
    )
    if response.status_code != 200:
        raise ValueError(
            "Request to slack returned an error %s, the response is:\n%s"
            % (response.status_code, response.text)
        )


def log(error_args):

    post_slack(error_args)
