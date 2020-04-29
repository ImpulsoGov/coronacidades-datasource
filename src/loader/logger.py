import json
import requests
import os

# Set the webhook_url to the one provided by Slack when you create the webhook at https://my.slack.com/services/new/incoming-webhook/
def post_slack(error_args, status):
    
    webhook_url = os.getenv("SLACK_WEBHOOK")
    
    if status == 'fail':
        slack_data = {
            "text": "*Não!*\n\n*Origem:* {origin}\n*Tipo de erro:* {error_type}\n*Erro:* ```{error}```".format(
                **error_args
            )
        }
    
#     if status == 'okay':

#         slack_data = {
#             "text": "Sim! Tudo sob controle :D"
#         }

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

def log(error_args, status):

    post_slack(error_args, status)
