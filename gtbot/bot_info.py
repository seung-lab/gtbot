from os import environ
slack_token = environ["SLACK_BOT_TOKEN"]
oauth_token = environ["OAUTH_TOKEN"]
workspace_prefix = environ["WORKSPACE_PREFIX"]
botid = "<@{}>".format("BOT_ID")
