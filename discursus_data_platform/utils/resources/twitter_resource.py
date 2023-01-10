from dagster import resource, StringSource
import tweepy

class TwitterAPIClient:
    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        twitter_auth_keys = {
            "consumer_key"        : consumer_key,
            "consumer_secret"     : consumer_secret,
            "access_token"        : access_token,
            "access_token_secret" : access_token_secret
        }
    
        auth = tweepy.OAuthHandler(
            twitter_auth_keys['consumer_key'],
            twitter_auth_keys['consumer_secret']
        )
        auth.set_access_token(
            twitter_auth_keys['access_token'],
            twitter_auth_keys['access_token_secret']
        )

        self.api = tweepy.API(auth)

    
    # API methods
    ######################
    def post(self, status, media_ids):
        status = self.api.update_status(
                status = status, 
                media_ids = media_ids)
        
        return(status)
    

    def upload_media(self, filename):
        media = self.api.media_upload(
                filename = filename)
        
        return(media)



@resource(
    config_schema={
        "resources": {
            "twitter_client": {
                "config": {
                    "consumer_key": StringSource,
                    "consumer_secret": StringSource,
                    "access_token": StringSource,
                    "access_token_secret": StringSource
                }
            }
        }
    },
    description="Twitter API client.",
)
def twitter_api_client(context):
    return TwitterAPIClient(
        consumer_key = context.resource_config["resources"]["twitter_client"]["config"]["consumer_key"],
        consumer_secret = context.resource_config["resources"]["twitter_client"]["config"]["consumer_secret"],
        access_token = context.resource_config["resources"]["twitter_client"]["config"]["access_token"],
        access_token_secret = context.resource_config["resources"]["twitter_client"]["config"]["access_token_secret"]
    )