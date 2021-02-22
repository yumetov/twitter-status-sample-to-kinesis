import json
import boto3
import tweepy
from keys import ConsumerKey, ConsumerSecret, AccessToken, AccessSecret, IamAccessKey, IamSecretKey


class Listener(tweepy.StreamListener):
    def __init__(self, firehose_client):
        tweepy.StreamListener.__init__(self)
        self.client = firehose_client

    def on_status(self, status):
        status_dict = status._json
        if status_dict['lang'] != 'ja':
            return True
        status_json_text = json.dumps(status_dict) + '\n'
        response = self.client.put_record(
            DeliveryStreamName='twitter-statuses-sample-stream',
            Record={
                'Data': status_json_text
            }
        )
        return True

    def on_error(self, status_code):
        print('Got an error with status code: ' + str(status_code))
        return True

    def on_timeout(self):
        print('Timeout...')
        return True


def main():
    client = boto3.client('firehose',
                          aws_access_key_id=IamAccessKey,
                          aws_secret_access_key=IamSecretKey,
                          region_name='ap-northeast-1')

    auth = tweepy.OAuthHandler(ConsumerKey, ConsumerSecret)
    auth.set_access_token(AccessToken, AccessSecret)
    api = tweepy.API(auth)
    listener = Listener(client)
    streaming = tweepy.Stream(auth, listener)

    streaming.sample()


if __name__ == "__main__":
    main()
