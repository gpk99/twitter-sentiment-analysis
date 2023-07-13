import requests
import json
import tweepy
from keys import BEARERTOKEN
# Bearer_token used to access the tweepy
bearer_Token = BEARERTOKEN
# Keyword to be searched
key_word = "fifaworldcup2022"
# Only text field is needed to execute
req_fields = "tweet.fields=text"
# Function to search desired keyword
def access_search_key(search_term, twitter_data_fields, bearerToken = bearer_Token):
    auth = {"Authorization": "Bearer {}".format(bearerToken)}
    url_req = "https://api.twitter.com/2/tweets/search/recent?query={}&{}".format(key_word, twitter_data_fields)
    response = requests.request("GET", url_req, headers = auth)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()
# Calling the function to search keyword
json_response = access_search_key(search_term = key_word, twitter_data_fields = req_fields, bearerToken= bearer_Token)
#pretty printing
print(json.loads(json.dumps(json_response, indent=4, sort_keys=True)))
with open('_fifaworldcup2022.json', 'w') as f:
   f.write((json.dumps(json_response, indent=4, sort_keys=True)))