import requests
import openai

api_endpoint = 'https://api.openapi.com/v1/completions'
api_key = 'JEP3DzSsNsDremmeXlDET3BlbkFJ5usKnIG3CBOc7E1OWD7e'


data = {
"model":"text-davinci-003",
"prompt":"Write python script for hello world",
"max_tokens": 100,
"temperature": 0.5
}

response = openai.ChatCompletion.create(
        model = model_name,
        messages = messages,
        n=n,
        temperature = temperature
    )


