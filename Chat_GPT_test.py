# API_KEY = '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'

import openai

my_api_key = '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'
model_name = 'gpt-3.5-turbo'
n = 2
temperature = 1.2

#dictionary
message_1 = {'role': 'system', 'content':'You are a helpful assistant.' }
message_2 = {'role': 'user', 'content':'Who are you?' }
message_3 = {'role': 'assistant', 'content':'My name is Leona' }
message_4 = {'role': 'user', 'content':'What is your name?' }

#list
messages = [message_1, message_2,message_3,message_4]

openai.api_key = my_api_key
response = openai.ChatCompletion.create(
        model = model_name,
        messages = messages,
        n=n,
        temperature = temperature
    )
print(response)
