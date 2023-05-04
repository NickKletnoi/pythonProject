import os
import config1 as cfg
from dotenv import load_dotenv
load_dotenv()

user = os.getenv('USER')
key = os.getenv('KEY')
server = cfg.DATABASE_CONFIG['server']

print(user)
print(key)
print(server)