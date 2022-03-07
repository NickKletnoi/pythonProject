import os
import decouple

os.environ['HOME'] = '/new/value'

API_USERNAME = config('USER')
API_KEY = config('KEY')