import os
from langchain.agents import create_pandas_dataframe_agent
from langchain.prompts.chat import ChatPromptTemplate
from langchain.chat_models import ChatOpenAI
from langchain.agents.agent_types import AgentType
from langchain.schema import BaseOutputParser
from langchain.llms import OpenAI
import pandas as pd
from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv())

col_names=['ContactId', 'ColleagueId', 'TaskId', 'Description']

OpenAI_key = os.environ.get("OPENAI_API_KEY")
df = pd.read_csv('unify_call_notes.csv',encoding='ISO-8859-1')

df['motivation'] = df['Description'].str.extract('Motivation: (.*) Completed', expand=False)
df['goals'] = df['Description'].str.extract('Goals: (.*) Motivation', expand=False)
#print(df.head(3))

# agent = create_pandas_dataframe_agent(
#     ChatOpenAI(temperature=0, model="gpt-3.5-turbo-0613",openai_api_key=OpenAI_key),
#     df,
#     verbose=True,
#     agent_type=AgentType.OPENAI_FUNCTIONS,
# )
#
# prompt_template = """You will be provided with unstructured data and your task is to parse it into table format.
# Extract the student goal or goals from the text."""
#
#
# agent = create_pandas_dataframe_agent(OpenAI(temperature=0), df, verbose=True)
# result = agent.run(prompt_template)

print(df)

