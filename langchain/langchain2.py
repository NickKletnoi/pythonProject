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

OpenAI_key = os.environ.get("OPENAI_API_KEY")
df = pd.read_csv("C:\\Users\\kletn\\PycharmProjects\\PythonProject\\data\\titanic.csv")

agent = create_pandas_dataframe_agent(
    ChatOpenAI(temperature=0, model="gpt-3.5-turbo-0613",openai_api_key=OpenAI_key),
    df,
    verbose=True,
    agent_type=AgentType.OPENAI_FUNCTIONS,
)

df1 = df.copy()
df1["Age"] = df1["Age"].fillna(df1["Age"].mean())


prompt_template = """You are a helpful assistant who generates comma separated lists.
A user will pass in a category, and you should generate 5 objects in that category in a comma separated list.
ONLY return a comma separated list, and nothing more."""

prompt_template2 ="how many rows in the age column are different?"

agent = create_pandas_dataframe_agent(OpenAI(temperature=0), [df, df1], verbose=True)
result = agent.run(prompt_template2)

