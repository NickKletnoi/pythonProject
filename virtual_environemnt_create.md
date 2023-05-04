1. create project folder
2. go into the folder  
3. open terminal
4. type in terminal:

--- to install new stuff ----
python -m venv venv --prompt="client-new"
venv\Scripts\activate
python -m pip install  requests
python -m pip freeze > requirements.txt
deactivate
or 
--- install existing stuff from file --- 
python -m venv venv --prompt="client-old"
venv\Scripts\activate
pip install -r requirements.txt
#python -m pip install -r requirements.txt
deactivate






---
pip install -r requirements.txt
python -m pip list
python -m pip freeze > requirements.txt
deactivate
---
