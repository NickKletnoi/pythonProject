1. create project folder
2. go into the folder  
3. open terminal
4. type in terminal:
------------------------------
--- to install new stuff ----
------------------------------
python -m venv client-new-env --prompt="client-new-env"
client-new-env\Scripts\activate.bat
python -m pip install  requests
python -m pip list
python -m pip show requests
#python -m pip uninstall requests
python -m pip freeze > requirements.txt
deactivate
-------------------------------
or 
----------------------------------------
--- install existing stuff from file --- 
----------------------------------------
python -m venv client-old-env --prompt="client-old-env"
client-old-env\Scripts\activate.bat
python -m pip install -r requirements.txt
python -m pip list
deactivate






---
pip install -r requirements.txt
python -m pip list
python -m pip freeze > requirements.txt
deactivate
---
