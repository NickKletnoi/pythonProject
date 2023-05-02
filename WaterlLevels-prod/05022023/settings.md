function.json
--------------------------
{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "name": "mytimer",
      "type": "timerTrigger",
      "direction": "in",
      "schedule": "0 * * * *"
    }
  ]
}
----------------------------------------
requirements.txt
-----------------------------------------
autopep8==2.0.2
azure-functions==1.12.0
certifi==2022.12.7
charset-normalizer==3.0.1
greenlet==2.0.2
idna==3.4
numpy==1.24.1
pandas==1.5.3
pycodestyle==2.10.0
pyodbc==4.0.35
python-dateutil==2.8.2
pytz==2022.7.1
requests==2.28.2
six==1.16.0
SQLAlchemy==2.0.1
tomli==2.0.1
typing_extensions==4.4.0
urllib3==1.26.14
------------------------------------------
host.json
------------------------------------------
{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "excludedTypes": "Request"
      }
    }
  },
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[3.3.0, 4.0.0)"
  }
}