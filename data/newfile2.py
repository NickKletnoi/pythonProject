import logging
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    req_body = req.get_json()
    firstName = req_body['firstName']
    lastName = req_body['lastName']
    return func.HttpResponse(
        f"Hello, you -  {firstName} {lastName}. This HTTP triggered function executed successfully.")

