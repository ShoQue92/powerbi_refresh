from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable

import argparse
import logging
import json
import msal
import requests
import time
import logging

'''Vaste variabelen'''
TENNANT_ID = "<Microsoft Tennant ID>"
AUTHORITY_URL = 'https://login.microsoftonline.com/'+TENNANT_ID+'/'
RESOURCE_URL = 'https://analysis.windows.net/powerbi/api'
ENV_FILE = '/opt/airflow/dags/plugins/stedin/operators/powerbi_refresh_env_vars.json'

# environment variable staat opgegeven als variable binnen Airflow zelf
environment = Variable.get("ENV")
client_token_key = f"powerbi_sp-bdmintpbi-{environment.lower()}_client_token"
client_token = Variable.get(client_token_key)


log = logging.getLogger(__name__)

class PowerBIRefreshOperator(BaseOperator):
    # Constructor functie
    def __init__(self, ENV: str, action: str, workspace: str, object: str ,**kwargs) -> None:
        super().__init__(**kwargs)
        self.ENV = ENV
        self.action = action
        self.workspace = workspace
        self.object = object

    def get_environment_settings(self, ENV_FILE):

        if self.ENV == environment:
            log.info(f'Airflow environment: {self.ENV}')
        else:
            raise Exception(f"ENV variable van Airflow niet gevonden, gegeven is: {self.ENV}")

        with open(ENV_FILE) as f:
            _environment = json.load(f)
            return _environment

    def check_return_value(self, primary_function_namme, secondary_function_name, value, input, ):
        if isinstance(value, type(None)):
            raise Exception(f"Fout opgetreden in primaire functie \'{primary_function_namme}\'. Functie \'{secondary_function_name}\' heeft geen resultaat teruggegeven. De invoer was \'{input}\'.")

    def get_access_token_msal(self, authority_url, scope, client_id, client_token):
        context = msal.ConfidentialClientApplication(client_id,authority=authority_url, client_credential=client_token)

        # Initialiseren van lege variabele
        result = None

        # Kijken of de access token nog geldig is (niet expired). Dit checken we voor een app, geen end user (hence account=None)

        result = context.acquire_token_silent(scope, account=None)

        # Als deze er niet is, vraag een nieuwe aan
        if not result:
            log.info('Geen token beschikbaar in cache, er wordt een nieuwe gevraagd van AAD.')
            result = context.acquire_token_for_client(scopes=scope)

        # En als deze dan verkregen is
        if "access_token" in result:
            
            log.info("Access token wordt opgevraagd van AAD.")
            log.info("Token verkregen!")
            log.info("Token type: " + result['token_type'])
            log.info("Geldig tot: " + str(result['expires_in']))
            access_token = result['access_token']
            return access_token

        # Als er wat mis is gegaan, print error gerelateerde zaken uit result
        else:
            log.info(result.get("error"))
            log.info(result.get("error_description"))
            log.info(result.get("correlation_id"))

            raise Exception("Geen token kunnen verkrijgen!")

    def get_dataset_id_in_workspace_by_name(self, workspace_id, dataset_name, access_token):
        dataset_url = 'https://api.powerbi.com/v1.0/myorg/groups/'+workspace_id+'/datasets'
        header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
        r = requests.get(url=dataset_url, headers=header)
        result = r.raise_for_status()
        if not result:
            data = r.json()
            len1 = len(data['value'])
            for i in range(len1):
                if data['value'][i]['name'] == dataset_name:
                    log.info('Dataset ID van ' + dataset_name + ' gevonden; \'' + data['value'][i]['id'] + '\'')
                    return data['value'][i]['id']
        else:
            log.info(f'Er is wat fout gegaan; {str(result)}')

    def get_dataflow_id_in_workspace_by_name(self, workspace_id, dataflow_name, access_token):
        dataset_url = 'https://api.powerbi.com/v1.0/myorg/groups/'+workspace_id+'/dataflows'
        header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
        r = requests.get(url=dataset_url, headers=header)
        result = r.raise_for_status()
        if not result:
            data = r.json()
            len1 = len(data['value'])
            for i in range(len1):
                if data['value'][i]['name'] == dataflow_name:
                    log.info('Dataflow ID van ' + dataflow_name + ' gevonden; \'' + data['value'][i]['objectId'] + '\'')
                    #print(data['value'][i])
                    return data['value'][i]['objectId']
        else:
            log.info(f'Er is wat fout gegaan; {str(result)}')

    def get_dataset_in_workspace_by_id(self, workspace_id, dataset_id, access_token):
        dataset_url = 'https://api.powerbi.com/v1.0/myorg/groups/' + workspace_id + '/datasets/' + dataset_id 
        header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
        r = requests.get(url=dataset_url, headers=header)
        result = r.raise_for_status()
        if not result:
            return r.json()
        else:
            log.info('Er is wat fout gegaan; ' + str(result))

    def get_workspace_id_by_name(self, workspace_name, access_token):
        workspace_url = 'https://api.powerbi.com/v1.0/myorg/groups/'
        header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
        r = requests.get(url=workspace_url, headers=header)
        result = r.raise_for_status()
        if not result:
            data = r.json()
            len1 = len(data['value'])
            for i in range(len1):
                if data['value'][i]['name'] == workspace_name:
                    log.info('Workspace ID van \'' + workspace_name + '\' gevonden; \'' + data['value'][i]['id'] + '\'')
                    return data['value'][i]['id']

    def refresh_dataset(self, workspace_id, dataset_id, client_id, client_token, authority_url, scope):
        access_token = self.get_access_token_msal(authority_url, scope, client_id, client_token)
        dataset_refresh_url = 'https://api.powerbi.com/v1.0/myorg/groups/' + workspace_id + '/datasets/' + dataset_id + '/refreshes'
        header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
        log.info('Dataset ' + dataset_id + ' in workspace ' + workspace_id + ' wordt ververst.')
        r = requests.post(url=dataset_refresh_url, headers=header)
        result = r.raise_for_status()
        if not result:
            log.info(f'Succesvol dataset {dataset_id} ververst!')
            requestId = r.headers.get('RequestId')
            log.info(f'Refresh geregistreerd onder refreshID: {str(requestId)}')
        else:
            log.info(f'Er is wat fout gegaan; {str(result)}')

    def refresh_dataset_by_names(self, workspace_name, dataset_name, client_id, client_token, authority_url, scope):
        access_token = self.get_access_token_msal(authority_url, scope, client_id, client_token)
        workspace_id = self.get_workspace_id_by_name(workspace_name, access_token)
        self.check_return_value(self.refresh_dataset_by_names.__name__, self.get_workspace_id_by_name.__name__, workspace_id, workspace_name)

        dataset_id = self.get_dataset_id_in_workspace_by_name(workspace_id, dataset_name, access_token)
        self.check_return_value(self.refresh_dataset_by_names.__name__, self.get_dataset_id_in_workspace_by_name.__name__, dataset_id, dataset_name)

        dataset_refresh_url = 'https://api.powerbi.com/v1.0/myorg/groups/' + workspace_id + '/datasets/' + dataset_id + '/refreshes'
        header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
        log.info('Dataset ' + dataset_name + ' in workspace ' + '\'' + workspace_name + '\'' + ' wordt ververst.')
        r = requests.post(url=dataset_refresh_url, headers=header)
        result = r.raise_for_status()

        #teruggeven van requstID, deze is nodig voor functie get_refresh_execution_details
        requestId = r.headers.get('RequestId')
        log.info(f'Refresh geregistreerd onder refreshID: {str(requestId)}')

        if not result:
            log.info('Succesvol verzoek voor refreshen van dataset \'' + dataset_name + '\' gegeven!')
            #ophalen details refresh status
            self.get_refresh_execution_details(requestId, workspace_id, dataset_id, access_token)
        else:
            log.info('Er is wat fout gegaan; ' + str(result))

    def refresh_dataflow_by_names(self, workspace_name, dataflow_name, client_id, client_token, authority_url, scope):
        access_token = self.get_access_token_msal(authority_url, scope, client_id, client_token)
        workspace_id = self.get_workspace_id_by_name(workspace_name, access_token)
        self.check_return_value(self.refresh_dataflow_by_names.__name__, self.get_workspace_id_by_name.__name__, workspace_id, workspace_name)

        dataflow_id = self.get_dataflow_id_in_workspace_by_name(workspace_id, dataflow_name, access_token)
        self.check_return_value(self.refresh_dataflow_by_names.__name__, self.get_dataflow_id_in_workspace_by_name.__name__, dataflow_id, dataflow_name)

        dataflow_refresh_url = 'https://api.powerbi.com/v1.0/myorg/groups/' + workspace_id + '/dataflows/' + dataflow_id + '/refreshes?processType=default'
        header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
        request_body = {"refreshRequest": "y"}
        log.info('Dataflow ' + dataflow_name + ' in workspace ' + '\'' + workspace_name + '\'' + ' wordt ververst.')
        r = requests.post(url=dataflow_refresh_url, json=request_body, headers=header)
        log.info(r.text)
        result = r.raise_for_status()

        #teruggeven van requstID, deze is nodig voor functie get_refresh_execution_details
        requestId = r.headers.get('RequestId')
        log.info(f'Refresh geregistreerd onder refreshID: {str(requestId)}')

    def get_refresh_execution_details(self, requestId, workspace_id, dataset_id, access_token):
        refresh_execution_details_url = 'https://api.powerbi.com/v1.0/myorg/groups/' + workspace_id + '/datasets/' + dataset_id + '/refreshes'
        header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
        
        log.info('Resultaat van refresh ophalen, om te zien of dit is gelukt of niet...')
        r = requests.get(url=refresh_execution_details_url, headers=header)
        result = r.raise_for_status()

        #volledig response ophalen en formatten naar json
        response_full = r.json()
        #Alleen de values van de request body ophalen
        response_values_full = response_full['value']
        #Dit geeft een lijstje terug, maar we willen alleen de details weten van de requestID van toepassing
        response_values_matched_requestId = next((d for d in response_values_full if d['requestId'] == requestId), {'status': 'Unknown'})
        #Ophalen wat de status is van de requestId
        refresh_status = response_values_matched_requestId['status']
        
        #Als de status unknown is voor de opgegeven refreshId, dan is deze nog bezig waarschijnlijk. Wacht een minuut en voer opnieuw recursive de functie uit
        if refresh_status == 'Unknown':
            sleep = 60
            log.info(f'Nu nog onbekend, waarschijnlijk omdat deze nog bezig is. We proberen het over {sleep} seconden opnieuw.')
            time.sleep(sleep)
            self.get_refresh_execution_details(requestId, workspace_id, dataset_id, access_token)
        else:
            refresh_status = response_values_matched_requestId['status']
            refresh_startTime = response_values_matched_requestId['startTime']
            refresh_endTime = response_values_matched_requestId['endTime']
            
            log.info('Ophalen status refresh gelukt!')
            log.info(f'Refresh status: {refresh_status}')
            log.info(f'Refresh starttijd: {refresh_startTime}')
            log.info(f'Refresh eindtijd: {refresh_endTime}')

            # Als de status 'Failed' is dan raise Exception
            if refresh_status == 'Failed':
                refresh_serviceExceptionJson = response_values_matched_requestId['serviceExceptionJson']
                refresh_serviceExceptionJsonerrorCode = json.loads(refresh_serviceExceptionJson)['errorCode']
                raise Exception(f"Refresh niet gelukt! Status: \'{refresh_status}\' Melding: \'{refresh_serviceExceptionJsonerrorCode}\' ")

    def parse_execution_arguments(self):
        """
        Function to parse the input arguments (passed by airflow) to be used for the script execution
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('--ENV',
                            help="Argument voor de omgeving waarop de refresh uitgevoerd moet worden; [DEV], [ACC], [PRD]",
                            required=True,
                            type=str)
        parser.add_argument('--action',
                            help="Argument voor de uit te voeren acties; [refresh_dataset], [refresh_dataset_by_names], [get_access_token_msal], [refresh_dataflow_by_names]",
                            required=True,
                            type=str)
        parser.add_argument('--workspace',
                            help="Naam van de Workspace (/werkruimte) binnen de PowerBI service; bijvoorbeeld [\'M&A Reporting\']",
                            required=True,
                            type=str)
        parser.add_argument('--object',
                            help="Naam van het object (/dataset / dataflow) binnen de PowerBI service in een WOrkspace; bijvoorbeeld [\'Metermutatie proces\']",
                            required=True,
                            type=str)
        try:
            args = vars(parser.parse_args())
            environment: str = args['ENV']
            action: str = args['action']
            workspace: str = args['workspace']
            object: str = args['object']

        except Exception as e:
            prilog.infont(f"iet gelukt om de input arguments te parsen!")

        return environment, action, workspace, object

    # Main Execute functie van de operator
    def execute(self, context):
        try:
            # Ophalen van de environment settings ENV_FILE
            environment_settings = self.get_environment_settings(ENV_FILE)

            authority_url = environment_settings[self.ENV][0]['authority']
            scope = environment_settings[self.ENV][0]['scope']
            sa_name = environment_settings[self.ENV][0]['sa_name']
            client_id = environment_settings[self.ENV][0]['client_id']

            commands = {
                "refresh_dataset": self.refresh_dataset,
                "refresh_dataset_by_names":self.refresh_dataset_by_names,
                "get_access_token": self.get_access_token_msal,
                "refresh_dataflow_by_names": self.refresh_dataflow_by_names
            }

            matched_command = commands.get(self.action)

            #check of command bestaat, als dat het geval is roep deze aan met de rest van de argumenten
            if not matched_command:
                    raise Exception("Geen geldig commando gevonden.")
            matched_command(self.workspace, self.object, client_id, client_token, authority_url, scope)
        except Exception as exp:
            log.exception("An error occurred while running the PowerBIRefreshOperator")
            raise