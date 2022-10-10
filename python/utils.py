
#Copy this code to initialize Kensu

from kensu.utils.kensu_provider import KensuProvider

ingestion_url = ""
ingestion_token = ""
process = ""
user = ""
project = ""
env = ""

KensuProvider().initKensu(kensu_ingestion_url=ingestion_url, kensu_ingestion_token=ingestion_token, process_name= process,
                            user_name= user, code_location='https://gitlab.example.com',
                            project_name=project, environment= env, pandas_support=True, sklearn_support=False,
                                tensorflow_support=False, bigquery_support=False,pyspark_support=False)