import os
from utils.dbHandlers import Redshift_Handler
from utils.dataHandlers import Articles
from utils.pathManager import PathDir
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# Parametros para la api 

url = os.environ["TOP_HEADLINES_URL"]
api_params = {}
api_params["country"] = "ar"
api_params["category"] = "technology"
api_params["pageSize"] = 100
api_params["apikey"] = os.environ["KEY"]

# Inicializando clases

redshift = Redshift_Handler()
articulos = Articles()
path = PathDir()
# Obteniendo la conexion de redshift

conexion = redshift.get_engine()


@dag(os.environ["PROJECT"], 
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'retry_delay': timedelta(minutes=5),
            "start_date": datetime(2023,7,20),
        },
        catchup = False,
        schedule_interval= '0 0 * * *'
)
def main():
    
    """
    
    Función principal que ejecuta los tasks 

   """
   
    #task de inicio
    start = EmptyOperator(task_id ="Start")
    
    # Ejecutando la api con los parametros indicados
    @task()
    def call_api_task(url, prs):
        return articulos.call_api(url,args=prs)
    
    # Limpiando  y transformando la informacion obtenida
    @task()
    def normalize_data_task(data):
        return articulos.normalize_data(data)

    # Creando la tabla en redshift
    @task()
    def create_table_task(script_name, conexion):
        return redshift.create_table(script_name, conexion)

    # Insertando la información obtenida en la tabla previamente create
    @task()
    def load_task(df, conexion):
        return redshift.write_df(df,conexion)
    
    # Ultimo task
    finish = EmptyOperator(task_id ="Finish")
    
    call_api = call_api_task(url, api_params)
    clean_data = normalize_data_task(call_api)
    create_table = create_table_task("articles.sql", conexion)
    insert_data = load_task(clean_data, conexion)
    
    #  #Ordenar de ejecucion de los dags 
    start >> create_table >>  call_api >> clean_data >> insert_data >> finish
    
main()
    