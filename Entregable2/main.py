import os
from dotenv import load_dotenv
from utils.dbHandlers import Redshift_Handler
from utils.dataHandlers import Articles
from utils.pathManager import PathDir
# Configuraciones y variables 
load_dotenv()

url = os.getenv("TOP_HEADLINES_URL")

# Parametros para la api 
params = {}
params["country"] = "ar"
params["category"] = "technology"
params["pageSize"] = 100
params["apikey"] = os.getenv("KEY")

# Inicializando clases

redshift = Redshift_Handler()
articulos = Articles()

# Ejecutando la api con los parametros indicados
source = articulos.call_api(url,args=params)

# Limpiando  y transformando la informacion obtenida 
transform = articulos.normalize_data(source)

# Creando la conexion con redshift
conexion = redshift.get_engine()

# Creando la tabla en redshift
table = redshift.create_table("articles.sql", conexion)

# Insertando la información obtenida en la tabla previamente create
load = redshift.write_df(transform,conexion)
