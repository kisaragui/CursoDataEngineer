import os
from dotenv import load_dotenv
from utils.dbHandlers import redshift_connect, create_table_in_redshift, load_data_to_redshift
from utils.dataHandlers import get_data, normalize_data

# Configuraciones y variables 
load_dotenv()

url = os.getenv("TOP_HEADLINES_URL")

# Parametros para la api 
params = {}
params["country"] = "ar"
params["category"] = "technology"
params["pageSize"] = 100
params["apikey"] = os.getenv("KEY")

# Ejecutando la api con los parametros indicados
data = get_data(url,args=params)

# Limpiando  y transformando la informacion obtenida 
clean = normalize_data(data)

# Creando la conexion con redshift
conexion = redshift_connect()

# Creando la tabla en redshift
table = create_table_in_redshift("articles.sql", conexion)

# Insertando la información obtenida en la tabla previamente create
load_data = load_data_to_redshift(clean,conexion)
