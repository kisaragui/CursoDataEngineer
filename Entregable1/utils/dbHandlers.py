import requests as r
import os
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import pandas as pd

load_dotenv()

def pathdir():
        
    """
        Funcion para obtener la ruta absoluta del directorio del proyecto.

        Returns
        -------
            directory: Ruta concadenada a la carpeta del primer entregable
    """
        
    directory = os.path.join(os.getcwd(), "Entregable1")
        
    return directory


def redshift_connect():
    
    """
        Funcion encargada de hacer la conexion con redshift.
        
        Returns
        -------
            con: Objeto con la conexion enncendida.
    """
    
    try:
        path= pathdir()
        file_exist = os.path.isfile('{}/.env'.format(path))
        
        if file_exist:
            
            url = URL.create(
                drivername = os.getenv("REDSHIFT_DRIVERNAME"),
                host= os.getenv("REDSHIFT_HOST"),
                port=int(os.getenv("REDSHIFT_PORT")),
                database=os.getenv("REDSHIFT_DATABASE"),
                username=os.getenv("REDSHIFT_USER"),
                password=os.getenv("REDSHIFT_PASS")) 
            
            engine = sa.create_engine(url)
            con = engine.connect() 
            
            print("Conexion a Redshift exitosa!!")
            
        else:
            con = print("Conexion a Redshift fallida...")
            print("El archivo .env no se encuentra...")
        
    except Exception as err:
        return err
    
    return con

def create_table_in_redshift(script_name: str, conn: object ):
    
    """
        Ejecuta el script que contiene la tabla y la cantidad de columnas a necesitar.

        Parameters
        ----------
            script_name (str): Obligatorio.
                Nombre del script ubicado en la carpeta "db"
            conn (object): Obligatorio.
                Objeto con la conexion de redshift.

        Returns
        -------
            msg: Mensaje si creo la tabla o ya fue creada
    """
    
    path = os.path.join(pathdir(), "db")
    table_name = script_name.rsplit('.', 1)[0]
    
    if conn:
        try:
            with open("{}/{}".format(path,script_name), 'r') as query:

                conn.execute(query.read())
                msg = print("Se creo la tabla {} exitosamente!".format(table_name))
                
        except Exception as err:
            msg= print("La tabla {} ya fue creada o existe...".format(table_name))
    else:
        msg = print("Conexion a Redshift fallida...")
         
    return msg


def load_data_to_redshift(df: "pd.DataFrame", conn: object):
    
    """
        Carga el contenido del dataframe a la tabla previamente creada en redshift
    
        Parameters
        ----------
            df (Dataframe): Obligatorio.
                Dataframe que contiene los articulos a insertar en la tabla.
            conn (object): Obligatorio.
                Objeto con la conexion de redshift.

        Returns
        -------
            msg: Mensaje con el numero de articulos insertados 
                o no se pudo insertar los articulos porque la tabla 
                no fue creada aun.
    """
    
    table_name = "articles"
    schema = os.getenv("REDSHIFT_USER")
    
    if conn.dialect.has_table(conn, table_name):  
            
        df.to_sql(table_name, schema=schema,  if_exists="append", index=False, con=conn)
        msg = print("Se registron {} articulos".format(len(df)))
    else:
        msg = print("La tabla {} aun no se ha creado".format(table_name))
        
    return msg   


    