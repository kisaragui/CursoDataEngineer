import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from utils.pathManager import PathDir
from psycopg2.extras import execute_values


class Redshift_Handler():
    
    """
        Clase encargada de la gestion con redshift
    
    """
    
    load_dotenv()
        
    def get_engine(self):
    
        """
            Obtiene el motor con los parametros me cargados de redshift.
        
            Returns
            -------
                engine: Objeto con el motor de redshift.
        """
    
        try:
        
            engine = ""
            path = PathDir()
            env = ".env"
            file_exist = path.file_exist_to(path.current_directory, env)
            drivername = os.getenv("REDSHIFT_DRIVERNAME")
            host = os.getenv("REDSHIFT_HOST")
            port = int(os.getenv("REDSHIFT_PORT"))
            database = os.getenv("REDSHIFT_DATABASE")
            username = os.getenv("REDSHIFT_USER")
            password = os.getenv("REDSHIFT_PASS")
            
            if file_exist:
                
                url = f"{drivername}://{username}:{password}@{host}:{port}/{database}"
                engine = create_engine(url)

            else:
                print("El archivo .env no se encuentra...")
        
        except Exception as err:
            return err
    
        return engine
    
    
    def run_script_sql(self, script_name: str, conn: object, data=None):
    
        """
            Ejecuta el script en Redshift, dicho script esta ubicado en la carpeta "db".

            Parameters
            ----------
                script_name (str): Obligatorio.
                    Nombre del script que se va ejecutar
                conn (object): Obligatorio.
                    Objeto con la conexion de redshift.
                data: Opcional.
                    Conjunto de datos a usar en la query. 

            Returns
            -------
                result: Resultado de la ejecucion del script
        """
        
        path = PathDir()
        db_folder = path.get_path_to("db")
        
        if conn:
            
            try:
                with open("{}/{}".format(db_folder,script_name), 'r') as query:

                    if data != None:
                        result = conn.execute(text(query.read()), **data)
                    else:
                        result = conn.execute(query.read())
                    
                    
            except Exception as err:
                result = err

        else:
            print("Conexion a Redshift fallida o no se paso la conexion...")
           
        return result
    

    def create_table(self, script_name: str, engine: object):
        
        """
           Funcion que manda a crea la tabla en redshift.

            Parameters
            ----------
            script_name (str): Obligatorio.
                Nombre del script que se va ejecutar
            engine (object): Obligatorio.
                engine: Objeto con el motor de redshift.

            Returns
            -------
                result: Resultado de la ejecucion del script
        """

        table_name = script_name.rsplit('.', 1)[0]
        message = None
        
        try:
            
            if engine:
            
                with engine.connect() as conn:
                    
                    print("Conexion con Redshift: OK")
                    print("Verificando la existencia de tabla {}...".format(table_name))
                    message = ""
                    istable = self.run_script_sql("isTableExist.sql", conn)
                    
                    if istable.fetchone():
                        print("La tabla {} ya fue creada o existe...".format(table_name))
                        message = True
                        return message 
                    else:
                        message = False
                        print("La tabla {} no existe en el esquema, se procede a crearla.".format(table_name) )
                        result = self.run_script_sql(script_name, conn)
            
            else:
                print("El archivo .env no se encuentra...")
                
        except Exception as err:
            message = err
            
        return message            
                
                
    def write_df(self, df: "pd.DataFrame", engine: object):
        
        """
            Funcion que se encarga de guardar el contenido del dataframe a la tabla  
            en redshift, consulta primero si ya estan insertado y guarda los que no existe.
        
            Parameters
            ----------
                df (Dataframe): Obligatorio.
                    Dataframe que contiene los articulos a insertar en la tabla.
                engine (object): Obligatorio.
                    Objeto con el motor de redshift.
                
        """
        
        table_name = "articles"
        schema = os.getenv("REDSHIFT_USER")
        df_columns_list = df.columns.tolist()
        count_rows = 0
        exist_rows = 0
        
        insert_query ="insert into {}.{} ( {} ) values  %s  ".format(schema, 
                                                                     table_name, 
                                                                     ", ".join(df_columns_list))
        
        print("Cantidad de Articulos a guardar: {} ".format(len(df)))
        
        if engine:
            
            with engine.connect() as conn:
                
                cursor = conn.connection.cursor()

                if conn.dialect.has_table(conn, table_name): 
                    
                    print("Consultando la existencia de articulos en Redshift...")
                    for i ,row in df.iterrows():
                        
                        result = self.run_script_sql("beforeQuery.sql", conn, data=row.to_dict())

                        if result.rowcount > 0:
                            count_rows+=1  
                            execute_values(cursor, 
                                          insert_query, 
                                           [tuple(row)])
                        else:
                            exist_rows+=1
                        
                    print("Cantidad de Articulos existentes: {} ".format(exist_rows))
                    print("Cantidad de Articulos insertados: {}".format(count_rows))        
                    conn.connection.commit()
                    cursor.close()  
                
                else:
                    print("La tabla {} aun no se ha creado".format(table_name)) 
        
        else:
            print("El archivo .env no se encuentra...")            
  


    