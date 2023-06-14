import requests as r
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

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


def get_data(url: str, args=None):
    
    """
        Realiza la solucitud de la api y devuelve los articulos mas populares segun
        la caterigoria  y la fecha.

        Parameters
        ----------
        url: str, Obligatorio.
            Endpoint de la api
        args: Opcional.
            Son los prametros a utilizar para filtrar la solicitud.
        
        Returns
        -------
            msg: 
                Lista de los articulos 
                
            NOTA: Es necesario pasar por el parametro "args" la clave de autenticacion 
                  de la api (apikey). 
            
    """
    
    response = r.get(url, params=args)
    
    try: 
        msg = response.json()
        
        if msg["status"] == "ok" and msg["totalResults"] > 0: 
            
            print("Se encontraron {} articulos".format(msg["totalResults"]))
            msg = msg["articles"]
            
        elif msg["status"] == "ok" and msg["totalResults"] == 0:
            msg= print("No se encontraron Articulos")    
               
        else: 
            msg = msg["message"]
            
    except Exception as err:
        return err
        
    return msg


def normalize_data(data: list):
    """_summary_

    Parameters
    ----------
        data (list), Obligatorio.
            Lista de los articulos encontrados por la api.

    Returns
    -------
        df:  Contenido los datos normalizados en dataframe
    """
    
    data_list = []
    
    for row in data:
        
        template = {
            "id": row["source"]["id"],
            "name": row["source"]["name"],
            "author": row["author"],
            "title": row["title"],
            "description": row["description"],
            "url": row["url"],
            "urlToImage": row["urlToImage"],
            "publishedAt": row["publishedAt"].rsplit('T', 1)[0],
            "content": row["content"]
        }
        
        data_list.append(template)
    
    df = pd.DataFrame(data_list)
    df["publishedAt"] =pd.to_datetime(df["publishedAt"]) 
    
    return df
    