import requests as r
import pandas as pd


class Articles ():
    
    """
        Clase encargada de gestion de los Articulos.
    
    """

    def call_api(self, url: str, args=None):
        
        """
            Realiza la solucitud de la api y devuelve los articulos mas populares segun
            la categoria  y la fecha.

            Parameters
            ----------
            url: str, Obligatorio.
                Endpoint de la api
            args: Obligatorio.
                Son los prametros a utilizar para filtrar la solicitud.
            
            Returns
            -------
                result: 
                    Lista de los articulos 
                    
                NOTA: Es necesario pasar por el parametro "args" la clave de autenticacion 
                    de la api (apikey). 
                
        """
        
        response = r.get(url, params=args)
        
        try: 
            result = response.json()
            
            if result["status"] == "ok" and result["totalResults"] > 0: 
                
                print("Cantidad de Articulos obtenidos: {}".format(result["totalResults"]))
                result = result["articles"]
                
                
            elif result["status"] == "ok" and result["totalResults"] == 0:
                print("No se encontraron Articulos")    
                
            else: 
                result = result["message"]
                
        except Exception as err:
            result = err
            
        return result


    def normalize_data(self, data: list):

        """
            Funcion que aplica el tratamiento corresponde en la informacion y 
            la devuelde en formato de dataframe

        Parameters
        ----------
            data (list), Obligatorio.
                Lista de los articulos encontrados por la api.

        Returns
        -------
            df:  Contenido los datos normalizados en dataframe
        """

        data_list = []

        if data: 
            
            print("Aplicando transformaciones y ajustes...")
            
            for index, row in enumerate(data):

                template = {
                    "id":int(index +1),
                    "idSource": row["source"]["id"],
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
        
        else:
            print("No se pasaron los articulos...")
            
        return df
