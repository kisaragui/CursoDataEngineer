import requests as r
import pandas as pd


class Articles():
    
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
                result = {"result": result["articles"]}
                
            elif result["status"] == "ok" and result["totalResults"] == 0:
                print("No se encontraron Articulos")    
                
            else: 
                result = result["message"]
                
        except Exception as err:
            result = err
            
        return result


    def normalize_data(self, data: dict):

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

        if data: 
            
            print("Aplicando transformaciones y ajustes...")
            
            temp = pd.DataFrame(data["result"])
            df_sourse = temp.from_records(temp["source"])
            temp["publishedAt"] = temp["publishedAt"].apply(lambda x : x.rsplit('T', 1)[0]) 
            temp.insert(0,"id", temp.reset_index().index +1)
            temp.insert(1,"idSource", df_sourse["id"])
            temp.insert(2,"name", df_sourse["name"])
            df = temp.drop(columns="source")
            
        else:
            print("No se pasaron los articulos...")
            
        return df
