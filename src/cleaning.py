from utils import exception_and_time_handler
import pandas as pd
import numpy as np

@exception_and_time_handler
def read_and_preprocess(file_path):
    # Ititialisation de la dataframe df, les données sont dans la RAM et sont donc plus rapide à traiter
    df = pd.read_csv(file_path, dtype=str)
    df.columns = ["id", "type_acte", "nom_p1", "prenom_p1", "prenom_pere_p1", "nom_mere_p1", "prenom_mere_p1",
                    "nom_p2", "prenom_p2", "prenom_pere_p2", "nom_mere_p2", "prenom_mere_p2", 
                    "commune_nom", "num_dept", "date", "num_vue"]
    df.replace({np.nan: None}, inplace=True)
    
    # Gestion des valeurs de date qui sont non-standard
    df = clean_date(df)    
    return df

def clean_date(df):
    # Convertis la colonne date en datetime, et spécifier le format
    df['clean_date'] = pd.to_datetime(df['date'], errors='coerce', format='%d/%m/%Y')
    
    # Remplace les éléments vide pour faciliter l'insertion
    df['date'] = df['clean_date'].dt.date.astype('str').replace('NaT', '01/01/0001')
    
    # Supprime la colonne clean_date car elle n'est plus nécessaire
    df.drop(columns=['clean_date'], inplace=True)
    return df

