import pandas as pd
from psycopg2 import Error
from utils import exception_and_time_handler
import time

@exception_and_time_handler
def clean_and_insert_commune(df, cursor):  
    # Convertis num_dept en int, efface les champs érronés  
    df['num_dept'] = pd.to_numeric(df['num_dept'], errors='coerce')
    communes_df = df[['commune_nom', 'num_dept']].dropna().drop_duplicates()
    communes_df = communes_df[communes_df['num_dept'].isin([44, 49, 79, 85])]
    
    # Prépare l'insertion
    data_to_insert = list(communes_df.itertuples(index=False, name=None))
    insert_query = "INSERT INTO Communes (nom, num_dept) VALUES (%s, %s)"
    # Insertion
    cursor.executemany(insert_query, data_to_insert)
    

@exception_and_time_handler
def clean_and_insert_personnes(df, cursor):
    start_time = time.time() # Début de la fonction

    # Colonnes concernant la personne 1 et la personne 2
    columns_p1 = ['nom_p1', 'prenom_p1', 'prenom_pere_p1', 'nom_mere_p1', 'prenom_mere_p1']
    columns_p2 = ['nom_p2', 'prenom_p2', 'prenom_pere_p2', 'nom_mere_p2', 'prenom_mere_p2']
    
    # Début du nettoyage
    clean_start_time = time.time()

    # Regex pour nettoyer les données
    regex_clean = r"[^a-zA-ZéèêëîïôöûüàáâãäåçñóòôõöúùûüýÿÉÈÊËÎÏÔÖÛÜÀÁÂÃÄÅÇÑÓÒÔÕÖÚÙÛÜÝ\s-]"
    df[columns_p1 + columns_p2] = df[columns_p1 + columns_p2].replace(regex_clean, "", regex=True)
    
    # Temps pris pour le nettoyage
    print(f"Temps de nettoyage: {time.time() - clean_start_time} secondes")

    # Préparation des dataframes
    prep_start_time = time.time()

    # Création de dataframes pour les personnes 1 et 2
    personne1_df = df[columns_p1].rename(columns=lambda x: x.replace('_p1', ''))
    personne2_df = df[columns_p2].rename(columns=lambda x: x.replace('_p2', ''))

    # Combinaison des deux puis suppression des doublons
    all_personnes_df = pd.concat([personne1_df, personne2_df]).drop_duplicates().reset_index(drop=True)

    # On fait le choix de supprimer les personnes n'ayant pas de prénom ou de nom renseigné
    all_personnes_df = all_personnes_df[all_personnes_df['nom'].notnull() & all_personnes_df['nom'].str.strip().ne('') & 
                                        all_personnes_df['prenom'].notnull() & all_personnes_df['prenom'].str.strip().ne('')]

    # Temps pris pour la préparation des dataframes
    print(f"Temps de préparation des dataframes: {time.time() - prep_start_time} secondes")

    # Préparation de l'insertion
    insert_start_time = time.time()

    # Prépare l'insertion
    data_to_insert = list(all_personnes_df.itertuples(index=False, name=None))
    insert_query = "INSERT INTO Personnes (nom, prenom, prenom_pere, nom_mere, prenom_mere) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
    
    # Insertion
    if data_to_insert: 
        cursor.executemany(insert_query, data_to_insert)

    # Temps pris pour l'insertion
    print(f"Temps d'insertion: {time.time() - insert_start_time} secondes")

    # Temps total de la fonction
    print(f"Temps total de la fonction: {time.time() - start_time} secondes")


@exception_and_time_handler
def insert_typeacte(cursor):
    # Insertion des élements en brut, cela permet de gagner en simplicité
    elements = ("Certificat de mariage", "Contrat de mariage", "Divorce", "Mariage", "Promesse de mariage - fiançailles", "Publication de mariage", "Rectification de mariage")
    for e in elements:
        cursor.execute("INSERT INTO Type_acte (nom) VALUES (%s)", (e,))

@exception_and_time_handler
def fetch_and_assign_ids(df, cursor):
    # Récupérer les IDs des communes
    cursor.execute("SELECT id, nom FROM Communes")
    communes_ids = cursor.fetchall()
    communes_df = pd.DataFrame(communes_ids, columns=['commune_id', 'commune_nom'])
    # Fusionner pour assigner les IDs des communes
    df = df.merge(communes_df, on='commune_nom', how='left')

    # Récupérer les IDs des personnes pour p1
    cursor.execute("SELECT id, nom, prenom, prenom_pere, nom_mere, prenom_mere FROM Personnes")
    personnes_ids = cursor.fetchall()
    personnes_df = pd.DataFrame(personnes_ids, columns=['personne_id', 'nom', 'prenom', 'prenom_pere', 'nom_mere', 'prenom_mere'])
    # Pour p1
    df = df.merge(personnes_df, left_on=['nom_p1', 'prenom_p1', 'prenom_pere_p1', 'nom_mere_p1', 'prenom_mere_p1'], right_on=['nom', 'prenom', 'prenom_pere', 'nom_mere', 'prenom_mere'], how='left').rename(columns={'personne_id': 'p1_id'}).drop(columns=['nom', 'prenom', 'prenom_pere', 'nom_mere', 'prenom_mere'])
    # Pour p2
    df = df.merge(personnes_df, left_on=['nom_p2', 'prenom_p2', 'prenom_pere_p2', 'nom_mere_p2', 'prenom_mere_p2'], right_on=['nom', 'prenom', 'prenom_pere', 'nom_mere', 'prenom_mere'], how='left').rename(columns={'personne_id': 'p2_id'}).drop(columns=['nom', 'prenom', 'prenom_pere', 'nom_mere', 'prenom_mere'])

    # Récupérer les IDs des types d'acte
    cursor.execute("SELECT id, nom FROM Type_acte")
    type_acte_ids = cursor.fetchall()
    type_acte_df = pd.DataFrame(type_acte_ids, columns=['type_acte_id', 'nom'])
    # Fusionner pour assigner les IDs des types d'acte
    df = df.merge(type_acte_df, left_on='type_acte', right_on='nom', how='left').rename(columns={'type_acte_id': 'type_acte_id'}).drop(columns=['nom'])
    return df

@exception_and_time_handler
def insert_actes(df, cursor):    
    # Filtrage des lignes où des IDs nécessaires ou la date sont manquants
    valid_entries = df.dropna(subset=['type_acte_id', 'commune_id', 'p1_id', 'p2_id'])
    
    # Préparer les données pour l'insertion
    data_to_insert = valid_entries[['type_acte_id', 'p1_id', 'p2_id', 'commune_id', 'date', 'num_vue']].values.tolist()

    # Effectuer l'insertion en masse
    insert_query = """
    INSERT INTO actes (type_acte, p1, p2, commune, date, num_vue) 
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    # Afficher le nombre d'entrées valides
    print(f"Entrées valides pour la table actes: {len(valid_entries)}")
    if len(valid_entries) == 0:
        print("Il y a un problème avec les données, aucune n'est insérable")
        print(df[df.isnull().any(axis=1)].head())
    try:
        cursor.executemany(insert_query, data_to_insert)
        print(f"{len(data_to_insert)} actes prêts pour l'insertion")
        print(f"{cursor.rowcount} actes insérés")
    except Error as e:
        print(f"Erreur à l'insertion dans la table actes: {e}")

