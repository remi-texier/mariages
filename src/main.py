import psycopg2
import cleaning
import insert
from utils import exception_and_time_handler

@exception_and_time_handler
def main():
    # Lecture et prétraitement des données à partir du csv
    df = cleaning.read_and_preprocess("Data/mariages_L3.csv")
    if df is not None:
        # connexion à la base PostgreSQL
        connection = psycopg2.connect("dbname=mariages user=postgres password=admin")
        cursor = connection.cursor()
        
        # Nettoyage et insertion des données dans la base
        insert.clean_and_insert_commune(df, cursor)
        insert.clean_and_insert_personnes(df, cursor)
        insert.insert_typeacte(cursor)
        
        # Récupération des IDs nécessaires
        df = insert.fetch_and_assign_ids(df, cursor)
        # Insertion des actes dans la base
        insert.insert_actes(df, cursor)
        
        connection.commit()
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()
