import pandas as pd
import psycopg2
from config.settings import DATABASE  # Importar las credenciales de settings.py

# Función para conectarse a la base de datos
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DATABASE['dbname'],
            user=DATABASE['user'],
            password=DATABASE['password'],
            host=DATABASE['host'],
            port=DATABASE['port']
        )
        return conn
    except Exception as error:
        print(f"Error al conectar a la base de datos: {error}")
        return None

# Función para insertar los artículos y sus relaciones
def insert_article_and_associations(article_data):
    conn = connect_db()
    cursor = conn.cursor()
    print(article_data)

    try:
        # Verificar si el artículo ya existe
        cursor.execute('SELECT id FROM "ArXiv"."research_articles" WHERE arxiv_id = %s', (article_data['arxiv_id'],))
        article_id = cursor.fetchone()

        if article_id is None:
            # Insertar el artículo
            cursor.execute(
                """
                INSERT INTO "ArXiv"."research_articles" (arxiv_id, title, summary, published, updated, html_url, pdf_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (
                    article_data['arxiv_id'], article_data['title'], article_data['summary'],
                    article_data['published'], article_data['updated'],
                    article_data['html_url'], article_data['pdf_url']
                )
            )
            article_id = cursor.fetchone()[0]
        else:
            article_id = article_id[0]

        # Dividir autores, afiliaciones y países
        author_names = article_data['authors'].split(', ')
        author_affiliations = article_data['affiliations'].split(', ') if pd.notna(article_data['affiliations']) else []
        author_countries = article_data['countries'].split(', ') if pd.notna(article_data['countries']) else []

        # Insertar autores y la asociación artículo-autor
        for i, author_name in enumerate(author_names):
            cursor.execute('SELECT id FROM "ArXiv"."research_authors" WHERE name = %s', (author_name,))
            author_id = cursor.fetchone()

            if author_id is None:
                # Insertar el autor
                cursor.execute(
                    'INSERT INTO "ArXiv"."research_authors" (name) VALUES (%s) RETURNING id',
                    (author_name,)
                )
                author_id = cursor.fetchone()[0]
            else:
                author_id = author_id[0]

            # Relacionar artículo con autor
            cursor.execute(
                'INSERT INTO "ArXiv"."article_author_associations" (article_id, author_id) VALUES (%s, %s)',
                (article_id, author_id)
            )

            # Relacionar autor con las categorías adicionales (author_category_associations)
            if pd.notna(article_data['primary_category']) and article_data['categories'] != '':
                for category_code in article_data['primary_category'].split(', '):
                    cursor.execute('SELECT id FROM "ArXiv"."research_subcategories" WHERE category_code = %s', (category_code,))
                    category_id = cursor.fetchone()

                    if category_id is not None:
                        category_id = category_id[0]

                        # Relacionar autor con la categoría
                        cursor.execute(
                            'INSERT INTO "ArXiv"."author_category_associations" (author_id, category_id) VALUES (%s, %s)',
                            (author_id, category_id)
                        )

            # Asignar afiliación e institución por autor si están presentes
            if i < len(author_affiliations):
                affiliation = author_affiliations[i].strip()
                cursor.execute('SELECT id FROM "ArXiv"."research_institutions" WHERE name = %s', (affiliation,))
                institution_id = cursor.fetchone()

                if institution_id is None:
                    # Insertar la institución
                    cursor.execute(
                        'INSERT INTO "ArXiv"."research_institutions" (name) VALUES (%s) RETURNING id',
                        (affiliation,)
                    )
                    institution_id = cursor.fetchone()[0]
                else:
                    institution_id = institution_id[0]

                # Relacionar autor con institución
                cursor.execute(
                    'INSERT INTO "ArXiv"."author_institution_associations" (author_id, institution_id) VALUES (%s, %s)',
                    (author_id, institution_id)
                )

        # Insertar categorías adicionales y asociaciones, asegurando que las categorías no estén vacías
        if pd.notna(article_data['primary_category']) and article_data['categories'] != '':
            for category_code in article_data['primary_category'].split(', '):
                cursor.execute('SELECT id FROM "ArXiv"."research_subcategories" WHERE category_code = %s', (category_code,))
                category_id = cursor.fetchone()

                if category_id is not None:
                    category_id = category_id[0]

                    # Relacionar artículo con la categoría
                    cursor.execute(
                        'INSERT INTO "ArXiv"."article_category_associations" (article_id, category_id) VALUES (%s, %s)',
                        (article_id, category_id)
                    )

        conn.commit()  # Confirmar los cambios
    except Exception as error:
        print(f"Error al insertar el artículo: {error}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Función para cargar los datos desde el DataFrame
def load_data_to_db(dataframe):
    # Eliminar espacios en blanco de los nombres de las columnas
    dataframe.columns = dataframe.columns.str.strip()

    for i, row in dataframe.iterrows():
        insert_article_and_associations(roww)

# Función principal para cargar los datos
def main():
    # Cargar el DataFrame desde el archivo CSV
    dataframe = pd.read_csv('../data/arxiv_articles.csv')  # Cambia la ruta si es necesario

    # Cargar los datos en la base de datos
    load_data_to_db(dataframe)

if __name__ == "__main__":
    main()



