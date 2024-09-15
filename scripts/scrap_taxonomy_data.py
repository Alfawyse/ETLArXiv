import requests
from bs4 import BeautifulSoup
import psycopg2
from config.settings import DATABASE

# Función para conectarse a la base de datos
def connect_db():
    conn = psycopg2.connect(
        dbname=DATABASE['dbname'],
        user=DATABASE['user'],
        password=DATABASE['password'],
        host=DATABASE['host'],
        port=DATABASE['port']
    )
    return conn

# Función para insertar datos en la base de datos
def insert_data_to_db(main_category, subcategory_code, abbreviation_meaning, description):
    try:
        conn = connect_db()
        cursor = conn.cursor()

        # Verificar si la categoría principal ya existe
        cursor.execute('SELECT id FROM "ArXiv"."research_main_categories" WHERE name = %s', (main_category,))
        main_category_id = cursor.fetchone()

        if main_category_id is None:
            # Insertar la nueva categoría principal
            cursor.execute(
                'INSERT INTO "ArXiv"."research_main_categories" (name) VALUES (%s) RETURNING id',
                (main_category,)
            )
            main_category_id = cursor.fetchone()[0]
        else:
            main_category_id = main_category_id[0]

        # Verificar si la subcategoría ya existe
        cursor.execute('SELECT id FROM "ArXiv"."research_subcategories" WHERE category_code = %s', (subcategory_code,))
        subcategory_id = cursor.fetchone()

        if subcategory_id is None:
            # Insertar la subcategoría
            cursor.execute(
                """
                INSERT INTO "ArXiv"."research_subcategories" (category_code, abbreviation_meaning, description, main_category_id)
                VALUES (%s, %s, %s, %s)
                """,
                (subcategory_code, abbreviation_meaning, description, main_category_id)
            )

        # Confirmar los cambios
        conn.commit()

    except Exception as e:
        print(f"Error al procesar los datos: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

# Función para hacer scraping de la página
def scrape_and_insert_data():
    # URL de la página que deseas hacer scraping
    url = "https://arxiv.org/category_taxonomy"

    # Hacer una solicitud GET a la página
    response = requests.get(url)

    # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        # Parsear el contenido HTML de la página
        soup = BeautifulSoup(response.text, 'html.parser')

        # Encontrar el div con el id "category_taxonomy_list"
        taxonomy_div = soup.find('div', id='category_taxonomy_list')

        if taxonomy_div:
            print("Div 'category_taxonomy_list' encontrado.")

            # Listas para almacenar los datos
            data = []

            # Encontrar todas las secciones de categorías dentro del div (h2 con clase accordion-head)
            secciones = taxonomy_div.find_all('h2', class_='accordion-head')

            for seccion in secciones:
                # Obtener la categoría principal
                categoria_text = seccion.text.strip()

                # Obtener la sección de la categoría (accordion-body) asociada
                accordion_body = seccion.find_next_sibling('div', class_='accordion-body')

                if accordion_body:
                    # En algunos casos hay un h3 para una subcategoría más amplia (como en Physics)
                    h3_sections = accordion_body.find_all('div', class_='columns')

                    for h3_section in h3_sections:
                        # Si hay un h3 en la sección actual, obtenemos su texto (por ejemplo, "Astrophysics (astro-ph)")
                        h3 = h3_section.find('h3')
                        subcategoria_h3_text = h3.text.strip() if h3 else ''

                        # Recorrer las subcategorías y descripciones dentro de esta sección
                        subcat_divs = h3_section.find_all('div', class_='columns divided')

                        for subcat_div in subcat_divs:
                            # Buscar la subcategoría (h4) y su descripción (p)
                            subcat = subcat_div.find('h4')
                            if subcat:
                                subcategoria_text = subcat.text.strip()
                                description = subcat_div.find('p')
                                if description:
                                    description_text = description.text.strip()

                                    # Agregar los datos a la lista y guardarlos en la base de datos
                                    subcategory_code = subcategoria_text.split(' ')[0]
                                    abbreviation_meaning = ' '.join(subcategoria_text.split(' ')[1:])

                                    # Insertar en la base de datos
                                    insert_data_to_db(categoria_text, subcategory_code, abbreviation_meaning, description_text)

            print("Datos insertados correctamente en la base de datos.")
        else:
            print("No se encontró el div con id 'category_taxonomy_list'.")

    else:
        print(f"Error al hacer scraping de la página. Status code: {response.status_code}")

# Ejecutar el proceso de scraping e inserción
if __name__ == '__main__':
    scrape_and_insert_data()
