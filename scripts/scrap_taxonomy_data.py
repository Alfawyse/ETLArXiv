import requests
from bs4 import BeautifulSoup
import pandas as pd

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
                    if h3:
                        subcategoria_h3_text = h3.text.strip()

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

                                # Agregar los datos a la lista, respetando la estructura con h3 si existe
                                if h3:
                                    data.append({
                                        'Categoría': categoria_text,
                                        'Subclasificación': subcategoria_h3_text,
                                        'Subcategoría': subcategoria_text,
                                        'Descripción': description_text
                                    })
                                else:
                                    data.append({
                                        'Categoría': categoria_text,
                                        'Subclasificación': '',
                                        'Subcategoría': subcategoria_text,
                                        'Descripción': description_text
                                    })

        # Crear el DataFrame con los datos
        if data:
            df = pd.DataFrame(data)
            # Mostrar el DataFrame en pantalla
            print(df)
            # Guardar el DataFrame en un archivo CSV
            df.to_csv('../data/arxiv_categories_with_descriptions.csv', index=False)
        else:
            print("No se encontró ningún dato para las categorías, subcategorías y descripciones.")
    else:
        print("No se encontró el div con id 'category_taxonomy_list'.")

else:
    print(f"Error al hacer scraping de la página. Status code: {response.status_code}")
