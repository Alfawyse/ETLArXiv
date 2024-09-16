import os
import xml.etree.ElementTree as ET
import pandas as pd

# Ruta donde se guardan los archivos XML
DATA_FOLDER = "../data"

# Función para procesar un archivo XML
def process_xml_file(filepath):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()

        articles = []
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            # Inicializar listas para los autores, categorías y afiliaciones
            authors = []
            affiliations = []
            countries = []  # Vacío según indicación
            categories = []

            # Extraer autores y posibles afiliaciones
            for author in entry.findall('{http://www.w3.org/2005/Atom}author'):
                author_name = author.find('{http://www.w3.org/2005/Atom}name').text
                authors.append(author_name)

                # Extraer la afiliación (institución) si existe
                affiliation = author.find('{http://arxiv.org/schemas/atom}affiliation')
                if affiliation is not None:
                    affiliations.append(affiliation.text)
                else:
                    affiliations.append('Unknown')

            # Extraer la categoría principal
            primary_category = entry.find('{http://arxiv.org/schemas/atom}primary_category').attrib['term']

            # Extraer categorías adicionales
            for category in entry.findall('{http://www.w3.org/2005/Atom}category'):
                category_term = category.attrib['term']
                if category_term != primary_category:
                    categories.append(category_term)

            # Extraer los enlaces HTML y PDF
            html_url = entry.find('{http://www.w3.org/2005/Atom}link[@rel="alternate"]').attrib['href']
            pdf_link_elem = entry.find('{http://www.w3.org/2005/Atom}link[@title="pdf"]')
            pdf_url = pdf_link_elem.attrib['href'] if pdf_link_elem is not None else None

            # Construir el artículo
            article = {
                'arxiv_id': entry.find('{http://www.w3.org/2005/Atom}id').text,
                'title': entry.find('{http://www.w3.org/2005/Atom}title').text,
                'summary': entry.find('{http://www.w3.org/2005/Atom}summary').text,
                'published': entry.find('{http://www.w3.org/2005/Atom}published').text,
                'updated': entry.find('{http://www.w3.org/2005/Atom}updated').text,
                'html_url': html_url,  # Enlace HTML
                'pdf_url': pdf_url,  # Enlace PDF (si existe)
                'authors': ', '.join(authors),  # Lista de autores
                'affiliations': ', '.join(affiliations),  # Lista de afiliaciones
                'countries': '',  # Se deja vacío
                'primary_category': primary_category,  # Categoría principal
                'categories': ', '.join(categories)  # Lista de categorías adicionales
            }

            articles.append(article)
        return articles
    except Exception as e:
        print(f"Error al parsear el archivo XML {filepath}: {e}")
        return []

# Función para procesar todos los archivos XML en la carpeta de datos
def process_all_files(data_folder):
    all_articles = []
    for filename in os.listdir(data_folder):
        if filename.endswith(".xml"):
            filepath = os.path.join(data_folder, filename)
            articles = process_xml_file(filepath)
            all_articles.extend(articles)
    return all_articles

# Función para convertir los artículos en un DataFrame
def create_dataframe(articles):
    return pd.DataFrame(articles)

# Función principal para ejecutar la transformación
def transform_data():
    all_articles = process_all_files(DATA_FOLDER)
    df = create_dataframe(all_articles)

    # Mostrar el DataFrame completo con todas las columnas
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df)

    # Guardar el DataFrame en un archivo CSV
    df.to_csv(os.path.join(DATA_FOLDER, 'arxiv_articles.csv'), index=False)

if __name__ == "__main__":
    transform_data()
