import os
import xml.etree.ElementTree as ET
import pandas as pd

DATA_FOLDER = '../data'


# Función para procesar un archivo XML y convertirlo en un DataFrame
def process_xml_file(filepath):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()

        articles = []
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            # Procesamiento de autores
            authors = ', '.join([author.find('{http://www.w3.org/2005/Atom}name').text for author in
                                 entry.findall('{http://www.w3.org/2005/Atom}author')])

            # Procesamiento de categorías
            primary_category = entry.find('{http://arxiv.org/schemas/atom}primary_category').attrib['term']
            categories = ', '.join(
                [cat.attrib['term'] for cat in entry.findall('{http://www.w3.org/2005/Atom}category') if
                 cat.attrib['term'] != primary_category])

            article = {
                'id': entry.find('{http://www.w3.org/2005/Atom}id').text,
                'title': entry.find('{http://www.w3.org/2005/Atom}title').text,
                'summary': entry.find('{http://www.w3.org/2005/Atom}summary').text,
                'published': entry.find('{http://www.w3.org/2005/Atom}published').text,
                'updated': entry.find('{http://www.w3.org/2005/Atom}updated').text,
                'authors': authors,
                'primary_category': primary_category,
                'categories': categories,
            }
            articles.append(article)

        return pd.DataFrame(articles)

    except ET.ParseError as e:
        print(f"Error al parsear el archivo XML {filepath}: {e}")
        return pd.DataFrame()  # Devolver un DataFrame vacío en caso de error


# Función para procesar todos los archivos en la carpeta y concatenar los DataFrames
def process_all_files(folder_path):
    all_articles = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".xml"):
            filepath = os.path.join(folder_path, filename)
            articles = process_xml_file(filepath)
            all_articles.append(articles)

    return pd.concat(all_articles, ignore_index=True)


# Función principal
def transform_data():
    # Procesar todos los archivos XML en la carpeta "data"
    all_articles = process_all_files(DATA_FOLDER)

    # Mostrar el DataFrame y guardarlo en un archivo CSV
    pd.set_option('display.max_columns', None)
    print(all_articles.head())

    all_articles.to_csv('./data/arxiv_articles.csv', index=False)


# Ejecutar la transformación
if __name__ == "__main__":
    transform_data()
