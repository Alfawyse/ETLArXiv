import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import pandas as pd

# Asegurarse de que pandas muestre todas las columnas cuando se imprime el DataFrame
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


# Función asíncrona para extraer datos de ArXiv
async def fetch_arxiv_data(session, query="Physics", start=0, max_results=10):
    url = f"http://export.arxiv.org/api/query?search_query=all:{query}&start={start}&max_results={max_results}"
    async with session.get(url) as response:
        if response.status == 200:
            return await response.text()  # Devolvemos el texto del contenido XML
        else:
            raise Exception(f"Error al obtener datos de ArXiv: {response.status}")


# Función para parsear el XML y extraer los artículos
def parse_arxiv_data(xml_data):
    root = ET.fromstring(xml_data)
    articles = []

    for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
        # Inicializar una lista para los autores y categorías
        authors = []
        categories = []

        # Extraer autores y afiliaciones
        for author in entry.findall('{http://www.w3.org/2005/Atom}author'):
            author_info = {
                'name': author.find('{http://www.w3.org/2005/Atom}name').text,
                'affiliation': None,
                'country': None
            }
            affiliation = author.find('{http://arxiv.org/schemas/atom}affiliation')
            if affiliation is not None:
                author_info['affiliation'] = affiliation.text
                affiliation_parts = affiliation.text.split(', ')
                if len(affiliation_parts) > 1:
                    author_info['country'] = affiliation_parts[-1]

            authors.append(author_info)

        # Extraer la categoría principal
        primary_category = entry.find('{http://arxiv.org/schemas/atom}primary_category').attrib['term']

        # Extraer categorías adicionales, asegurándonos de que la categoría principal no esté repetida
        for category in entry.findall('{http://www.w3.org/2005/Atom}category'):
            category_term = category.attrib['term']
            if category_term != primary_category:
                categories.append(category_term)

        # Construir el artículo
        article = {
            'id': entry.find('{http://www.w3.org/2005/Atom}id').text,
            'title': entry.find('{http://www.w3.org/2005/Atom}title').text,
            'summary': entry.find('{http://www.w3.org/2005/Atom}summary').text,
            'published': entry.find('{http://www.w3.org/2005/Atom}published').text,
            'updated': entry.find('{http://www.w3.org/2005/Atom}updated').text,
            'html_url': entry.find('{http://www.w3.org/2005/Atom}link[@rel="alternate"]').attrib['href'],
            'pdf_url': entry.find('{http://www.w3.org/2005/Atom}link[@title="pdf"]').attrib['href'] if entry.find(
                '{http://www.w3.org/2005/Atom}link[@title="pdf"]') is not None else None,
            'authors': authors,  # Lista de autores
            'primary_category': primary_category,  # Categoría principal
            'categories': categories  # Lista de categorías adicionales
        }

        articles.append(article)

    return articles


# Función asíncrona para ejecutar la extracción de varios lotes
async def extract_multiple_arxiv_data(queries, max_results_per_query=10):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_arxiv_data(session, query=q, max_results=max_results_per_query) for q in queries]
        responses = await asyncio.gather(*tasks)

        all_articles = []
        for response in responses:
            all_articles.extend(parse_arxiv_data(response))

        return all_articles


# Función para convertir los artículos en un DataFrame
def create_dataframe(articles):
    data = []

    for article in articles:
        # Crear una cadena de texto con todos los nombres de autores separados por comas
        authors = ', '.join([author['name'] for author in article['authors']])

        # Crear una cadena de texto con las afiliaciones, separadas por comas
        affiliations = ', '.join(
            [author['affiliation'] if author['affiliation'] else 'Unknown' for author in article['authors']])

        # Crear una cadena de texto con los países, separadas por comas
        countries = ', '.join([author['country'] if author['country'] else 'Unknown' for author in article['authors']])

        # Crear una cadena de texto con las categorías adicionales, separadas por comas
        categories = ', '.join(article['categories'])

        # Agregar una única fila por artículo
        data.append({
            'Article ID': article['id'],
            'Title': article['title'],
            'Summary': article['summary'],
            'Published': article['published'],
            'Updated': article['updated'],
            'HTML URL': article['html_url'],
            'PDF URL': article['pdf_url'],
            'Primary Category': article['primary_category'],  # Categoría principal
            'Additional Categories': categories,  # Categorías adicionales sin la principal
            'Authors': authors,  # Todos los autores en una sola celda
            'Affiliations': affiliations,  # Todas las afiliaciones en una sola celda
            'Countries': countries  # Todos los países en una sola celda
        })

    return pd.DataFrame(data)


# Función principal para ejecutar el ETL
async def main():
    queries = ["Physics", "Computer Science", "Mathematics"]
    articles = await extract_multiple_arxiv_data(queries, max_results_per_query=5)
    df = create_dataframe(articles)

    print(df)  # Mostrar el DataFrame completo con todas las columnas
    df.to_csv('arxiv_articles.csv', index=False)  # Guardar en un archivo CSV


# Ejecutar el ETL
if __name__ == "__main__":
    asyncio.run(main())
