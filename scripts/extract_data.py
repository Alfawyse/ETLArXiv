import aiohttp
import os
import asyncio

BASE_URL = "http://export.arxiv.org/api/query"


# Función para hacer la consulta
async def fetch_data(session, query, start, max_results=100):
    url = f"{BASE_URL}?search_query=all:{query}&start={start}&max_results={max_results}"
    async with session.get(url) as response:
        return await response.text()


# Función para guardar los datos en un solo archivo
def save_xml(data, filename):
    save_path = "../data"  # Ruta fuera de la carpeta del código
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    with open(os.path.join(save_path, filename), "w", encoding="utf-8") as file:
        file.write(data)


# Función para extraer y guardar datos con paginación
async def extract_and_save_data(queries, total_results=300, max_results_per_query=100):
    async with aiohttp.ClientSession() as session:
        for query in queries:
            accumulated_data = ""  # Variable para acumular los datos
            results_to_fetch = total_results
            start = 0
            while results_to_fetch > 0:
                # Obtener resultados con el límite de max_results por consulta
                results_to_get = min(max_results_per_query, results_to_fetch)
                xml_data = await fetch_data(session, query, start, results_to_get)

                # Acumular los resultados
                accumulated_data += xml_data

                # Actualizar los contadores para la paginación
                results_to_fetch -= results_to_get
                start += results_to_get

            # Guardar el archivo acumulado
            filename = f"results_{query.replace(' ', '_')}.xml"
            save_xml(accumulated_data, filename)


# Main
async def main():
    queries = [
        "Computer Science",
        "Economics",
        "Electrical Engineering and Systems Science",
        "Mathematics",
        "Physics",
        "Quantitative Biology",
        "Quantitative Finance",
        "Statistics"
    ]
    await extract_and_save_data(queries, total_results=300)


if __name__ == "__main__":
    asyncio.run(main())

