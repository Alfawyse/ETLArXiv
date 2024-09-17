import aiohttp
import os
import asyncio

BASE_URL = "http://export.arxiv.org/api/query"


async def fetch_data(session, query, start, max_results=100):
    """
    Fetches data from the ArXiv API using the given query parameters.

    Args:
        session (aiohttp.ClientSession): The session object used for HTTP requests.
        query (str): The search query for the API request.
        start (int): The starting point for pagination.
        max_results (int, optional): The maximum number of results to fetch. Defaults to 100.

    Returns:
        str: The XML response data from the API.
    """
    url = f"{BASE_URL}?search_query=all:{query}&start={start}&max_results={max_results}"
    async with session.get(url) as response:
        return await response.text()


def save_xml(data, filename):
    """
    Saves the given XML data to a file in the specified directory.

    Args:
        data (str): The XML data to save.
        filename (str): The name of the file to save the data in.
    """
    save_path = "../data"  # Path outside the code folder
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    with open(os.path.join(save_path, filename), "w", encoding="utf-8") as file:
        file.write(data)


async def extract_and_save_data(queries, total_results=300, max_results_per_query=100):
    """
    Extracts data from the ArXiv API for multiple queries, handling pagination,
    and saves the results as XML files.

    Args:
        queries (list): A list of search queries.
        total_results (int, optional): The total number of results to fetch per query. Defaults to 300.
        max_results_per_query (int, optional): The maximum number of results per API call. Defaults to 100.
    """
    async with aiohttp.ClientSession() as session:
        for query in queries:
            accumulated_data = ""
            results_to_fetch = total_results
            start = 0

            while results_to_fetch > 0:
                results_to_get = min(max_results_per_query, results_to_fetch)
                xml_data = await fetch_data(session, query, start, results_to_get)

                if start == 0:
                    accumulated_data = xml_data.split("</feed>")[0]
                else:
                    accumulated_data += xml_data.split("<feed")[1].split("</feed>")[0]

                results_to_fetch -= results_to_get
                start += results_to_get

            accumulated_data += "</feed>"

            filename = f"results_{query.replace(' ', '_')}.xml"
            save_xml(accumulated_data, filename)


async def main():
    """
    Main function to extract and save ArXiv data for predefined queries.
    """
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


