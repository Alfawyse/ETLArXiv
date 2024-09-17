import requests
from bs4 import BeautifulSoup
import psycopg2
from config.settings import DATABASE


def connect_db():
    """
    Establishes a connection to the PostgreSQL database using the credentials
    defined in settings.py.

    Returns:
        psycopg2.connection: A connection object to interact with the PostgreSQL database.
    """
    conn = psycopg2.connect(
        dbname=DATABASE['dbname'],
        user=DATABASE['user'],
        password=DATABASE['password'],
        host=DATABASE['host'],
        port=DATABASE['port']
    )
    return conn


def insert_data_to_db(main_category, subcategory_code, abbreviation_meaning, description):
    """
    Inserts main category and subcategory information into the PostgreSQL database.

    Args:
        main_category (str): The main research category name.
        subcategory_code (str): The code for the research subcategory.
        abbreviation_meaning (str): The full meaning of the subcategory abbreviation.
        description (str): A description of the research subcategory.
    """
    try:
        conn = connect_db()
        cursor = conn.cursor()

        # Check if the main category already exists
        cursor.execute('SELECT id FROM "ArXiv"."research_main_categories" WHERE name = %s', (main_category,))
        main_category_id = cursor.fetchone()

        if main_category_id is None:
            # Insert the new main category
            cursor.execute(
                'INSERT INTO "ArXiv"."research_main_categories" (name) VALUES (%s) RETURNING id',
                (main_category,)
            )
            main_category_id = cursor.fetchone()[0]
        else:
            main_category_id = main_category_id[0]

        # Check if the subcategory already exists
        cursor.execute('SELECT id FROM "ArXiv"."research_subcategories" WHERE category_code = %s', (subcategory_code,))
        subcategory_id = cursor.fetchone()

        if subcategory_id is None:
            # Insert the new subcategory
            cursor.execute(
                """
                INSERT INTO "ArXiv"."research_subcategories" (category_code, abbreviation_meaning, description, main_category_id)
                VALUES (%s, %s, %s, %s)
                """,
                (subcategory_code, abbreviation_meaning, description, main_category_id)
            )

        # Commit the changes
        conn.commit()

    except Exception as e:
        print(f"Error processing data: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


def scrape_and_insert_data():
    """
    Scrapes the ArXiv category taxonomy page and inserts the extracted data into
    the PostgreSQL database.
    """
    url = "https://arxiv.org/category_taxonomy"

    # Make a GET request to fetch the page
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        taxonomy_div = soup.find('div', id='category_taxonomy_list')

        if taxonomy_div:
            print("Div 'category_taxonomy_list' found.")

            # Process each category section
            secciones = taxonomy_div.find_all('h2', class_='accordion-head')

            for seccion in secciones:
                main_category = seccion.text.strip()

                accordion_body = seccion.find_next_sibling('div', class_='accordion-body')

                if accordion_body:
                    h3_sections = accordion_body.find_all('div', class_='columns')

                    for h3_section in h3_sections:
                        h3 = h3_section.find('h3')
                        subcategory_h3_text = h3.text.strip() if h3 else ''

                        subcat_divs = h3_section.find_all('div', class_='columns divided')

                        for subcat_div in subcat_divs:
                            subcat = subcat_div.find('h4')
                            if subcat:
                                subcategory_text = subcat.text.strip()
                                description = subcat_div.find('p')
                                if description:
                                    description_text = description.text.strip()

                                    subcategory_code = subcategory_text.split(' ')[0]
                                    abbreviation_meaning = ' '.join(subcategory_text.split(' ')[1:])

                                    # Insert data into the database
                                    insert_data_to_db(main_category, subcategory_code, abbreviation_meaning, description_text)

            print("Data successfully inserted into the database.")
        else:
            print("Div with id 'category_taxonomy_list' not found.")

    else:
        print(f"Error scraping the page. Status code: {response.status_code}")


if __name__ == '__main__':
    scrape_and_insert_data()

