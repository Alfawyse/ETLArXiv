import pandas as pd
import psycopg2
from config.settings import DATABASE  # Import database credentials from settings.py


def connect_db():
    """
    Establishes a connection to the PostgreSQL database using the credentials
    defined in settings.py.

    Returns:
        psycopg2.connection: A connection object to interact with the PostgreSQL database.
    """
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
        print(f"Error connecting to the database: {error}")
        return None


def insert_article_and_associations(article_data):
    """
    Inserts an article and its associated data (authors, categories, institutions)
    into the database.

    Args:
        article_data (dict): A dictionary containing article information.
    """
    conn = connect_db()
    cursor = conn.cursor()
    print(article_data)

    try:
        # Check if the article already exists
        cursor.execute('SELECT id FROM "ArXiv"."research_articles" WHERE arxiv_id = %s', (article_data['arxiv_id'],))
        article_id = cursor.fetchone()

        if article_id is None:
            # Insert the article
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

        # Split authors, affiliations, and countries
        author_names = article_data['authors'].split(', ')
        author_affiliations = article_data['affiliations'].split(', ') if pd.notna(article_data['affiliations']) else []
        author_countries = article_data['countries'].split(', ') if pd.notna(article_data['countries']) else []

        # Insert authors and article-author associations
        for i, author_name in enumerate(author_names):
            cursor.execute('SELECT id FROM "ArXiv"."research_authors" WHERE name = %s', (author_name,))
            author_id = cursor.fetchone()

            if author_id is None:
                # Insert author
                cursor.execute(
                    'INSERT INTO "ArXiv"."research_authors" (name) VALUES (%s) RETURNING id',
                    (author_name,)
                )
                author_id = cursor.fetchone()[0]
            else:
                author_id = author_id[0]

            # Associate article with author
            cursor.execute(
                'INSERT INTO "ArXiv"."article_author_associations" (article_id, author_id) VALUES (%s, %s)',
                (article_id, author_id)
            )

            # Associate author with additional categories (author_category_associations)
            if pd.notna(article_data['primary_category']) and article_data['categories'] != '':
                for category_code in article_data['primary_category'].split(', '):
                    cursor.execute('SELECT id FROM "ArXiv"."research_subcategories" WHERE category_code = %s', (category_code,))
                    category_id = cursor.fetchone()

                    if category_id is not None:
                        category_id = category_id[0]

                        # Associate author with the category
                        cursor.execute(
                            'INSERT INTO "ArXiv"."author_category_associations" (author_id, category_id) VALUES (%s, %s)',
                            (author_id, category_id)
                        )

            # Assign affiliation and institution to each author if present
            if i < len(author_affiliations):
                affiliation = author_affiliations[i].strip()
                cursor.execute('SELECT id FROM "ArXiv"."research_institutions" WHERE name = %s', (affiliation,))
                institution_id = cursor.fetchone()

                if institution_id is None:
                    # Insert institution
                    cursor.execute(
                        'INSERT INTO "ArXiv"."research_institutions" (name) VALUES (%s) RETURNING id',
                        (affiliation,)
                    )
                    institution_id = cursor.fetchone()[0]
                else:
                    institution_id = institution_id[0]

                # Associate author with institution
                cursor.execute(
                    'INSERT INTO "ArXiv"."author_institution_associations" (author_id, institution_id) VALUES (%s, %s)',
                    (author_id, institution_id)
                )

        # Insert additional categories and associations, ensuring non-empty categories
        if pd.notna(article_data['primary_category']) and article_data['categories'] != '':
            for category_code in article_data['primary_category'].split(', '):
                cursor.execute('SELECT id FROM "ArXiv"."research_subcategories" WHERE category_code = %s', (category_code,))
                category_id = cursor.fetchone()

                if category_id is not None:
                    category_id = category_id[0]

                    # Associate article with category
                    cursor.execute(
                        'INSERT INTO "ArXiv"."article_category_associations" (article_id, category_id) VALUES (%s, %s)',
                        (article_id, category_id)
                    )

        conn.commit()  # Commit changes
    except Exception as error:
        print(f"Error inserting article: {error}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def load_data_to_db(dataframe):
    """
    Loads data from a DataFrame and inserts it into the database.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing the article data.
    """
    dataframe.columns = dataframe.columns.str.strip()

    for i, row in dataframe.iterrows():
        insert_article_and_associations(row)


def main():
    """
    Main function to load article data from a CSV file and insert it into the database.
    """
    dataframe = pd.read_csv('../data/arxiv_articles.csv')  # Change path if necessary

    load_data_to_db(dataframe)


if __name__ == "__main__":
    main()



