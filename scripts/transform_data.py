import os
import xml.etree.ElementTree as ET
import pandas as pd

DATA_FOLDER = "../data"


def process_xml_file(filepath):
    """
    Processes an XML file, extracting relevant article information such as authors,
    affiliations, categories, and links to HTML and PDF versions.

    Args:
        filepath (str): The path to the XML file to process.

    Returns:
        list: A list of dictionaries containing the extracted article data.
    """
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()

        articles = []
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            authors = []
            affiliations = []
            countries = []  # Empty as per indication
            categories = []

            # Extract authors and possible affiliations
            for author in entry.findall('{http://www.w3.org/2005/Atom}author'):
                author_name = author.find('{http://www.w3.org/2005/Atom}name').text
                authors.append(author_name)

                # Extract affiliation (institution) if present
                affiliation = author.find('{http://arxiv.org/schemas/atom}affiliation')
                affiliations.append(affiliation.text if affiliation is not None else 'Unknown')

            # Extract primary category
            primary_category = entry.find('{http://arxiv.org/schemas/atom}primary_category').attrib['term']

            # Extract additional categories
            for category in entry.findall('{http://www.w3.org/2005/Atom}category'):
                category_term = category.attrib['term']
                if category_term != primary_category:
                    categories.append(category_term)

            # Extract HTML and PDF links
            html_url = entry.find('{http://www.w3.org/2005/Atom}link[@rel="alternate"]').attrib['href']
            pdf_link_elem = entry.find('{http://www.w3.org/2005/Atom}link[@title="pdf"]')
            pdf_url = pdf_link_elem.attrib['href'] if pdf_link_elem is not None else None

            # Build the article dictionary
            article = {
                'arxiv_id': entry.find('{http://www.w3.org/2005/Atom}id').text,
                'title': entry.find('{http://www.w3.org/2005/Atom}title').text,
                'summary': entry.find('{http://www.w3.org/2005/Atom}summary').text,
                'published': entry.find('{http://www.w3.org/2005/Atom}published').text,
                'updated': entry.find('{http://www.w3.org/2005/Atom}updated').text,
                'html_url': html_url,
                'pdf_url': pdf_url,
                'authors': ', '.join(authors),
                'affiliations': ', '.join(affiliations),
                'countries': '',
                'primary_category': primary_category,
                'categories': ', '.join(categories)
            }

            articles.append(article)
        return articles
    except Exception as e:
        print(f"Error parsing XML file {filepath}: {e}")
        return []


def process_all_files(data_folder):
    """
    Processes all XML files in the specified folder, extracting article data from each file.

    Args:
        data_folder (str): The folder containing XML files to process.

    Returns:
        list: A list of dictionaries containing article data from all processed XML files.
    """
    all_articles = []
    for filename in os.listdir(data_folder):
        if filename.endswith(".xml"):
            filepath = os.path.join(data_folder, filename)
            articles = process_xml_file(filepath)
            all_articles.extend(articles)
    return all_articles


def create_dataframe(articles):
    """
    Converts a list of article dictionaries into a pandas DataFrame.

    Args:
        articles (list): A list of dictionaries containing article data.

    Returns:
        pd.DataFrame: A DataFrame containing the article data.
    """
    return pd.DataFrame(articles)


def transform_data():
    """
    Main function to process XML files, transform them into a DataFrame,
    and save the result as a CSV file.
    """
    all_articles = process_all_files(DATA_FOLDER)
    df = create_dataframe(all_articles)

    # Display the full DataFrame with all columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df)

    # Save the DataFrame to a CSV file
    df.to_csv(os.path.join(DATA_FOLDER, 'arxiv_articles.csv'), index=False)


if __name__ == "__main__":
    transform_data()

