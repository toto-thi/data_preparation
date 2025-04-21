import os
import xml.etree.ElementTree as ET
import pandas as pd
from glob import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

INPUT_FOLDER = "data/gbif/"
OUTPUT_FILE = "data/gbif_sightings.csv"

# Namespace for subelements like <additionalMetadata>
ns = {"eml": "eml://ecoinformatics.org/eml-2.1.1"}

def get_text(el, tag, ns_tag=False):
    """Safely get text content from a tag, with or without namespace."""
    try:
        full_tag = f"{{{ns['eml']}}}{tag}" if ns_tag else tag
        child = el.find(full_tag)
        return child.text.strip() if child is not None and child.text else None
    except Exception:
        return None

def parse_gbif_folder(folder, output_file):
    records = []
    files = glob(os.path.join(folder, "*.xml"))
    logging.info(f"🔍 Found {len(files)} XML files in {folder}")

    for file in files:
        try:
            tree = ET.parse(file)
            root = tree.getroot()

            dataset = root.find("dataset")
            if dataset is None:
                logging.warning(f"Could not find <dataset> element in {file}")
                continue

            # Extract elements from dataset
            title = get_text(dataset, "title")
            pub_date = get_text(dataset, "pubDate")
            doi = get_text(dataset, "alternateIdentifier")
            url = get_text(dataset.find("distribution"), "url") if dataset.find("distribution") is not None else None

            # Creators
            creator_names = []
            creators = dataset.findall("creator")
            for c in creators:
                gn = get_text(c.find("individualName"), "givenName")
                sn = get_text(c.find("individualName"), "surName")
                if gn and sn:
                    creator_names.append(f"{gn} {sn}")

            # Publisher and distributor info
            associated_parties = dataset.findall("associatedParty")
            publisher = distributor = publisher_country = distributor_country = None
            if len(associated_parties) >= 1:
                publisher = get_text(associated_parties[0], "organizationName")
                publisher_country = get_text(associated_parties[0].find("address"), "country") if associated_parties[0].find("address") is not None else None
            if len(associated_parties) >= 2:
                distributor = get_text(associated_parties[1], "organizationName")
                distributor_country = get_text(associated_parties[1].find("address"), "country") if associated_parties[1].find("address") is not None else None

            records.append({
                "title": title,
                "creators": "; ".join(creator_names),
                "publication_date": pub_date,
                "publisher_country": publisher_country,
                "distributor_country": distributor_country,
                "publisher": publisher,
                "distributor": distributor,
                "doi": doi,
                "url": url
            })

            logging.info(f"Parsed file: {os.path.basename(file)}")

        except Exception as e:
            logging.warning(f"Failed to parse {file}: {e}")

    df = pd.DataFrame(records)
    df.to_csv(output_file, index=False)
    logging.info(f"Parsed {len(df)} XML files into {output_file}")

if __name__ == "__main__":
    parse_gbif_folder(INPUT_FOLDER, OUTPUT_FILE)
