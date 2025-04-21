import json
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

INPUT_PATH = "data/iucn_green_status.json"
OUTPUT_PATH = "data/iucn_species_status.csv"

def parse_iucn_json(input_path, output_path):
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        rows = []
        for item in data.get("assessments", []):
            taxon = item.get("taxon", {})
            common_names = taxon.get("common_names", [])
            main_common_name = next((n["name"] for n in common_names if n.get("main")), None)
            
            rows.append({
                "scientific_name": taxon.get("scientific_name"),
                "common_name": main_common_name,
                "class_name": taxon.get("class_name"),
                "order_name": taxon.get("order_name"),
                "family_name": taxon.get("family_name"),
                "genus_name": taxon.get("genus_name"),
                "species_name": taxon.get("species_name"),
                "assessment_year": item.get("assessment_year"),
                "species_recovery_score_best": item.get("species_recovery_score_best"),
                "species_recovery_category": item.get("species_recovery_category"),
                "conservation_legacy_category": item.get("conservation_legacy_category"),
                "conservation_dependence_category": item.get("conservation_dependence_category"),
                "conservation_gain_category": item.get("conservation_gain_category"),
                "recovery_potential_category": item.get("recovery_potential_category"),
                "url": item.get("url")
            })

        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False)
        logging.info(f"Parsed {len(df)} records from IUCN JSON and saved to {output_path}")

    except Exception as e:
        logging.error(f"Failed to parse IUCN JSON: {e}")

if __name__ == "__main__":
    parse_iucn_json(INPUT_PATH, OUTPUT_PATH)