import pandas as pd
import re
import os
import sys

# --- CONFIGURATION ---
INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed"
OUTPUT_FILE = "cleaned_data.csv"

# Target Schema (Your desired final columns)
TARGET_COLUMNS = [
    "category", "sub_category", "city", "name", "area", "address",
    "phone_no_1", "phone_no2", "source", "ratings", "state", "country",
    "email", "latitude", "longitude", "reviews", "facebook_url",
    "linkdin_url", "twitter_url", "description", "pincode",
    "virtual_phone_no", "whatsapp_no", "phone_no_3", "avg_spent",
    "cost_of_two"
]

# Alias Dictionary (Synonyms to look for)
COLUMN_ALIASES = {
    "source": ["website", "url", "web_link", "link", "homepage"],
    "phone_no_1": ["phone", "mobile", "contact", "tel", "phone_number", "cell"],
    "name": ["name", "business_name", "title", "company_name", "store_name"],
    "address": ["address", "location", "full_address", "street"],
    "ratings": ["rating", "stars", "review_score", "avg_rating"],
    "reviews": ["review_count", "total_reviews", "number_of_reviews"],
    "category": ["category", "type", "business_type"],
    "sub_category": ["subcategory", "sub_type"],
    "city": ["city", "town", "district"],
    "latitude": ["lat", "latitude", "y"],
    "longitude": ["long", "lng", "longitude", "x"],
    "email": ["email", "e-mail", "mail", "contact_email"]
}

def extract_pincode(text):
    """Extracts 6-digit Indian Pincode from text."""
    if not isinstance(text, str): return ""
    match = re.search(r'\b[1-9]\d{5}\b', text)
    return match.group(0) if match else ""

def extract_coordinates(text):
    """Attempts to find Lat/Long in a text field (like a URL or address)."""
    if not isinstance(text, str): return "", ""
    # Look for patterns like @12.345,78.901 or 12.345, 78.901
    match = re.search(r'([-+]?\d{1,2}\.\d+),\s*([-+]?\d{1,3}\.\d+)', text)
    if match:
        return match.group(1), match.group(2)
    return "", ""

def smart_map_columns(df):
    """Maps raw columns to target columns using aliases."""
    mapped_data = pd.DataFrame(columns=TARGET_COLUMNS)
    used_columns = []

    # 1. Direct & Alias Mapping
    for target, aliases in COLUMN_ALIASES.items():
        # Check aliases in priority order
        for alias in aliases:
            # Case-insensitive check
            matches = [col for col in df.columns if col.lower() == alias.lower()]
            if matches:
                # Found a match! Map it.
                col_name = matches[0]
                if col_name not in used_columns:
                    mapped_data[target] = df[col_name].astype(str)
                    used_columns.append(col_name)
                    break
    
    # 2. "Leftover" Phone Logic (If 2 phone cols exist, map the 2nd one to phone_no2)
    phone_aliases = COLUMN_ALIASES["phone_no_1"]
    found_phones = [col for col in df.columns if col.lower() in phone_aliases]
    
    # If we found multiple phone columns in raw data
    if len(found_phones) > 1:
        # We already mapped the first one to phone_no_1 above.
        # Let's map the second one to phone_no2 if it's empty
        second_phone_col = found_phones[1] 
        mapped_data["phone_no2"] = df[second_phone_col].astype(str)

    # 3. Fill missing critical columns with defaults
    mapped_data["country"] = "India"
    
    return mapped_data, df  # Return original df too for extraction

def main():
    # Ensure directories exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Find the first CSV in the input directory
    try:
        files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
        if not files:
            print("No CSV files found in data/raw/")
            return
            
        input_path = os.path.join(INPUT_DIR, files[0])
        print(f"Processing file: {input_path}")
        
        # Read Raw Data (Handle encoding errors)
        try:
            raw_df = pd.read_csv(input_path, encoding='utf-8')
        except UnicodeDecodeError:
            raw_df = pd.read_csv(input_path, encoding='ISO-8859-1')

        # --- STEP 1: SMART MAPPING ---
        final_df, original_df = smart_map_columns(raw_df)
        
        # --- STEP 2: ADVANCED EXTRACTION ---
        
        # A. Pincode Extraction (from Address)
        if "address" in final_df.columns:
            print("Extracting Pincodes...")
            final_df["pincode"] = final_df["address"].apply(extract_pincode)

        # B. Lat/Long Extraction (from Source URL or Address if Lat/Long is missing)
        if final_df["latitude"].isna().all() and "source" in final_df.columns:
             print("Extracting Coordinates from URL...")
             # Apply extraction to the 'source' column
             coords = final_df["source"].apply(extract_coordinates)
             # Unzip tuple into two columns
             final_df["latitude"] = coords.apply(lambda x: x[0])
             final_df["longitude"] = coords.apply(lambda x: x[1])

        # Save Output
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        final_df.to_csv(output_path, index=False)
        print(f"Success! Cleaned data saved to: {output_path}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
