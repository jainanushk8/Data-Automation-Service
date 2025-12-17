import pandas as pd
import re
import os
import sys
from pathlib import Path

# --- CONFIGURATION ---
INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed"
PINCODE_REF = "data/reference/IndiaPostPincode.csv"

# Target Schema
TARGET_COLUMNS = [
    "category", "sub_category", "city", "name", "area", "address",
    "phone_no_1", "phone_no2", "source", "ratings", "state", "country",
    "email", "latitude", "longitude", "reviews", "facebook_url",
    "linkdin_url", "twitter_url", "description", "pincode",
    "virtual_phone_no", "whatsapp_no", "phone_no_3", "avg_spent",
    "cost_of_two"
]

# Alias Dictionary (FIXED - now truly case-insensitive)
COLUMN_ALIASES = {
    "source": ["website", "url", "web_link", "link", "homepage"],
    "phone_no_1": ["phone", "mobile", "contact", "tel", "phone_number", "cell", "telephone"],
    "phone_no2": ["phone2", "mobile2", "alternate_phone", "secondary_phone"],
    "name": ["name", "business_name", "title", "company_name", "store_name"],
    "address": ["address", "location", "full_address", "street", "addr"],
    "ratings": ["rating", "stars", "review_score", "avg_rating", "reviews_average"],
    "reviews": ["review_count", "total_reviews", "number_of_reviews", "reviews_count"],
    "category": ["category", "type", "business_type"],
    "sub_category": ["subcategory", "sub_category", "sub_type", "subtype"],  # FIXED
    "city": ["city", "town", "district"],
    "state": ["state", "province", "statename"],
    "area": ["area", "locality", "neighborhood", "region"],
    "latitude": ["latitude", "lat"],  # FIXED - removed loose matching
    "longitude": ["longitude", "lng", "long"],  # FIXED
    "email": ["email", "e-mail", "mail", "contact_email"],
    "pincode": ["pincode", "pin", "postal_code", "zip"]
}


class PincodeResolver:
    """Fast pincode lookup using dictionary (O(1) access)"""
    def __init__(self, csv_path):
        self.lookup = {}
        self.city_set = set()
        self.state_set = set()
        
        try:
            df = pd.read_csv(csv_path, dtype={'pincode': str, 'latitude': str, 'longitude': str})
            df['pincode'] = df['pincode'].str.strip()
            df = df.dropna(subset=['pincode'])
            
            for _, row in df.iterrows():
                pin = row['pincode']
                if pin not in self.lookup:
                    self.lookup[pin] = {
                        'district': str(row.get('district', '')),
                        'statename': str(row.get('statename', '')),
                        'latitude': str(row.get('latitude', '')),
                        'longitude': str(row.get('longitude', ''))
                    }
                    if pd.notna(row.get('district')):
                        self.city_set.add(str(row['district']).lower().strip())
                    if pd.notna(row.get('statename')):
                        self.state_set.add(str(row['statename']).lower().strip())
            
            print(f"✓ Loaded {len(self.lookup)} pincodes from reference")
        except Exception as e:
            print(f"⚠ Warning: Could not load pincode reference: {e}")
    
    def get_info(self, pincode):
        """Returns dict with district, statename, lat, long or None"""
        return self.lookup.get(str(pincode).strip())


def extract_pincode_from_text(text):
    """Extract 6-digit Indian pincode"""
    if not isinstance(text, str):
        return ""
    match = re.search(r'\b[1-9]\d{5}\b', text)
    return match.group(0) if match else ""


def extract_plus_code_coordinates(text):
    """
    Extract lat/long from Google Plus Codes (e.g., R9P7+8RC)
    Note: This is approximate - real conversion requires Google API
    For now, we'll mark it for manual review
    """
    if not isinstance(text, str):
        return "", ""
    # Pattern: 4-8 alphanumeric + "+" + 2-3 alphanumeric
    match = re.search(r'\b([A-Z0-9]{4,8}\+[A-Z0-9]{2,3})\b', text.upper())
    if match:
        # Return empty but flag that plus code exists
        return "PLUS_CODE", match.group(1)
    return "", ""


def extract_coordinates_from_text(text):
    """Extract lat/long from URLs or formatted text"""
    if not isinstance(text, str):
        return "", ""
    # Pattern: @12.345,78.901 or (12.345, 78.901)
    match = re.search(r'([-+]?\d{1,2}\.\d+)[,\s]+([-+]?\d{1,3}\.\d+)', text)
    if match:
        return match.group(1), match.group(2)
    return "", ""


def extract_email_from_text(text):
    """Extract email address"""
    if not isinstance(text, str):
        return ""
    match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
    return match.group(0) if match else ""


def parse_address_smart(address_text, pincode_resolver):
    """Intelligently parse address into components"""
    result = {
        'city': '',
        'state': '',
        'pincode': '',
        'area': ''
    }
    
    if not isinstance(address_text, str) or not address_text.strip():
        return result
    
    text = address_text.strip()
    
    # Step 1: Extract pincode
    pincode = extract_pincode_from_text(text)
    if pincode:
        result['pincode'] = pincode
        info = pincode_resolver.get_info(pincode)
        if info:
            result['city'] = info['district']
            result['state'] = info['statename']
            return result
    
    # Step 2: Match against known cities/states
    text_lower = text.lower()
    words = re.split(r'[,\s]+', text_lower)
    
    for word in words:
        if word in pincode_resolver.state_set:
            result['state'] = word.title()
            break
    
    for word in words:
        if word in pincode_resolver.city_set:
            result['city'] = word.title()
            break
    
    # Step 3: Extract area
    parts = [p.strip() for p in text.split(',') if p.strip()]
    if len(parts) > 0:
        result['area'] = parts[0]
    
    return result


def smart_map_columns(df, pincode_resolver):
    """Map raw columns to target schema with intelligent extraction"""
    mapped_data = pd.DataFrame(index=df.index, columns=TARGET_COLUMNS)
    used_columns = set()
    
    print("🔄 Mapping columns...")
    
    # Step 1: Direct column mapping (CASE-INSENSITIVE) - CONSERVATIVE
    for target, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            matches = [col for col in df.columns if col.lower().strip() == alias.lower().strip()]
            if matches and matches[0] not in used_columns:
                mapped_data[target] = df[matches[0]].astype(str).replace('nan', '').replace('NaN', '')
                used_columns.add(matches[0])
                print(f"   ✓ {target} ← '{matches[0]}'")
                break
    
    # Step 2: Handle multiple phone columns
    phone_cols = [col for col in df.columns if any(alias in col.lower() for alias in COLUMN_ALIASES["phone_no_1"])]
    if len(phone_cols) >= 2:
        mapped_data["phone_no2"] = df[phone_cols[1]].astype(str).replace('nan', '').replace('NaN', '')
        print(f"   ✓ phone_no2 ← '{phone_cols[1]}'")
    
    # Step 3: Set default country
    mapped_data["country"] = "India"
    
    print("\n🔍 Extracting missing data (conservative mode)...")
    
    # Step 4: Extract from address field (ONLY if target columns are empty)
    address_col = mapped_data["address"]
    extracted_pincodes = 0
    
    for i in range(len(mapped_data)):
        addr = address_col.iloc[i]
        
        if not isinstance(addr, str) or not addr.strip() or addr in ['nan', 'NaN', '']:
            continue
        
        # Extract pincode ONLY if empty
        current_pincode = str(mapped_data["pincode"].iloc[i]).strip()
        if not current_pincode or current_pincode in ['nan', 'NaN', '', 'None']:
            extracted_pin = extract_pincode_from_text(addr)
            if extracted_pin:
                mapped_data.loc[i, "pincode"] = extracted_pin
                extracted_pincodes += 1
        
        # Parse address ONLY if city/state are empty
        current_city = str(mapped_data["city"].iloc[i]).strip()
        current_state = str(mapped_data["state"].iloc[i]).strip()
        
        need_city = not current_city or current_city in ['nan', 'NaN', '', 'None']
        need_state = not current_state or current_state in ['nan', 'NaN', '', 'None']
        
        if need_city or need_state:
            parsed = parse_address_smart(addr, pincode_resolver)
            
            if need_city and parsed['city']:
                mapped_data.loc[i, "city"] = parsed['city']
            if need_state and parsed['state']:
                mapped_data.loc[i, "state"] = parsed['state']
            
            # Fill area ONLY if empty
            current_area = str(mapped_data["area"].iloc[i]).strip()
            if (not current_area or current_area in ['nan', 'NaN', '', 'None']) and parsed['area']:
                mapped_data.loc[i, "area"] = parsed['area']
    
    if extracted_pincodes > 0:
        print(f"   ✓ Extracted {extracted_pincodes} pincodes from addresses")
    
    # Step 5: Fill lat/long from pincode lookup (CONSERVATIVE - only if empty)
    print("\n🌍 Filling coordinates from pincode reference...")
    lat_filled = 0
    lon_filled = 0
    skipped_existing = 0
    
    for i in range(len(mapped_data)):
        pin = str(mapped_data["pincode"].iloc[i]).strip()
        current_lat = str(mapped_data["latitude"].iloc[i]).strip()
        current_lon = str(mapped_data["longitude"].iloc[i]).strip()
        
        # Check if we NEED to fill lat/long
        need_lat = not current_lat or current_lat in ['nan', 'NaN', '', 'None']
        need_lon = not current_lon or current_lon in ['nan', 'NaN', '', 'None']
        
        # Skip if raw data already has both
        if not need_lat and not need_lon:
            skipped_existing += 1
            continue
        
        # If we have a valid pincode and need coords
        if pin and pin not in ['nan', 'NaN', '', 'None']:
            info = pincode_resolver.get_info(pin)
            if info:
                # Fill latitude ONLY if empty
                if need_lat and info['latitude'] not in ['nan', 'NaN', '', 'None']:
                    mapped_data.loc[i, "latitude"] = info['latitude']
                    lat_filled += 1
                
                # Fill longitude ONLY if empty
                if need_lon and info['longitude'] not in ['nan', 'NaN', '', 'None']:
                    mapped_data.loc[i, "longitude"] = info['longitude']
                    lon_filled += 1
    
    print(f"   ✓ Filled {lat_filled} latitude values from pincode")
    print(f"   ✓ Filled {lon_filled} longitude values from pincode")
    if skipped_existing > 0:
        print(f"   ℹ Skipped {skipped_existing} rows (already had coordinates)")
    
    # Step 6: Try to extract coordinates from area (Plus Codes)
    print("\n📍 Checking for Google Plus Codes in area field...")
    plus_codes_found = 0
    for i in range(len(mapped_data)):
        area_val = str(mapped_data["area"].iloc[i]).strip()
        if area_val and area_val not in ['nan', 'NaN', '', 'None']:
            flag, code = extract_plus_code_coordinates(area_val)
            if flag == "PLUS_CODE":
                # Store the plus code in description for manual review
                current_desc = str(mapped_data["description"].iloc[i]).strip()
                if not current_desc or current_desc in ['nan', 'NaN', '', 'None']:
                    mapped_data.loc[i, "description"] = f"Google Plus Code: {code}"
                else:
                    mapped_data.loc[i, "description"] = f"{current_desc} | Plus Code: {code}"
                plus_codes_found += 1
    
    if plus_codes_found > 0:
        print(f"   ⚠ Found {plus_codes_found} Plus Codes (stored in description field)")
        print(f"   💡 Tip: Use Google Maps Geocoding API to convert Plus Codes to lat/long")
    
    # Step 7: Extract coordinates from source/website URLs (ONLY if still empty)
    coords_from_url = 0
    for i in range(len(mapped_data)):
        current_lat = str(mapped_data["latitude"].iloc[i]).strip()
        current_lon = str(mapped_data["longitude"].iloc[i]).strip()
        
        need_lat = not current_lat or current_lat in ['nan', 'NaN', '', 'None']
        need_lon = not current_lon or current_lon in ['nan', 'NaN', '', 'None']
        
        if need_lat or need_lon:
            source = str(mapped_data["source"].iloc[i]).strip()
            if source and source not in ['nan', 'NaN', '', 'None']:
                lat, lon = extract_coordinates_from_text(source)
                if lat and lon:
                    if need_lat:
                        mapped_data.loc[i, "latitude"] = lat
                    if need_lon:
                        mapped_data.loc[i, "longitude"] = lon
                    coords_from_url += 1
    
    if coords_from_url > 0:
        print(f"\n🔗 Extracted {coords_from_url} coordinates from URLs")
    
    # Step 8: Extract emails (ONLY if empty)
    emails_found = 0
    for i in range(len(mapped_data)):
        current_email = str(mapped_data["email"].iloc[i]).strip()
        if not current_email or current_email in ['nan', 'NaN', '', 'None']:
            for field in ["address", "description", "source"]:
                field_value = str(mapped_data[field].iloc[i]).strip()
                if field_value and field_value not in ['nan', 'NaN', '', 'None']:
                    email = extract_email_from_text(field_value)
                    if email:
                        mapped_data.loc[i, "email"] = email
                        emails_found += 1
                        break
    
    if emails_found > 0:
        print(f"📧 Extracted {emails_found} email addresses")
    
    # Clean up nan values
    mapped_data = mapped_data.replace(['nan', 'NaN', 'None'], '').infer_objects(copy=False)
    
    print("\n✅ Mapping complete!")
    
    return mapped_data


def process_file(input_path, output_path, pincode_resolver):
    """Process a single CSV file"""
    try:
        print(f"\n📄 Processing: {input_path}")
        
        try:
            raw_df = pd.read_csv(input_path, encoding='utf-8')
        except UnicodeDecodeError:
            raw_df = pd.read_csv(input_path, encoding='ISO-8859-1')
        
        print(f"   Rows: {len(raw_df)} | Columns: {len(raw_df.columns)}")
        
        final_df = smart_map_columns(raw_df, pincode_resolver)
        
        final_df.to_csv(output_path, index=False)
        print(f"✅ Saved: {output_path}")
        
        return True
    except Exception as e:
        print(f"✗ Error processing {input_path}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(PINCODE_REF):
        print(f"⚠ Warning: Pincode reference not found at {PINCODE_REF}")
        pincode_resolver = PincodeResolver.__new__(PincodeResolver)
        pincode_resolver.lookup = {}
        pincode_resolver.city_set = set()
        pincode_resolver.state_set = set()
    else:
        pincode_resolver = PincodeResolver(PINCODE_REF)
    
    csv_files = list(Path(INPUT_DIR).glob("*.csv"))
    
    if not csv_files:
        print(f"⚠ No CSV files found in {INPUT_DIR}/")
        return
    
    print(f"\n🚀 Found {len(csv_files)} file(s) to process\n")
    
    success_count = 0
    for csv_file in csv_files:
        output_filename = f"{csv_file.stem}_cleaned.csv"
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        if process_file(str(csv_file), output_path, pincode_resolver):
            success_count += 1
    
    print(f"\n✅ Done! Processed {success_count}/{len(csv_files)} files successfully")


if __name__ == "__main__":
    main()
