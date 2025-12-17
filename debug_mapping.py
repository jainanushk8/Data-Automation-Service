import pandas as pd
from pathlib import Path

# Check one of your raw files
raw_file = list(Path("data/raw").glob("*.csv"))[0]
print(f"Analyzing: {raw_file}\n")

# Read the raw file
try:
    df = pd.read_csv(raw_file, encoding='utf-8')
except:
    df = pd.read_csv(raw_file, encoding='ISO-8859-1')

print("=" * 70)
print("RAW FILE COLUMN NAMES (exact spelling):")
print("=" * 70)
for i, col in enumerate(df.columns, 1):
    print(f"{i}. '{col}'")

print("\n" + "=" * 70)
print("SAMPLE DATA (First 3 rows):")
print("=" * 70)
print(df.head(3).to_string())

print("\n" + "=" * 70)
print("CHECKING FOR TARGET COLUMNS:")
print("=" * 70)

# Check for subcategory variations
subcategory_variants = ['subcategory', 'sub_category', 'sub category', 'subtype', 'sub_type', 'type']
print("\n🔍 Subcategory column check:")
for col in df.columns:
    if any(variant in col.lower() for variant in subcategory_variants):
        print(f"   ✓ FOUND: '{col}'")
        print(f"     Sample values: {df[col].head(3).tolist()}")

# Check for latitude variations
lat_variants = ['lat', 'latitude', 'y', 'coord']
print("\n🔍 Latitude column check:")
for col in df.columns:
    if any(variant in col.lower() for variant in lat_variants):
        print(f"   ✓ FOUND: '{col}'")
        print(f"     Sample values: {df[col].head(3).tolist()}")

# Check for longitude variations
lon_variants = ['lon', 'lng', 'long', 'longitude', 'x']
print("\n🔍 Longitude column check:")
for col in df.columns:
    if any(variant in col.lower() for variant in lon_variants):
        print(f"   ✓ FOUND: '{col}'")
        print(f"     Sample values: {df[col].head(3).tolist()}")

print("\n" + "=" * 70)
