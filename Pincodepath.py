import pandas as pd

# Update this path to where your pincode CSV is located
pincode_df = pd.read_csv(r"F:\HBD Project Materials\IndiaPostPincode.csv") 

print("Column Names:")
print(pincode_df.columns.tolist())
print("\nFirst 5 Rows:")
print(pincode_df.head())
print("\nData Types:")  
print(pincode_df.dtypes)