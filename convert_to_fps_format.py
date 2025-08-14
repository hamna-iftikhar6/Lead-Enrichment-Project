import pandas as pd
import os
from pathlib import Path

def convert_to_fps_format():
    # Read the original CSV
    df = pd.read_csv("data/individual.csv")
    
    # Create a new DataFrame with FastPeopleSearch format
    fps_df = pd.DataFrame()
    
    # Extract name components
    fps_df["name"] = df["Owner Name(s) Formatted"].str.strip()
    fps_df[["First Name", "Last Name"]] = df["Owner Name(s) Formatted"].str.split(n=1, expand=True)
    
    # Use Property Full Address as the primary address, fall back to Mailing Full Address
    fps_df["address"] = df["Property Full Address"].fillna(df["Mailing Full Address"])
    fps_df["ZIP"] = df["ZIP Code"].fillna(df["Mailing ZIP Code"])
    
    # Initialize FastPeopleSearch data columns
    fps_df["Full Address"] = None
    fps_df["Phone1"] = None
    fps_df["Phone2"] = None
    fps_df["Phone3"] = None
    fps_df["Phone4"] = None
    fps_df["Phone5"] = None
    fps_df["Age"] = None
    fps_df["Relatives"] = None
    fps_df["Emails"] = None
    fps_df["Marital Status"] = None
    fps_df["Associates"] = None
    fps_df["Previous Addresses"] = None
    fps_df["Current Address Details"] = None
    fps_df["Background Report Summary"] = None
    fps_df["FAQs"] = None
    fps_df["Page URL"] = None
    
    # Create output directory if it doesn't exist
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save to Excel and CSV
    output_excel = str(output_dir / "individual_fps_format.xlsx")
    output_csv = str(output_dir / "individual_fps_format.csv")
    
    fps_df.to_excel(output_excel, index=False)
    fps_df.to_csv(output_csv, index=False)
    
    print(f"\nConverted {len(fps_df)} records to FastPeopleSearch format")
    print("Files saved as:")
    print(f"- {output_excel}")
    print(f"- {output_csv}")
    
    print(f"Converted {len(fps_df)} records to FastPeopleSearch format")
    print("Files saved as:")
    print("- data/processed/individual_fps_format.xlsx")
    print("- data/processed/individual_fps_format.csv")
    
if __name__ == "__main__":
    convert_to_fps_format()
