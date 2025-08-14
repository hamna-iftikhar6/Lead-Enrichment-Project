import pandas as pd

# Read the original CSV
df = pd.read_csv("data/processed/individual_fps_format.csv")

# Take the first 10 rows
df_sample = df.head(10)

# Save to a new CSV
df_sample.to_csv("data/processed/test.csv", index=False)

print("✅ test.csv created with 10 rows.")
