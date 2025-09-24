import pandas as pd
import os


# Feel free to change the paths to the data files when you run it
CRSP_PATH = "data/crsp_monthly 1.ftr"
COMPUSTAT_PATH = "data/wxxni9ny8rmiogbr.csv"
SAVE_PATH = "data/merged_data.ftr"

# Column Parameters
ROLL_DOWN_COLUMNS = ["chq","actq","atq"] # NOTE I don't know if these are the correct columns to roll down
COLUMNS_TO_DROP = ["curcdq","datafmt","indfmt","consol"] # NOTE these are columns with only 1 value for all rows

# Ensure the data files exist
if not os.path.exists(CRSP_PATH):
    raise FileNotFoundError(f"CRSP data file not found at {CRSP_PATH}")
if not os.path.exists(COMPUSTAT_PATH):
    raise FileNotFoundError(f"Compustat data file not found at {COMPUSTAT_PATH}")

# Read the data files
crsp_df = pd.read_feather(CRSP_PATH)
print("CRSP data read successfully")
compustat_df = pd.read_csv(COMPUSTAT_PATH)
print("Compustat data read successfully")

# Convert the datadate column to datetime and rename the column to date
compustat_df["datadate"] = pd.to_datetime(compustat_df["datadate"])
compustat_df = compustat_df.rename(columns={'datadate': 'date'})
crsp_df = crsp_df.rename(columns={'caldt': 'date'})

# Truncate the cusip column to match the crsp format
# NOTE I don't know why compustat has 9 characters and crsp has 8
# but I'm pretty sure this lines them up
compustat_df['cusip'] = compustat_df['cusip'].astype(str).str[:8]

# NOTE we might want to get a table that maps gvkeys to permn
# because appeartnly the cusip's can change

# Merge the data frames
merged_df = pd.merge(crsp_df, compustat_df, on=['cusip', 'date'], how='outer')
merged_df = merged_df.sort_values(by=['cusip', 'date'])
print("Successfully merged the data frames")

# Roll down columns
merged_df[ROLL_DOWN_COLUMNS] = (
    merged_df.groupby("cusip", group_keys=False)[ROLL_DOWN_COLUMNS] # Group by cusip to roll down the columns
    .ffill() # Forward fill any data
    .bfill() # Backward fill any data that is left
    # NOTE the bfill might be nessisary, I thought we would rather be safe than sorry
)

# Drop unnecessary columns
merged_df = merged_df.drop(columns=COLUMNS_TO_DROP)

# Save the merged data frame to a csv file
merged_df.to_feather(SAVE_PATH)
print(f"Successfully saved the merged data frame to {SAVE_PATH}")


