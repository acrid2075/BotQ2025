import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()  # reads .env file
CRSP_PATH = os.getenv("CRSP_PATH")
COMPUSTAT_PATH = os.getenv("COMPUSTAT_PATH")
SAVE_PATH = os.getenv("SAVE_PATH")

# Column Parameters
ROLL_DOWN_COLUMNS = ["chq","actq","atq"] # These are the cash, current asset, and total asset columns
COLUMNS_TO_DROP = ["curcdq","datafmt","indfmt","consol"]

# Ensure the data files exist
if not os.path.exists(str(CRSP_PATH)):
    raise FileNotFoundError(f"CRSP data file not found at {CRSP_PATH}")
if not os.path.exists(str(COMPUSTAT_PATH)):
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
# FIXME: This seems concerning
compustat_df['cusip'] = compustat_df['cusip'].astype(str).str[:8]

# NOTE we might want to get a table that maps gvkeys to permn
# because apparently the cusip's can change

# Merge the data frames
merged_df = pd.merge(crsp_df, compustat_df, on=['cusip', 'date'], how='outer')
merged_df = merged_df.sort_values(by=['cusip', 'date'])
print("Successfully merged the data frames")

# Roll down columns
merged_df[ROLL_DOWN_COLUMNS] = (
    merged_df.groupby("cusip", group_keys=False)[ROLL_DOWN_COLUMNS] # Group by cusip to roll down the columns
    .ffill() # Forward fill any data
)

merged_df.rename(columns={'chq': 'cash', 'actq': 'current', 'atq': 'assets'}, inplace=True)

# Drop unnecessary columns
merged_df.drop(columns=COLUMNS_TO_DROP, inplace=True)

# Drop to the original set of monthly rows
merged_df = merged_df.loc[merged_df.date.isin(crsp_df.date)]
merged_df = merged_df.loc[merged_df['ret'].dropna().index]

# Save the merged data frame to a csv file
merged_df.to_feather(SAVE_PATH)
print(f"Successfully saved the merged data frame to {SAVE_PATH}")


