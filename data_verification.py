import pandas as pd


def check_tic_ticker_mismatch():
    """
    Checks for mismatches between the 'tic' and 'ticker' columns within each CUSIP group
    in the merged data. For each CUSIP, it compares the 'tic' and 'ticker' values where
    both are present and prints out any rows where they do not match. The function pauses
    after each mismatch and prompts the user to continue or stop the verification process.
    """

    df = pd.read_feather("data/merged_data.ftr")

    stop = False

    for group in df.groupby('cusip'):
        group_df = group[1]
        # Only check for mismatches where both tic and ticker are not None/NaN
        valid_pairs = group_df.dropna(subset=['tic', 'ticker'])
        if not valid_pairs.empty:
            mismatch = valid_pairs[valid_pairs['tic'] != valid_pairs['ticker']]
            if not mismatch.empty:
                print(mismatch[['tic', 'ticker']])
                
                i = input("Continue? (y/n)")
                if i == 'n':
                    stop = True
                    break

        if stop:
            break   

def check_if_tic_exists():
    """
    Check if there are mismatched tic or ticker values that exist under different CUSIPs.
    This helps identify cases where the same ticker might be associated with different companies
    or where there are data quality issues.
    """
    df = pd.read_feather("data/merged_data.ftr")
    
    print("=== CHECKING FOR TIC/TICKER MISMATCHES ACROSS DIFFERENT CUSIPs ===")
    
    # Get unique combinations of tic, ticker, and cusip
    ticker_cusip_pairs = df[['tic', 'ticker', 'cusip']].dropna(subset=['tic', 'ticker']).drop_duplicates()
    
    # Group by ticker to see if same ticker appears with different tics
    print("\n1. Checking if same TICKER appears with different TICs:")
    ticker_groups = ticker_cusip_pairs.groupby('ticker')
    
    ticker_conflicts = 0
    for ticker, group in ticker_groups:
        if len(group['tic'].unique()) > 1:
            ticker_conflicts += 1
            print(f"\nTICKER '{ticker}' appears with different TICs:")
            for _, row in group.iterrows():
                print(f"  CUSIP: {row['cusip']} | TIC: {row['tic']} | TICKER: {row['ticker']}")
            
            if ticker_conflicts >= 10:  # Limit output
                print(f"\n... and {len(ticker_groups) - ticker_conflicts} more ticker conflicts")
                break
    
    print(f"\nTotal ticker conflicts found: {ticker_conflicts}")
    
    # Group by tic to see if same tic appears with different tickers
    print("\n2. Checking if same TIC appears with different TICKERs:")
    tic_groups = ticker_cusip_pairs.groupby('tic')
    
    tic_conflicts = 0
    for tic, group in tic_groups:
        if len(group['ticker'].unique()) > 1:
            tic_conflicts += 1
            print(f"\nTIC '{tic}' appears with different TICKERs:")
            for _, row in group.iterrows():
                print(f"  CUSIP: {row['cusip']} | TIC: {row['tic']} | TICKER: {row['ticker']}")
            
            if tic_conflicts >= 10:  # Limit output
                print(f"\n... and {len(tic_groups) - tic_conflicts} more tic conflicts")
                break
    
    print(f"\nTotal tic conflicts found: {tic_conflicts}")
    
    # Check for cases where ticker matches but CUSIPs are different
    print("\n3. Checking for ticker matches across different CUSIPs:")
    
    # Find tickers that appear with multiple CUSIPs
    ticker_cusip_counts = ticker_cusip_pairs.groupby('ticker')['cusip'].nunique()
    multi_cusip_tickers = ticker_cusip_counts[ticker_cusip_counts > 1]
    
    print(f"Tickers appearing with multiple CUSIPs: {len(multi_cusip_tickers)}")
    
    if len(multi_cusip_tickers) > 0:
        print("\nSample cases:")
        for ticker in multi_cusip_tickers.head(5).index:
            ticker_data = ticker_cusip_pairs[ticker_cusip_pairs['ticker'] == ticker]
            print(f"\nTICKER '{ticker}':")
            for _, row in ticker_data.iterrows():
                print(f"  CUSIP: {row['cusip']} | TIC: {row['tic']}")
    
    # Summary statistics
    print(f"\n=== SUMMARY ===")
    print(f"Unique ticker-cusip pairs: {len(ticker_cusip_pairs)}")
    print(f"Unique tickers: {ticker_cusip_pairs['ticker'].nunique()}")
    print(f"Unique tics: {ticker_cusip_pairs['tic'].nunique()}")
    print(f"Tickers with multiple CUSIPs: {len(multi_cusip_tickers)}")
    print(f"Ticker conflicts (same ticker, different tic): {ticker_conflicts}")
    print(f"TIC conflicts (same tic, different ticker): {tic_conflicts}")
    
    return {
        'ticker_conflicts': ticker_conflicts,
        'tic_conflicts': tic_conflicts,
        'multi_cusip_tickers': len(multi_cusip_tickers)
    }


if __name__ == "__main__":
    # Run both functions
    print("Running ticker/tic mismatch check...")
    check_tic_ticker_mismatch()
    
    print("\n" + "="*60)
    print("Running cross-CUSIP analysis...")
    results = check_if_tic_exists()