import pandas as pd

def run_extraction():
    try:
        data = pd.read_csv("zipco_transaction.csv")
        print("Data extracted successfully!")
        return data
    except Exception as e:
        print(f"data extraction failed: {e}")
        return None