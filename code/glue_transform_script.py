import pandas as pd  # Pandas to process CSVs

# Load the raw transactions
df = pd.read_csv("transactions_raw.csv")

# Remove duplicates
df.drop_duplicates(inplace=True)

# Convert all currencies to USD (fake conversion rates)
rates = {"USD": 1, "EUR": 1.1, "INR": 0.012}
df["amount_usd"] = df.apply(lambda x: x["amount"] * rates[x["currency"]], axis=1)

# Extract month for aggregation
df["month"] = pd.to_datetime(df["timestamp"]).dt.to_period("M")

# Aggregate monthly revenue
monthly = df.groupby("month")["amount_usd"].sum().reset_index()

# Save cleaned and aggregated files
df.to_csv("transactions_clean.csv", index=False)
monthly.to_csv("monthly_revenue.csv", index=False)

print("Data cleaned and saved to transactions_clean.csv and monthly_revenue.csv")
