from faker import Faker  # library to create fake names, dates, etc.
import pandas as pd      # Pandas for handling data tables
import random            # Random for generating random numbers

# Create a fake data generator with 1 million rows
fake = Faker()
rows = 1_000_000 
data = []

currencies = ["USD", "EUR", "INR"]

# Generate 1 million fake transactions
for _ in range(rows):
    data.append({
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1000, 5000),
        "amount": round(random.uniform(5, 500), 2),
        "currency": random.choice(currencies),
        "timestamp": fake.date_time_this_year()
    })

# Save as CSV
df = pd.DataFrame(data)
df.to_csv("transactions_raw.csv", index=False)
print("Generated transactions_raw.csv with", rows, "rows")
