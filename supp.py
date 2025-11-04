import pandas as pd

# 1. Define the data as a dictionary.
# The keys ('name', 'id') will become the column headers.
data = {
    'name': [
        'alem', 
        'lema', 
        'kasu', 
        'girma', 
        'mesfin', 
        'tigist'
    ],
    'id': [
        'efb6299a-17dd-484f-86e4-f7ba491d62ad',
        'f107cf33-26e0-476c-8710-0e54ffd960a2',
        '70f3777c-30d0-4f75-8318-8e35b94839ff',
        '8346e2b8-3128-418a-bd92-ed171c4c2e1b',
        '13387d80-97b7-441a-b0fd-164ac8706540',
        '8346e2b8-3128-418a-bd92-ed171c4c2e1b'  # Note: This is the same ID as 'girma'
    ]
}

# 2. Create a pandas DataFrame from the dictionary.
df = pd.DataFrame(data)

# 3. Define the name of the output file.
output_filename = 'user_data.csv'

# 4. Export the DataFrame to a CSV file.
# `index=False` prevents pandas from writing the row numbers into the file.
df.to_csv(output_filename, index=False)

# 5. Print a confirmation message to the console.
print(f"✅ Successfully created the CSV file named '{output_filename}'")