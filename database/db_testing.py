# db_testing.py
# This file is a sandbox for me to explore the database as it was put into SQLite and the JSON files using pandas to verify 
# that Tables are populated correctly, sub-queries return their expected result, and for Exploratory Data Analysis (EDA) on the dataset.

import sqlite3
con = sqlite3.connect('./fetch.db')
cur = con.cursor()

# one_month_in_unix = 2629743
# most_recent_receipt_scanned = cur.execute("""
#     SELECT strftime('%s', dateScanned) 
#     FROM Receipts 
#     ORDER BY dateScanned DESC 
#     LIMIT 1
# """).fetchone()[0]

# res = cur.execute("""
#     SELECT i.brandCode, SUM(i.finalPrice) AS total
#     FROM Receipts r JOIN Items i
#     ON r.id = i.receiptId
#     WHERE unixepoch(r.dateScanned) >= ? - ?
#     GROUP BY i.brandCode
# """, (most_recent_receipt_scanned, one_month_in_unix))

# print(res.fetchall())

########### END SQLite Testing #############



#########  BEGIN pandas Testing ############

import pandas as pd
from json import loads

# Import Receipts data
receipt_data = []
with open('./data/receipts.json', 'r') as f:
    for line in f:
        receipt_data.append(loads(line))

receipts_df = pd.DataFrame(receipt_data)
# print('receipts_df:', receipts_df)

# Check how many rows have a NULL value for 'totalSpent'
total_spent_null_count = receipts_df['totalSpent'].isnull().sum()
total_spent_total_count = receipts_df['totalSpent'].shape[0]
print(f'Rows where Receipts.totalSpent is null: {total_spent_null_count} out of {total_spent_total_count}')

# Import Users data
users_data = []
with open('./data/users.json', 'r') as f:
    for line in f:
        users_data.append(loads(line))

users_df = pd.DataFrame(users_data)
users_df['uuid'] = users_df['_id'].apply(lambda x: x.get('$oid'))
# print('users_df:', users_df)

# Check how many Users.id's are duplicates
num_duplicate_uuid = len(users_df[users_df.duplicated(subset=['uuid'], keep='first')])
num_uuid = users_df['uuid'].shape[0]
print(f'Rows with a duplicate Users.id: {num_duplicate_uuid} out of {num_uuid}', '\n')


###### Find summary for values in Receipts.totalSpent, and check for outliers/negative values ######
totalSpent = cur.execute('SELECT totalSpent FROM Receipts WHERE totalSpent IS NOT NULL').fetchall()
totalSpent = [x[0] for x in totalSpent]
# print(totalSpent)

total_spent_df = pd.DataFrame(totalSpent, columns=['totalSpent'])
total_spent_summary = {
    'Size' : total_spent_df['totalSpent'].count().item(), 
    'Mean' : total_spent_df['totalSpent'].mean().item(), 
    'Standard Deviation' : total_spent_df['totalSpent'].std().item(), 
    'Minimum' : total_spent_df['totalSpent'].min().item(), 
    'Maximum' : total_spent_df['totalSpent'].max().item()
}

q1, q3 = total_spent_df['totalSpent'].quantile(.25), total_spent_df['totalSpent'].quantile(.75)
iqr = q3 - q1
lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr

outliers = total_spent_df[(total_spent_df['totalSpent'] < lower_bound) | (total_spent_df['totalSpent'] > upper_bound)]
print('Summary for Receipts.totalSpent:\n', total_spent_summary)
print(f'{outliers.count().item()} outliers out of {total_spent_df["totalSpent"].count()}', '\n')


###### Find summary for values in Items.finalPrice, and check for outliers/negative values ######
finalPrice = cur.execute('SELECT finalPrice FROM Items WHERE finalPrice IS NOT NULL').fetchall()
finalPrice = [x[0] for x in finalPrice]
# print(finalPrice)

final_price_df = pd.DataFrame(finalPrice, columns=['finalPrice'])
final_price_summary = {
    'Size' : final_price_df['finalPrice'].count().item(), 
    'Mean' : final_price_df['finalPrice'].mean().item(), 
    'Standard Deviation' : final_price_df['finalPrice'].std().item(), 
    'Minimum' : final_price_df['finalPrice'].min().item(), 
    'Maximum' : final_price_df['finalPrice'].max().item()
}

q1, q3 = final_price_df['finalPrice'].quantile(.25), final_price_df['finalPrice'].quantile(.75)
iqr = q3 - q1
lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr

outliers = final_price_df[(final_price_df['finalPrice'] < lower_bound) | (final_price_df['finalPrice'] > upper_bound)]
print('Summary for Receipts.totalSpent:\n', final_price_summary)
print(f'{outliers.count().item()} outliers out of {final_price_df["finalPrice"].count()}', '\n')