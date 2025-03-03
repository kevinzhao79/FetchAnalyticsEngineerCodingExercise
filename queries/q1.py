# q1.py
# QUERY: What are the top 5 brands by receipts scanned for most recent month?

# NOTE: The most recent receipt scanned from this dataset was March 1, 2021. To keep this query relevant, I will 
# move "the most recent month" to be a month from March 1, 2021. 

# NOTE: This query only returns 1 brand, due to Items and Brands having null brandCode values which prevents a complete JOIN. 

import sqlite3
con = sqlite3.connect('./database/fetch.db')
cur = con.cursor()

one_month_in_unix = 2629743 # Approximately 1 month in seconds of UNIX time
most_recent_receipt_scanned = cur.execute("""
    SELECT strftime('%s', dateScanned)
    FROM Receipts 
    ORDER BY dateScanned DESC 
    LIMIT 1
""").fetchone()[0]

highest_selling = cur.execute("""
WITH temp AS (
    SELECT r.id, i.brandCode, COUNT(*) AS itemCount
    FROM Receipts r JOIN Items i
    ON r.id = i.receiptId
    WHERE unixepoch(r.dateScanned) >= ? - ?
    GROUP BY r.id, i.brandCode
)
SELECT b.name AS Brand, SUM(temp.itemCount > 0) AS NumReceipts
FROM Brands b JOIN temp
ON b.brandCode = temp.brandCode
GROUP BY b.name
ORDER BY NumReceipts DESC
LIMIT 5
""", (
    most_recent_receipt_scanned, one_month_in_unix
))

print('What are the top 5 brands by receipts scanned for most recent month?\n', highest_selling.fetchall(), '\n')

