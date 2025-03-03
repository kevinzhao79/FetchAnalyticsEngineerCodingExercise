# q5.py
# QUERY: Which brand has the most spend among users who were created within the past 6 months?

# NOTE: I found that the most recently created account was created Feb 12, 2021. Today's date is March 1, 2025, meaning no accounts
# in this dataset were created within the past 4 years. To rectify this, I will be taking "today" as the most recently created account, 
# which is 2021-02-12 14:11:06, and finding accounts within 6 months of this datetime.

import sqlite3
con = sqlite3.connect('./database/fetch.db')
cur = con.cursor()

six_months_in_unix = 2629743 * 6 # Approximately 6 months in seconds of UNIX time
most_recent_created_account_date = cur.execute("""
    SELECT strftime('%s', dateScanned) 
    FROM Receipts 
    ORDER BY dateScanned DESC 
    LIMIT 1
""").fetchone()[0]

res = cur.execute(f"""
WITH temp AS (
    WITH recent_users AS (
        SELECT id FROM USERS 
        WHERE unixepoch(createdDate) > ? - ?
    )
    SELECT i.brandCode, SUM(i.finalPrice) AS brandCodeSum
    FROM Receipts r JOIN recent_users u
    ON r.userId = u.id
    JOIN Items i
    ON r.id = i.receiptId
    GROUP BY i.brandCode
)
SELECT b.name AS BrandName, SUM(temp.brandCodeSum) AS TotalSales
FROM Brands b JOIN temp ON
b.brandCode = temp.brandCode
GROUP BY b.name
ORDER BY TotalSales DESC
LIMIT 1
""", (
    most_recent_created_account_date, six_months_in_unix
))

print('Which brand has the most spend among users who were created within the past 6 months?\n', res.fetchall(), '\n')