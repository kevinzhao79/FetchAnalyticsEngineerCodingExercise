# q3.py
# QUERY: When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

# NOTE: While looking through the Receipt table, I could not find receipts with 'rewardsReceiptStatus' = 'Accepted'. The closest 
# status to this was 'Finished', which I am accepting as a synonym of 'Accepted'. 

import sqlite3
con = sqlite3.connect('./database/fetch.db')
cur = con.cursor()

res = cur.execute("""
SELECT rewardsReceiptStatus, ROUND(AVG(totalSpent), 2)
FROM Receipts
WHERE rewardsReceiptStatus = 'REJECTED'
OR rewardsReceiptStatus = 'FINISHED'
AND totalSpent NOT NULL
GROUP BY rewardsReceiptStatus
""")

print("When considering average spend from receipts with 'rewardsReceiptStatus' of 'Accepted' or 'Rejected', which is greater?\n", res.fetchall(), '\n') 

# Receipts with 'rewardsReceiptStatus' = 'Finished' have a greater average spend of 80.85, compared to 
# receipts with 'rewardsReceiptStatus' = 'Rejected' with an average spend of 23.33.