import sqlite3
con = sqlite3.connect('./fetch.db')
cur = con.cursor()

print(cur.execute("""

    SELECT r.id, i.brandCode, COUNT(*) AS itemCount
    FROM Receipts r JOIN Items i
    ON r.id = i.receiptId
    WHERE unixepoch(r.dateScanned) >= ? - ?
    GROUP BY r.id, i.brandCode

""").fetchall())