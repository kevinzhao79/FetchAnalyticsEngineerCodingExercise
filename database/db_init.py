# db_init.py
# This script will drop the existing tables and re-instantiate them within the "fetch.db" file.

import sqlite3
con = sqlite3.connect('./fetch.db')
cur = con.cursor()

cur.execute('DROP TABLE IF EXISTS Receipts')
cur.execute('DROP TABLE IF EXISTS Items')
cur.execute('DROP TABLE IF EXISTS Users')
cur.execute('DROP TABLE IF EXISTS Brands')
cur.execute('DROP TABLE IF EXISTS CPG')

cur.execute("""
    CREATE TABLE Receipts(
        id VARCHAR(255) PRIMARY KEY, 
        bonusPointsEarned INT, 
        bonusPointsEarnedReason VARCHAR(255), 
        createDate DATETIME, 
        dateScanned DATETIME, 
        finishedDate DATETIME, 
        modifyDate DATETIME, 
        pointsAwardedDate DATETIME, 
        pointsEarned INT, 
        purchaseDate DATETIME, 
        purchasedItemCount INT, 
        rewardsReceiptStatus VARCHAR(255), 
        totalSpent FLOAT, 
        userId VARCHAR(255), 
        FOREIGN KEY (userId) REFERENCES Users(id)
)""")

cur.execute("""
    CREATE TABLE Items(
        id INT PRIMARY KEY, 
        barcode INT, 
        brandCode VARCHAR(255), 
        receiptId VARCHAR(255) NOT NULL, 
        description VARCHAR(255), 
        finalPrice FLOAT, 
        pointsEarned FLOAT, 
        quantityPurchased FLOAT, 
        partnerItemId INT NOT NULL, 
        FOREIGN KEY (barcode) REFERENCES Brands(barcode), 
        FOREIGN KEY (receiptId) REFERENCES Receipts(id)
)""")

cur.execute("""
    CREATE TABLE Users(
        id VARCHAR(255) PRIMARY KEY, 
        active BOOL NOT NULL, 
        createdDate DATETIME, 
        lastLogin DATETIME, 
        role VARCHAR(255) DEFAULT "consumer", 
        signUpSource VARCHAR(255), 
        state VARCHAR(2)
)""")

cur.execute("""
    CREATE TABLE Brands(
        id VARCHAR(255) PRIMARY KEY, 
        name VARCHAR(255) NOT NULL, 
        barcode VARCHAR(255) NOT NULL, 
        brandCode VARCHAR(255), 
        category VARCHAR(255), 
        categoryCode VARCHAR(255), 
        cpgId VARCHAR(255) NOT NULL, 
        topBrand BOOL, 
        FOREIGN KEY (cpgId) REFERENCES CPG(id)
)""")

cur.execute("""
    CREATE TABLE CPG(
        id VARCHAR(255) PRIMARY KEY, 
        ref VARCHAR(255) NOT NULL
)""")

cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print('Tables:', [x[0] for x in cur.fetchall()], '\n')

con.commit()
con.close()