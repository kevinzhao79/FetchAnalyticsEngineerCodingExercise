# db_loading.py
# This file will load the data from "brands.json", "receipts.json", and "users.json" into the fetch.db tables

import sys
from re import sub
import json
import os

import sqlite3
con = sqlite3.connect('./fetch.db')
cur = con.cursor()

items = []
cpg = []

def jsonify(path):

    with open(path, 'r') as f:
        strings = f.readlines()

    for i, s in enumerate(strings):
        strings[i] = json.loads(s)

    return strings

def load_receipts(receipts):
    # Insert JSON data into Receipts table
    keys = ['bonusPointsEarned', 'bonusPointsEarnedReason', 'pointsEarned', 'purchasedItemCount', 'rewardsReceiptStatus', 'totalSpent', 'userId']
    
    for r in receipts:
        values = {key: r[key] if key in r else None for key in keys}

        values['id'] = r['_id']['$oid']
        values['createDate'] = r['createDate']['$date'] if r.get('createDate') else None
        values['dateScanned'] = r['dateScanned']['$date'] if r.get('dateScanned') else None
        values['finishedDate'] = r['finishedDate']['$date'] if r.get('finishedDate') else None
        values['modifyDate'] = r['modifyDate']['$date'] if r.get('modifyDate') else None
        values['purchaseDate'] = r['purchaseDate']['$date'] if r.get('purchaseDate') else None
        values['pointsAwardedDate'] = r['pointsAwardedDate']['$date'] if r.get('pointsAwardedDate') else None

        cur.execute("""
            INSERT INTO Receipts (
                id, bonusPointsEarned, bonusPointsEarnedReason, createDate, dateScanned, finishedDate, modifyDate, 
                pointsAwardedDate, pointsEarned, purchaseDate, purchasedItemCount, rewardsReceiptStatus, totalSpent, userId
            ) VALUES (
                ?, ?, ?, DATETIME(? / 1000, 'unixepoch'), DATETIME(? / 1000, 'unixepoch'), DATETIME(? / 1000, 'unixepoch'), 
                DATETIME(? / 1000, 'unixepoch'), DATETIME(? / 1000, 'unixepoch'), ?, DATETIME(? / 1000, 'unixepoch'), ?, ?, ?, ?
            )
        """, (
            values['id'], values['bonusPointsEarned'], values['bonusPointsEarnedReason'], values['createDate'], values['dateScanned'], 
            values['finishedDate'], values['modifyDate'], values['pointsAwardedDate'], values['pointsEarned'], values['purchaseDate'],
            values['purchasedItemCount'], values['rewardsReceiptStatus'], values['totalSpent'], values['userId']
        ))

        # Grab rewardsReceiptItemList data from Receipts.JSON and save for Items table
        if 'rewardsReceiptItemList' in r.keys():
            for item in r['rewardsReceiptItemList']:
                item['receiptId'] = values['id']
                items.append(item)

    print(cur.execute('SELECT COUNT(*) FROM Receipts').fetchall())

def load_items(items):
    # Insert Item data gathered from Receipts.JSON into Items table
    id_counter = 0
    keys = ['id', 'barcode', 'brandCode', 'receiptId', 'description', 'finalPrice', 'pointsEarned', 'quantityPurchased', 'partnerItemId']

    for i in items:

        values = {key : i[key] if key in i else None for key in keys}
        values['id'] = id_counter

        cur.execute("""
            INSERT INTO Items (id, barcode, brandCode, receiptId, description, finalPrice, pointsEarned, quantityPurchased, partnerItemId) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                values['id'], values['barcode'], values['brandCode'], values['receiptId'], values['description'], 
                values['finalPrice'], values['pointsEarned'], values['quantityPurchased'], values['partnerItemId']
            ))

        id_counter += 1

    print(cur.execute('SELECT COUNT(*) FROM Items').fetchall())

def load_users(users):

    keys = ['active', 'signUpSource', 'state']

    for u in users:
        values = {key : u[key] if key in u else None for key in keys}
        values['id'] = u['_id']['$oid']
        values['role'] = 'consumer'
        values['createdDate'] = u['createdDate']['$date'] if u.get('createdDate') else None
        values['lastLogin'] = u['lastLogin']['$date'] if u.get('lastLogin') else None

        if len(cur.execute(f"SELECT * FROM Users WHERE id = '{values['id']}'").fetchall()) == 0:
            cur.execute("""
                INSERT INTO Users (id, active, createdDate, lastLogin, role, signUpSource, state)
                VALUES (?, ?, DATETIME(? / 1000, 'unixepoch'), DATETIME(? / 1000, 'unixepoch'), ?, ?, ?)
            """, (
                values['id'], values['active'], values['createdDate'], values['lastLogin'], 
                values['role'], values['signUpSource'], values['state']
            ))

    print(cur.execute('SELECT COUNT(*) FROM Users').fetchall())

def load_brands(brands):

    keys = ['id', 'name', 'barcode', 'brandCode', 'category', 'categoryCode', 'topBrand']

    for b in brands:
        values = {key : b[key] if key in b else None for key in keys}
        values['cpgId'] = b['cpg']['$id']['$oid']
        values['cpgRef'] = b['cpg']['$ref']

        cur.execute("""
            INSERT INTO Brands (id, name, barcode, brandCode, category, categoryCode, cpgId, topBrand)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            values['id'], values['name'], values['barcode'], values['brandCode'], values['category'], 
            values['categoryCode'], values['cpgId'], values['topBrand']
        ))

        cpg.append({'id' : values['cpgId'], 'ref' : values['cpgRef']})

    print(cur.execute('SELECT COUNT(*) FROM Brands').fetchall())

def load_cpg(cpg):
    
    for c in cpg:
        id = c['id']

        # If a CPG with the same ID hasn't been inserted yet, then insert it
        if len(cur.execute(f"SELECT * FROM CPG WHERE id = '{id}'").fetchall()) == 0:
            cur.execute("""
                INSERT INTO CPG (id, ref) VALUES (?, ?)
            """, (
                c['id'], c['ref']
            ))

    print(cur.execute('SELECT COUNT(*) FROM CPG').fetchall())

receipts_path = '../data/receipts.json'
users_path = '../data/users.json'
brands_path = '../data/brands.json'

receipts = jsonify(receipts_path)
users = jsonify(users_path)
brands = jsonify(brands_path)

load_receipts(receipts)
load_items(items)
load_users(users)
load_brands(brands)
load_cpg(cpg)

con.commit()
con.close()