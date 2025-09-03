#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("companies.db")
cursor = conn.cursor()
cursor.execute("SELECT company_name, domain FROM company_domains WHERE domain != 'example.com'")
results = cursor.fetchall()
print(f"Total domains: {len(results)}")
for row in results:
    print(f"- {row[1]} ({row[0]})")
