import pymysql
import xml.etree.ElementTree as ET
import json 
import pandas as pd

class DatabaseXML:
    def __init__(self, host, user, password, database):
        self.conn = pymysql.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.conn.cursor()

    def close(self):
        self.conn.close()

    def store_xml_data(self, ticker, accession_number, filing_date, report_date, primary_document, primary_doc_description, xml_data):
        query = """INSERT INTO sec_data 
                (ticker, accession_number, filing_date, report_date, primary_document, primary_doc_description, xml_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""

        self.cursor.execute(query, (ticker,accession_number, filing_date, report_date, primary_document, primary_doc_description, xml_data))
        self.conn.commit()

    def get_ticker_data(self, ticker):
        query = "SELECT * FROM sec_data WHERE ticker = %s"
        self.cursor.execute(query, (ticker,))

        columns = [desc[0] for desc in self.cursor.description]
        
        while True:
            row = self.cursor.fetchone()
            if row is None:
                break
            yield dict(zip(columns, row))




