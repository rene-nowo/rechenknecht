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

    def store_results(self, ticker, last_stock_price, last_book_value, kbgv, kgv, eps, roi, div_rate, equity_ratio, roa, ebit_margin, free_cash_flow_ratio, currency="USD", ev_ebit_ratio=0, rv_ebit_ratio=0):
        query = """INSERT INTO results 
                (ticker, last_stock_price, last_book_value, kbgv, kgv, eps, roi, div_rate, equity_ratio, roa, ebit_margin, free_cash_flow_ratio, currency, ev_ebit_ratio, rv_ebit_ratio) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        self.cursor.execute(query, (ticker, last_stock_price, last_book_value, kbgv, kgv, eps, roi, div_rate, equity_ratio, roa, ebit_margin, free_cash_flow_ratio, currency, ev_ebit_ratio, rv_ebit_ratio))
        self.conn.commit()




