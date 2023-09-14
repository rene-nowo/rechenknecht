import pandas as pd
import requests
from sec_api import ExtractorApi

# SEC API is a app you pay for

class SEC_API:
    api_key = "80d9f5198dc9029a9385ac5b3d714df56356cb9a936c21ee32902371899bc309"

    def get_form10k(self, company_name: str):

        payload = {
            "query": {"query_string": {"query": 'formType:"10-K"'}},
            "from": "0",
            "size": "20",
            "sort": [{"filedAt": {"order": "desc"}}],
        }


extractor_api = ExtractorApi(SEC_API.api_key)

# Tesla 10-K filing
filing_url = "https://www.sec.gov/Archives/edgar/data/1318605/000156459021004599/tsla-10k_20201231.htm"

# get the standardized and cleaned text of section 1A "Risk Factors"
section_text = extractor_api.get_section(filing_url, "8", "text")
print(section_text)
# get the original HTML of section 7
# "Management’s Discussion and Analysis of Financial Condition and Results of Operations"
section_html = extractor_api.get_section(filing_url, "7", "html")
