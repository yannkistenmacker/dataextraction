from urllib.parse import urlsplit, parse_qs
import csv

url = """https://www.mydomain.com/page-name?utm_con
tent=textlink&utm_medium=social&utm_source=twit
ter&utm_campaign=fallsale"""

split_url = urlsplit(url)
params = parse_qs(split_url.query)
parsed_url = []
all_urls = []

#domain
parsed_url.append(split_url.netloc)

#url path
parsed_url.append(split_url.path)

#utm
parsed_url.append(params['utm_content'][0])
parsed_url.append(params['utm_medium'][0])
parsed_url.append(params['utm_source'][0])
parsed_url.append(params['utm_campaign'][0])

all_urls.append(parsed_url)

export_file = "export_file_parsed.csv"

with open(export_file, 'w') as fp:
    csvw = csv.writer(fp, delimiter='|')
    csvw.writerows(all_urls)
fp.close()