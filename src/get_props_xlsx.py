import pandas as pd
import re

file_path = '/scratch/hpc-prf-lola/albert/get_props/data/WDC_props/xlsx/html-rdfa_2023-12.xlsx'
df = pd.read_excel(file_path, sheet_name='prop')

df.columns = df.columns.str.strip()

def correct_concat(uri):
    last_https = uri.rfind('https://')
    last_http = uri.rfind('http://')

    # take the last part of concatenated URI
    if last_https != -1 and last_http != -1:
        if last_https > last_http:
            return 'https://' + uri.split('https://')[-1]
        else:
            return 'http://' + uri.split('http://')[-1]
        
    # remove multiple '#' in URI
    if uri.count('#') > 1:
        uri = uri.split('#', 1)[0] + '#' + uri.split('#', 1)[1].split('/')[0]
        
    return uri

def create_label(prop_uri):
    if '#' in prop_uri:
        last_part = prop_uri.split('#')[-1]
    else:
        last_part = prop_uri.split('/')[-1]

    cleaned_part = correct_concat(last_part)
    cleaned_part = re.sub(r'[-.:_/\[\],]', ' ', cleaned_part)

    return ' '.join([word.capitalize() for word in re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_part).split()])

nt_lines = []

for _, row in df.iterrows():
    prop = row['prop'].replace('"', '')

    if 'http' not in prop or 'skos' in prop:
        continue

    label = create_label(prop)
    nt_line = f"<{prop}> <http://www.w3.org/2000/01/rdf-schema#label> \"{label}\" ."
    nt_lines.append(nt_line)

output_file = "/scratch/hpc-prf-lola/albert/get_props/data/WDC_props/WDC_properties.nt"
with open(output_file, 'a') as f:
    f.write("\n".join(nt_lines) + "\n")

print(f"N-Triples wtitten to {output_file}")
