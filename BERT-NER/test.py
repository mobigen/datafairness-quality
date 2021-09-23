from bert import Ner
import pandas as pd
import numpy as np
import time

def convert_ner(ner_entity):
    '''
        O	Outside of a named entity
        B-MIS	Beginning of a miscellaneous entity right after another miscellaneous entity
        I-MIS	Miscellaneous entity
        B-PER	Beginning of a person’s name right after another person’s name
        I-PER	Person’s name
        B-ORG	Beginning of an organization right after another organization
        I-ORG	organization
        B-LOC	Beginning of a location right after another location
        I-LOC	Location
    '''
    result = None
    if "PER" in ner_entity:
        result = "PERSON"
    elif "MIS" in ner_entity:
        result = "MIS"
    elif "ORG" in ner_entity:
        result = "ORGANIZATION"
    elif "LOC" in ner_entity:
        result = "LOCATION"

    return result

model = Ner("out_base")
start = time.time()

df = pd.read_csv("/Users/cbc/DEV/Mobigen/datafairness-quality/sample_data/biopics.csv", header=0, dtype=str)
load_end = time.time()

print("load time : {}".format(load_end - start))

from pprint import pprint
result = {}
for column_name in df.columns:
    #column = df[column_name].sample(n = 70).replace(
    #            r"^\s*$", np.NaN, regex=True
    #        )
    column = df[column_name].replace(
                r"^\s*$", np.NaN, regex=True
            )
    
    print("[column name] : {}".format(column_name))
    start = time.time()
    column = np.where(
                column.isnull(), None, column
            )
    column = column[column != None]
    data = " ".join(column.tolist())
    output = []
    #try:
    output = model.predict_test(data)
    #except Exception as err:
    #print(err)

    result[column_name] = {}
    for res in output:
        label = convert_ner(res["tag"])
        if label == None:
            continue
        if label not in result[column_name]:
            result[column_name][label] = 0
        result[column_name][label] += 1
pprint(result)