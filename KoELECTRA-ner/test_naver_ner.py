from typing import Mapping
from transformers import ElectraTokenizer, ElectraForTokenClassification
from ner_pipeline import NerPipeline
from pprint import pprint
import pandas as pd
import numpy as np
import operator

def read_input_csv():
    file_path = "sample_data/company_100.csv"
    df = pd.read_csv(file_path, header=0, dtype=str)

    column_names = []
    columns = []
    for column_name in df.columns:
        column = np.where(
                df[column_name].isnull(), None, df[column_name]
            )
        columns.append(column[column != None])
        column_names.append(column_name)
    return column_names, columns

tokenizer = ElectraTokenizer.from_pretrained("monologg/koelectra-small-finetuned-naver-ner")
model = ElectraForTokenClassification.from_pretrained("monologg/koelectra-small-finetuned-naver-ner")

ner = NerPipeline(model=model,
                  tokenizer=tokenizer,
                  ignore_special_tokens=True)

column_names, columns = read_input_csv()

answer, stats = ner.ner(columns, column_names)
index = 1
for key, value in stats.items():
    ner = None
    sort_stats = sorted(value.items(), key=operator.itemgetter(1), reverse=True)
    if len(sort_stats) != 0:
        ner = sort_stats[0][0]
    print("\n{}.column name : {} ({})".format(index, key, ner))
    print("ner : {}".format(sort_stats))
    index += 1
