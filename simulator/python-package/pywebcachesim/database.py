import pandas as pd
import numpy as np
from pymongo import MongoClient


def load_reports(n_warmup, dburi, dbcollection, **kwargs):
    client = MongoClient(dburi)
    db = client.get_database()
    collection = db[dbcollection]

    rows = []
    for r in collection.find(kwargs):
        try:
            row = {
                'n_warmup': n_warmup,
            }
            for k, v in r.items():
                if k in {'_id'}:
                    continue
                elif k in {'cache_size', 'uni_size', 'segment_window', 'memory_window', 'n_early_stop'}:
                    try:
                        row[k] = int(v)
                    except Exception as e:
                        row[k] = np.nan
                else:
                    row[k] = v
            n_skip_segment = int(n_warmup/row['segment_window'])  #assume mod == 0
            row['byte_miss_ratio'] = sum(row['segment_byte_miss'][n_skip_segment:])/sum(row['segment_byte_req'][n_skip_segment:])
            row['object_miss_ratio'] = sum(row['segment_object_miss'][n_skip_segment:])/sum(row['segment_object_req'][n_skip_segment:])
        except Exception as e:
            print(f'parsing fail for {str(r)[:200]}: {str(e)[:200]}')
            continue
        rows.append(row)
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    if 'cache_size' in df.columns:
        df = df.sort_values(['cache_size', 'byte_miss_ratio']).reset_index(drop=True)
    else:
        df = df.sort_values('byte_miss_ratio').reset_index(drop=True)
    return df
