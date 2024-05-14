import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support

def build_queries(row, source):
    
    """
    Datamodel
    ```
    * (Item {id: source_id})-[:MATCH]->(Item {id: target_id})
    * (Item)-[:HAS_NAME]->(:Name {value: [original name]})
    * (Item)-[:HAS_DESCRIPTION]->(:Description)
    * (Name)-[:HAS_WORD]->(:Word {value: [preprocesed value], raw_value: [original_value]})
    * (Description)-[:HAS_WORD]->(:Word {value: [preprocesed value], raw_value: [original_value]})
    * (Item)-[:HAS_MANUFACTURER]->(:Manufacturer)

    ```
    """
    
    name = row['name'].replace("'", "")
    description = str(row['description']).replace("'", "")
    manufacturer = row.get('manufacturer', None)
    queries = []
    
    queries.append(f"MERGE (i:Item {{subject_id: {row['subject_id']}, source: '{source}'}})")
    queries.append(f"MERGE (n:Name {{value: '{name}'}})")
    queries.append(f"MERGE (n:Description {{value: '{description}'}})")    
    queries.append(f"MERGE (n:Manufacturer {{value: '{manufacturer}'}})")
    
    queries.append(f"MATCH (i:Item) WHERE i.subject_id = {row['subject_id']} MATCH (n:Name) WHERE n.value = '{name}' MERGE (i)-[:HAS_NAME]->(n)")
    queries.append(f"MATCH (i:Item) WHERE i.subject_id = {row['subject_id']} MATCH (n:Description) WHERE n.value = '{description}' MERGE (i)-[:HAS_DESCRIPTION]->(n)")    
    
    if manufacturer:
        manufacturer = str(manufacturer).strip().replace("'", "\\'")    
        #print("manufacturer:", manufacturer)
        queries.append(f"MATCH (i:Item) WHERE i.subject_id = {row['subject_id']} MATCH (n:Manufacturer) WHERE n.value = '{manufacturer}' MERGE (i)-[:HAS_MANUFACTURER]->(n)")    
        
    # words in names
    for word in str(row['name']).split(" "):
        word = word.replace("'", "\\'")
        queries.append(f"MERGE (w:Word {{value: '{word}'}})")
        queries.append(f"MATCH (n:Name) WHERE n.value = '{name}' MATCH (w:Word) WHERE w.value='{word}' MERGE (n)-[:HAS_WORD]->(w)")
    
    # words in descriptions
    for word in str(row['description']).split(" "):
        word = word.replace("'", "\\'")
        queries.append(f"MERGE (w:Word {{value: '{word}'}})")
        queries.append(f"MATCH (n:Description) WHERE n.value = '{description}' MATCH (w:Word) WHERE w.value='{word}' MERGE (n)-[:HAS_WORD]->(w)")

    return queries


def build_match_queries(row):
    queries = []
    
    if row['matching'] == True:
        rel_type = "IS_MATCH"
    else:
        rel_type = "NO_MATCH"
    
    queries.append(f"""
    MATCH (i1:Item) WHERE i1.subject_id = {row['source_id']}
    MATCH (i2:Item) WHERE i2.subject_id = {row['target_id']}
    MERGE (i1)-[:{rel_type}]->(i2)
    """)
    return queries


def get_model_results(graph, df, query, model_name, threshold, evaluated_on, debug=False):
    results = []

    if debug:
        print(query)
    
    df_p = pd.DataFrame(graph.query(query))

    if len(df_p) > 0:

        df_eval_p = df.merge(df_p, left_on=['source_id', 'target_id'], right_on=['i1.subject_id', 'i2.subject_id'], how='left')
        df_eval_p['p'] = df_eval_p['i1.subject_id'] > 0

        prec, recall, fscore, support = precision_recall_fscore_support(df_eval_p['matching'], df_eval_p['p'], average='binary')

        # store errors
        cond_p1 = df_eval_p['p'] == True
        cond_y1 = df_eval_p['matching'] == True

        df_tp = df_eval_p[cond_p1 & cond_y1]
        df_fp = df_eval_p[cond_p1 & ~cond_y1]
        df_tn = df_eval_p[~cond_p1 & ~cond_y1]
        df_fn = df_eval_p[~cond_p1 & cond_y1]

        tp = len(df_tp)
        fp = len(df_fp)
        fn = len(df_fn)
        tn = len(df_tn)

        results.append({'model': model_name, 'threshold': threshold, 'prec': np.round(prec, 5), 'recall': np.round(recall, 5), 'fscore': np.round(fscore, 5), 'evaluated_on': evaluated_on, 'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn})

    df_results = pd.DataFrame(results).sort_values('fscore', ascending=False).reset_index(drop=True)
    return df_results, {'df_tp': df_tp, 'df_fp': df_fp, 'df_tn': df_tn, 'df_fn': df_fn}