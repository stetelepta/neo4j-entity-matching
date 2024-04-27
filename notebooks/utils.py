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
