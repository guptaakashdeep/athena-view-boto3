import re
import boto3
import json
from base64 import b64encode, decode
from typing import List, Dict


def get_view_details(content: str, agg_cols: bool = False) -> tuple:
    view_nm_rgx = r"VIEW [IF NOT EXISTS]*\s*`(\w+)[\.](\w+)`"
    slct_stmt = r"\((.*)\)"

    view_match = re.search(view_nm_rgx, content, flags=re.IGNORECASE)
    if view_match:
        schema, view = view_match.groups()

    select_match = re.search(slct_stmt, content.replace("\n", " ").strip(), flags=re.IGNORECASE)
    if select_match:
        select_stmt=select_match.groups()[0].strip()
        cols_with_sel, _, source = re.split(r"\s+(FROM)\s+", select_stmt, flags=re.IGNORECASE)
        cols_match = re.search(r"^(SELECT)\s+(.*)", cols_with_sel, flags=re.IGNORECASE)
        if cols_match:
            cols = cols_match.groups()[1].split(",")
    return schema, view, source, cols, select_stmt


def filter_cols_list(cols: list, tgt_cols: set) -> str:
    view_cols_map = list(filter(lambda x: x["Name"] in tgt_cols, cols))
    return view_cols_map


def create_presto_view_req(sql: str, schema: str, cols_list: List[Dict[str, str]]) -> str:
    template = f'{{"originalSql":"{sql}","catalog":"awsdatacatalog","schema":"{schema}","columns":{json.dumps([{"name":c["Name"],"type": "varchar" if c["Type"] == "string" else c["Type"]} for c in cols_list])}}}'
    return template


def get_create_view_request(name: str, cols_list: List[Dict[str, str]], b64_encoded_query: str) -> dict:
    request = {
        "Name": name,
        "Parameters": {"comment":"Presto View", "presto_view": "true"},
        "StorageDescriptor" : {
            "Columns" : cols_list,
            "SerdeInfo": {}
        },
        "TableType": "VIRTUAL_VIEW",
        "ViewOriginalText": "/* Presto View: {} */".format(b64_encoded_query)
    }
    return request


def execute(content: str):
    glue = boto3.client("glue", region_name='eu-west-1')
    db, view_name, src_tbl, view_cols, select_stmt = get_view_details(content)
    src_db, tname = src_tbl.split(".")
    try:
        # drops existing vies by default
        drop_view = True
        # getting datatypes for columns in vies
        response = glue.get_table(DatabaseName=src_db, Name=tname)
        table_columns = response["Table"]["StorageDescriptor"]["Columns"] + response["Table"]["PartitionKeys"]
        cols_w_dtype = filter_cols_list(table_columns, set([col.strip().lower() for col in view_cols]))
        view_query = create_presto_view_req(select_stmt, db, cols_w_dtype)
        b64_enc_vw_query = b64encode(view_query.encode('ascii')).decode('utf-8')
        table_input = get_create_view_request(view_name, cols_w_dtype, b64_enc_vw_query)
        # check if the view already exists
        try:
            glue.get_table(DatabaseName=db, Name=view_name)
        except Exception as e:
            if type(e).__name__ == "EntityNotFoundException":
                drop_view = False
            else:
                raise e
        if drop_view:
            drop_response = glue.delete_table(DatabaseName=db, Name=view_name)
            if drop_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                print(f"{view_name} View has been dropped.")
        # create view
        response = glue.create_table(DatabaseName=db, TableInput=table_input)
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"{view_name} View has been created.")
        else:
            raise Exception(response)
    except Exception as e:
        raise e

if __name__ == "__main__":
    # This can be any create view statement string. Can be read from a file too.
    # main things to remember is:
    #   1. table name should be in tics (`table_name`)
    #   2. Select statement should be in round brackets ()
    sql_str = """CREATE VIEW `srtd_detail.fe_view_m` AS ( 
        SELECT deal_id, 
        drawn_ledger_exp, 
        jurisdiction_type, 
        batch_type, 
        day_rk, 
        run_rk from srtd_detail.fct_exposures_ft2
        """
    execute(sql_str)
    