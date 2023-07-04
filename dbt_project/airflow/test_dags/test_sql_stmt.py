table_list = ["ext_dvsn", "ext_chrg_info", "ext_lcl_crrncy", "ext_pos_shop", "ext_pos_trmnl"]
SQL_REFRESH_STATEMENT = "ALTER EXTERNAL TABLE EXTERNAL_DB.STAGE.%(table_name)s REFRESH"
SQL_LIST = [ SQL_REFRESH_STATEMENT % {"table_name": table_name}  for table_name in table_list ]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)

print(SQL_MULTIPLE_STMTS)