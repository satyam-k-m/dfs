list_of_files = ['pos/ind/INS.DI.MCS_TRGGR.MAC', 'pos/mac/INS.DI.MCS_TRGGR.MAC', 'sales/mac/INS.DI.MCS_TRGGR.MAC']
for tggr_file in list_of_files:
    division = tggr_file.split("/")[1]
    source = tggr_file.split("/")[0]

    print(f"{division}_{source}")