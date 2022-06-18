import pandas as pd

v_path_file = "/users/andrerico/dev/ge/ge-pg/src/psa/CTDCGINT/CTD_chem_gene_ixns.csv"
DFR = pd.read_csv(v_path_file)
DFR_TG = DFR.iloc[:,4]
DFR_TG = DFR_TG.drop_duplicates()
DFR_TG.to_csv('saida.csv')