
from re import match
prefix= "output"
wildcard = "[a-zA-Z0-9_]*trgg[a-zA-Z0-9]*" # */images/*.jpg
matched_files = match(string="in_mac_trgg_data", pattern=wildcard)

print(matched_files)
