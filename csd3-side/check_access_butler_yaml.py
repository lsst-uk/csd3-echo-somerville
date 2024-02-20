import os
import yaml
try:
	from yaml import CFullLoader as FullLoader
except ImportError:
	from yaml import FullLoader

print(FullLoader)

butler_yaml = '/rds/project/rds-lT5YGmtKack/ras81/butler_full_20221201/exports_deepCoadd_calexp_viking_20230525.yaml'

with open(butler_yaml, 'r') as byaml:
	print(yaml.load(byaml,yaml.UnsafeLoader))
