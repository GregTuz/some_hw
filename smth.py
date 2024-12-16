import re
from fastapi import FastAPI

s = 'Papapeva_Gemabody?niggers'
print(re.split(r'_|\?', s))
print(s.split('_|\?'))
