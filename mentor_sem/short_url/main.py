import string
from random import random
from fastapi import FastAPI, HTTPException, Depends
import uvicorn
import psycopg2

conn = psycopg2.connect(
	dbname='app_db',
	user='user',
	password='password',
	host='db',
	port=5432
)

cursor = conn.cursor()
app = FastAPI()

def generate_short_id(length=6):
	chars = string.ascii_letters + string.digits
	return "".join(random.choice(chars) for _ in range(length))


@app.post("/shorten")
def shorten_url(item:str):
	# Генерируем уникальный short_id
	for _ in range(10):
		short_id = generate_short_id()

	return short_id

if __name__ == "__main__":
	uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)