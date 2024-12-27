from fastapi import FastAPI, HTTPException, Depends
import uvicorn
import re
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


def validate_data(data:str) -> list:
	pattern = r"^[^;]*;[^;]+;[^;]*$"
	if re.match(pattern, data):
		return True
	else:
		return False


def split_input(data:str) -> list:
	return  re.split(r';', data)


@app.post("/items")
def create_item(data:str):
	if validate_data(data):
		cursor.execute(f"""
		DO $$
			BEGIN
	            IF NOT EXISTS (select * from todo_data where title = '{split_input(data)[0]}') THEN
		        insert into todo_data (title, description, completed) values ('{split_input(data)[0]}', '{split_input(data)[1]}', false);
		    END IF;
		END $$;
		""")
		conn.commit()
	else:
		raise HTTPException(status_code=400, detail="Bad input")


@app.get("/items")
def get_items() -> str:
	result = ''
	cursor.execute(f"""select * from todo_data""")
	rows = cursor.fetchall()
	for row in rows:
		row_data = ' | '.join(str(item) for item in row)
		result += f'| {row_data} |'

	return result


@app.get("/items/{item_id}")
def get_item(item_id:str) -> str:
	cursor.execute(f"""select * from todo_data where title = '{item_id}'""")
	data = cursor.fetchone()
	if len(data) == 0:
		raise HTTPException(status_code=400, detail="Issue not found")

	return str(data)


@app.put("/items/{item_id}")
def update_item(new_data: str):
	if validate_data(new_data):
		cursor.execute(f"""
    DO $$
    BEGIN
        IF EXISTS (SELECT * FROM todo_data WHERE title = '{split_input(new_data)[0]}') THEN
            UPDATE todo_data 
            SET completed = TRUE, description = '{split_input(new_data)[1]}'
            WHERE title = '{split_input(new_data)[0]}';
        END IF;
    END $$;
""")
		conn.commit()
	else:
		raise HTTPException(status_code=400, detail="Bad input")
		pass


@app.delete("/items/{item_id}")
def delete_item(item_id: str):
	cursor.execute(f"""delete from todo_data where title = '{item_id}'""")


if __name__ == "__main__":
	uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)