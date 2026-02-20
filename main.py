from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "Lake Projections API is running"}
