import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers.routers import router


app = FastAPI()

# origins = [
#     "http://localhost:3000",
#     "http://localhost:3001",
#     "http://localhost:3002",
#     "http://localhost:8000",
# ]
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


def main():
    uvicorn.run("api:app", host="0.0.0.0", port=2712, reload=True)


if __name__ == "__main__":
    main()
