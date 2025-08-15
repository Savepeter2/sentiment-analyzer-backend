import argparse
from routers.mentions import router as mentions_router
from routers.analysis import router as analysis_router
from routers.comparison import router as comparison_router
from routers.recommender import router as recommender_router
from fastapi import FastAPI, Depends, HTTPException, status, Request
import uvicorn

try:
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    port = args.port
except:
    print("here")
    pass


app = FastAPI()
app.include_router(mentions_router)
app.include_router(analysis_router)
app.include_router(comparison_router)
app.include_router(recommender_router)

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)