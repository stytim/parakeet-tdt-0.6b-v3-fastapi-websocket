from fastapi import FastAPI
from stream_routes import router as ws_router
app = FastAPI()
app.include_router(ws_router)
print([r.path for r in app.routes])
