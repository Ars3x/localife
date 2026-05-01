import os
import math
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncpg
import asyncio
from dotenv import load_dotenv
from ndvi import init_ee, get_location_score

load_dotenv()

app = FastAPI(title="LocaLife API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальный кэш объектов
cached_objects: List[Dict[str, Any]] = []

# Параметры модели
CATEGORY_PARAMS = {
    "school":           {"alpha": 1.0, "beta": 2.0, "weight": 10},
    "clinic":           {"alpha": 1.0, "beta": 1.5, "weight": 7},
    "transport_stop":   {"alpha": 1.0, "beta": 1.5, "weight": 3},
    "mcd":              {"alpha": 1.0, "beta": 1.5, "weight": 8},
    "railway":          {"alpha": 0.8, "beta": 1.0, "weight": 4},
    "new_building":     {"alpha": 0.5, "beta": 0.5, "weight": 1},
}

TABLE_MAP = {
    "school":           ("schools",         "location",     "school_name"),
    "clinic":           ("policlinics_v",   "location",     "fullname"),
    "transport_stop":   ("nazem_transport", "wkt_string",   "stop_name"),
    "mcd":              ("mtsd",            "wkt_string",   "StationName"),
    "railway":          ("railway_station", "wkt_string",   "Name"),
    "new_building":     ("new_building_2",  "wkt_correct",  "address"),
}

def distance_decay(d, alpha): return math.exp(-alpha * d)
def saturation(x, beta): return 1.0 - math.exp(-beta * x)

def haversine_distance(lat1, lng1, lat2, lng2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

class ComfortRequest(BaseModel):
    lat: float
    lng: float
    radius: float = 1.2
    scenario: str = "family"

class ComfortResponse(BaseModel):
    objects: List[Dict[str, Any]]
    totalScore: float
    maxScore: float
    percentage: float
    categoryScores: Dict[str, float]
    ndvi: Optional[float] = None
    

@app.on_event("startup")
async def startup():
    """Загружаем все объекты из БД в память."""
    global cached_objects
    pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "nis"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "nis2026"),
        min_size=1,
        max_size=1
    )
    async with pool.acquire() as conn:
        for cat, (table, geom_col, name_col) in TABLE_MAP.items():
            # Простейший запрос без всяких условий, всё отфильтруем в Python
            query = f'SELECT "{name_col}" AS name, "{geom_col}"::text AS wkt FROM {table}'
            rows = await conn.fetch(query)
            for row in rows:
                wkt = row["wkt"]
                if not wkt or not wkt.startswith("POINT("):
                    continue
                try:
                    # Парсим "POINT(lng lat)"
                    coords = wkt[6:-1].strip().split()
                    if len(coords) != 2:
                        continue
                    lng = float(coords[0])
                    lat = float(coords[1])
                    cached_objects.append({
                        "type": cat,
                        "name": row["name"],
                        "lat": lat,
                        "lng": lng
                    })
                except Exception:
                    continue
    await pool.close()
    try:
        init_ee(project="localife-ndvi", key_path="/home/user1/nis-project/localife/backend/.config/earthengine/localife-ndvi.json")
    except Exception as e:
        print(f"[WARN] Google Earth Engine не инициализирован: {e}")
    print(f"Cache loaded: {len(cached_objects)} objects")

@app.post("/api/comfort", response_model=ComfortResponse)
async def get_comfort(req: ComfortRequest):
    try:
        # Фильтруем объекты в радиусе
        objects_in_radius = []
        for obj in cached_objects:
            dist = haversine_distance(req.lat, req.lng, obj["lat"], obj["lng"])
            if dist <= req.radius:
                obj_copy = obj.copy()
                obj_copy["distance"] = dist
                objects_in_radius.append(obj_copy)

        # Группировка и расчёт комфорта
        objects_by_cat = {cat: [] for cat in CATEGORY_PARAMS}
        for obj in objects_in_radius:
            if obj["type"] in objects_by_cat:
                objects_by_cat[obj["type"]].append(obj)

        total_comfort = 0.0
        category_scores = {}
        for cat, params in CATEGORY_PARAMS.items():
            alpha = params["alpha"]
            beta = params["beta"]
            weight = params["weight"]
            raw = sum(distance_decay(o["distance"], alpha) for o in objects_by_cat[cat])
            score = saturation(raw, beta)
            category_scores[cat] = score
            total_comfort += weight * score

        max_possible = sum(p["weight"] for p in CATEGORY_PARAMS.values())
        percentage = min(100.0, round(total_comfort / max_possible * 100))
        
        try:
            ndvi_value = await asyncio.to_thread(
                get_location_score,
                req.lat, req.lng,
                buffer_m=500,
                max_cloud_pct=60.0
            )
        except Exception as e:
            ndvi_value = None
            print(f"Ошибка получения NDVI: {e}")

        return ComfortResponse(
            objects=objects_in_radius,
            totalScore=total_comfort,
            maxScore=max_possible,
            percentage=percentage,
            categoryScores=category_scores,
            ndvi=ndvi_value
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)  # reload=False важно для кэша