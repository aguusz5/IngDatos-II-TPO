"""
================================================================================
  GAMENEXUS — Módulo MongoDB: Catálogo de Videojuegos
  Trabajo Práctico Integrador — Ingeniería de Datos II — UADE 2026
  Motor: MongoDB  |  Dataset: Steam Games Dataset (FronkonGames, Kaggle)
  Lenguaje: Python 3.12  |  Librería: pymongo
================================================================================

Dataset:
  https://www.kaggle.com/datasets/fronkongames/steam-games-dataset
  Descargar games.json y colocarlo en la misma carpeta que este script.

Requisitos:
  pip install pymongo pandas

Iniciar MongoDB local antes de ejecutar:
  mongod  (o usar MongoDB Compass / MongoDB Atlas)
================================================================================
"""

import json
import time
from pathlib import Path
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
import pandas as pd


# ─────────────────────────────────────────────
#  CONFIGURACIÓN DE CONEXIÓN
# ─────────────────────────────────────────────

MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "gamenexus"
COLLECTION = "games"

# Ruta al archivo descargado del dataset
DATASET_PATH = Path(__file__).parent / "games.json"

# Límite de documentos a cargar (None = todos los ~122k)
# Usar 5000 para demos rápidas, None para producción
LOAD_LIMIT = 5000


# ─────────────────────────────────────────────
#  CONEXIÓN
# ─────────────────────────────────────────────

def conectar():
    """Establece conexión con MongoDB y retorna la colección de juegos."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")          # Verifica que el servidor responde
    db = client[DB_NAME]
    col = db[COLLECTION]
    print(f"✓ Conectado a MongoDB → base: '{DB_NAME}' | colección: '{COLLECTION}'")
    return client, col


# ─────────────────────────────────────────────
#  CARGA DEL DATASET
# ─────────────────────────────────────────────

def transformar_documento(app_id: str, datos: dict) -> dict:
    """
    Transforma un registro del dataset Steam al esquema de GameNexus.
    
    Decisión de diseño: se embeben géneros, categorías y plataformas
    como subdocumentos/arrays dentro del documento principal.
    Esto aprovecha el modelo documental de MongoDB y evita JOINs.
    """
    # El dataset trae precios en centavos USD → convertir a float
    price_raw = datos.get("price", 0)
    try:
        price = round(float(price_raw), 2)
    except (ValueError, TypeError):
        price = 0.0

    # Ratings: calcular porcentaje positivo si hay reviews
    pos  = int(datos.get("positive", 0))
    neg  = int(datos.get("negative", 0))
    total_reviews = pos + neg
    rating_pct = round((pos / total_reviews) * 100, 1) if total_reviews > 0 else None

    doc = {
        # Identificador único de Steam
        "steam_appid": app_id,

        # Información básica
        "name":        datos.get("name", ""),
        "type":        datos.get("type", "game"),       # game, dlc, demo, etc.
        "is_free":     datos.get("is_free", False),
        "price_usd":   price,

        # Fechas
        "release_date": datos.get("release_date", None),

        # Descripción — campo de texto completo para búsquedas
        "description": datos.get("short_description", ""),
    
        # Desarrolladores y publishers como arrays (un juego puede tener varios)
        "developers":  datos.get("developers", []),
        "publishers":  datos.get("publishers", []),

        # Taxonomía del juego — subdocumento embebido
        "taxonomy": {
            "genres":     [g.get("description", g) if isinstance(g, dict) else g
                           for g in datos.get("genres", [])],
            "categories": [c.get("description", c) if isinstance(c, dict) else c
                           for c in datos.get("categories", [])],
            "tags":       list(datos.get("tags", {}).keys()) if isinstance(datos.get("tags"), dict) else [],
        },

        # Plataformas soportadas — subdocumento con booleanos
        "platforms": {
            "windows": datos.get("windows", False),
            "mac":     datos.get("mac", False),
            "linux":   datos.get("linux", False),
        },

        # Reviews y ratings de la comunidad
        "reviews": {
            "positive":   pos,
            "negative":   neg,
            "total":      total_reviews,
            "rating_pct": rating_pct,           # % positivos
        },

        # Puntuación Metacritic si existe
        "metacritic": datos.get("metacritic", None),

        # Requisitos técnicos (estructura variable según el juego)
        "requirements": {
            "pc_minimum": datos.get("pc_requirements", {}).get("minimum", None),
        },
    }
    return doc


def cargar_dataset(col) -> int:
    """
    Lee el archivo games.json del dataset Steam y carga los documentos en MongoDB.
    Usa insert_many en lotes de 500 para rendimiento óptimo.
    Retorna la cantidad de documentos insertados.
    """
    if not DATASET_PATH.exists():
        print(f"\n⚠  Archivo no encontrado: {DATASET_PATH}")
        print("   Descargá el dataset desde:")
        print("   https://www.kaggle.com/datasets/fronkongames/steam-games-dataset")
        print("   y colocá 'games.json' en la misma carpeta que este script.\n")
        return 0

    print(f"\n→ Leyendo {DATASET_PATH.name}...")
    with open(DATASET_PATH, encoding="utf-8") as f:
        raw = json.load(f)

    # El JSON del dataset tiene estructura: {"APP_ID": {...datos...}, ...}
    items = list(raw.items())
    if LOAD_LIMIT:
        items = items[:LOAD_LIMIT]

    print(f"→ Transformando {len(items):,} registros...")
    documentos = [transformar_documento(app_id, datos) for app_id, datos in items]

    # Limpiar colección antes de insertar (para re-ejecuciones limpias)
    col.drop()
    print("→ Colección anterior eliminada.")

    # Inserción en lotes
    BATCH = 500
    total = 0
    for i in range(0, len(documentos), BATCH):
        lote = documentos[i:i + BATCH]
        try:
            resultado = col.insert_many(lote, ordered=False)
            total += len(resultado.inserted_ids)
        except BulkWriteError as e:
            total += e.details.get("nInserted", 0)

    print(f"✓ {total:,} documentos insertados en '{COLLECTION}'.\n")
    return total


# ─────────────────────────────────────────────
#  ÍNDICES
# ─────────────────────────────────────────────

def crear_indices(col):
    """
    Crea índices para optimizar las queries más frecuentes de GameNexus.
    
    Valor agregado: los índices en MongoDB no requieren reestructurar
    el esquema (como en SQL). Se agregan sobre cualquier campo,
    incluyendo campos dentro de subdocumentos y arrays.
    """
    indices = [
        # Búsquedas por nombre de juego
        ([("name", ASCENDING)],            {"name": "idx_name"}),
        # Filtros por género (campo dentro de subdocumento embebido)
        ([("taxonomy.genres", ASCENDING)], {"name": "idx_genres"}),
        # Ordenamiento por rating
        ([("reviews.rating_pct", DESCENDING)], {"name": "idx_rating"}),
        # Filtros por precio
        ([("price_usd", ASCENDING)],       {"name": "idx_price"}),
        # Búsquedas por plataforma
        ([("platforms.windows", ASCENDING)], {"name": "idx_platform_win"}),
        # Índice compuesto: género + rating (para queries de "top juegos por género")
        ([("taxonomy.genres", ASCENDING), ("reviews.rating_pct", DESCENDING)],
         {"name": "idx_genre_rating"}),
        # Índice de texto completo para búsqueda por descripción/nombre
        ([("name", "text"), ("description", "text")],
         {"name": "idx_text_search"}),
    ]

    for campos, opciones in indices:
        col.create_index(campos, **opciones)

    print(f"✓ {len(indices)} índices creados.\n")


# ─────────────────────────────────────────────
#  OPERACIONES CRUD
# ─────────────────────────────────────────────

def demo_crud(col):
    """Operaciones básicas: INSERT, FIND, UPDATE, DELETE."""
    sep = "─" * 60

    print(sep)
    print("  OPERACIONES CRUD")
    print(sep)

    # ── INSERT: agregar un juego ficticio de GameNexus ──────────
    print("\n[CREATE] Insertando juego de prueba...")
    juego_prueba = {
        "steam_appid": "TEST_001",
        "name":        "GameNexus Demo Game",
        "type":        "game",
        "is_free":     False,
        "price_usd":   29.99,
        "release_date": "2026-01-01",
        "description": "Juego de demostración del TP Integrador UADE.",
        "developers":  ["UADE Dev Team"],
        "publishers":  ["UADE Publishing"],
        "taxonomy": {
            "genres":     ["Action", "Indie"],
            "categories": ["Single-player", "Multi-player"],
            "tags":       ["Demo", "Educational"],
        },
        "platforms": {"windows": True, "mac": False, "linux": False},
        "reviews":   {"positive": 100, "negative": 5, "total": 105, "rating_pct": 95.2},
        "metacritic": None,
        "requirements": {"pc_minimum": "Windows 10, 8GB RAM"},
    }
    res = col.insert_one(juego_prueba)
    print(f"  → Insertado con _id: {res.inserted_id}")

    # ── READ: buscar por nombre exacto ──────────────────────────
    print("\n[READ] Buscando 'GameNexus Demo Game'...")
    doc = col.find_one({"name": "GameNexus Demo Game"}, {"_id": 0, "name": 1, "price_usd": 1})
    print(f"  → Encontrado: {doc}")

    # ── UPDATE: actualizar el precio ────────────────────────────
    print("\n[UPDATE] Actualizando precio a $19.99...")
    col.update_one(
        {"steam_appid": "TEST_001"},
        {"$set": {"price_usd": 19.99, "is_free": False}}
    )
    doc_actualizado = col.find_one({"steam_appid": "TEST_001"}, {"_id": 0, "name": 1, "price_usd": 1})
    print(f"  → Actualizado: {doc_actualizado}")

    # ── DELETE: eliminar el documento de prueba ─────────────────
    print("\n[DELETE] Eliminando juego de prueba...")
    col.delete_one({"steam_appid": "TEST_001"})
    print("  → Eliminado correctamente.")


# ─────────────────────────────────────────────
#  CONSULTAS AVANZADAS (AGGREGATION PIPELINE)
# ─────────────────────────────────────────────

def demo_queries_avanzadas(col):
    """
    Consultas con Aggregation Pipeline de MongoDB.
    
    El pipeline es la herramienta más potente de MongoDB:
    permite encadenar etapas ($match, $group, $sort, $project, $unwind)
    para transformar y analizar datos dentro del motor,
    sin mover datos a la aplicación.
    """
    sep = "─" * 60

    print(f"\n{sep}")
    print("  CONSULTAS AVANZADAS — AGGREGATION PIPELINE")
    print(sep)

    # ── Q1: Top 10 juegos mejor calificados con más de 500 reviews ──
    print("\n[Q1] Top 10 juegos mejor calificados (mín. 500 reseñas):")
    pipeline_top10 = [
        {"$match":  {"reviews.total": {"$gte": 500}, "reviews.rating_pct": {"$ne": None}}},
        {"$sort":   {"reviews.rating_pct": -1}},
        {"$limit":  10},
        {"$project": {
            "_id": 0,
            "Juego":   "$name",
            "Rating":  "$reviews.rating_pct",
            "Reseñas": "$reviews.total",
            "Precio":  "$price_usd",
        }}
    ]
    top10 = list(col.aggregate(pipeline_top10))
    for i, g in enumerate(top10, 1):
        print(f"  {i:2}. {g['Juego'][:45]:<45} {g['Rating']}% ({g['Reseñas']:,} reseñas) — ${g['Precio']}")

    # ── Q2: Cantidad de juegos por género, ordenado ──────────────
    print("\n[Q2] Distribución de juegos por género (top 10):")
    pipeline_generos = [
        {"$unwind":  "$taxonomy.genres"},          # Desanida el array de géneros
        {"$group":   {"_id": "$taxonomy.genres", "cantidad": {"$sum": 1}}},
        {"$sort":    {"cantidad": -1}},
        {"$limit":   10},
        {"$project": {"_id": 0, "Género": "$_id", "Cantidad": "$cantidad"}}
    ]
    generos = list(col.aggregate(pipeline_generos))
    for g in generos:
        barra = "█" * (g["Cantidad"] // 100)
        print(f"  {g['Género']:<20} {g['Cantidad']:>5,}  {barra}")

    # ── Q3: Precio promedio por género ───────────────────────────
    print("\n[Q3] Precio promedio por género (juegos de pago, top 8):")
    pipeline_precio_genero = [
        {"$match":  {"is_free": False, "price_usd": {"$gt": 0}}},
        {"$unwind": "$taxonomy.genres"},
        {"$group":  {
            "_id":       "$taxonomy.genres",
            "precio_avg": {"$avg": "$price_usd"},
            "cantidad":   {"$sum": 1},
        }},
        {"$match":  {"cantidad": {"$gte": 50}}},   # Solo géneros con suficiente muestra
        {"$sort":   {"precio_avg": -1}},
        {"$limit":  8},
        {"$project": {"_id": 0, "Género": "$_id",
                      "Precio Promedio": {"$round": ["$precio_avg", 2]},
                      "Juegos": "$cantidad"}}
    ]
    precios = list(col.aggregate(pipeline_precio_genero))
    for p in precios:
        print(f"  {p['Género']:<20}  ${p['Precio Promedio']:>6.2f}  ({p['Juegos']:,} juegos)")

    # ── Q4: Juegos gratuitos vs. de pago por plataforma ──────────
    print("\n[Q4] Juegos gratuitos vs. de pago (Windows vs. Linux):")
    pipeline_free = [
        {"$group": {
            "_id": {
                "is_free":  "$is_free",
                "windows":  "$platforms.windows",
                "linux":    "$platforms.linux",
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 6}
    ]
    free_data = list(col.aggregate(pipeline_free))
    for row in free_data:
        gratis = "Gratis" if row["_id"]["is_free"] else "De pago"
        win    = "Win ✓" if row["_id"]["windows"] else "Win ✗"
        lin    = "Lin ✓" if row["_id"]["linux"] else "Lin ✗"
        print(f"  {gratis:<10} | {win} | {lin} → {row['count']:,} juegos")

    # ── Q5: Desarrolladores con más juegos publicados ────────────
    print("\n[Q5] Top 10 desarrolladores por cantidad de títulos:")
    pipeline_devs = [
        {"$unwind": "$developers"},
        {"$match":  {"developers": {"$ne": ""}}},
        {"$group":  {"_id": "$developers", "titulos": {"$sum": 1}}},
        {"$sort":   {"titulos": -1}},
        {"$limit":  10},
        {"$project": {"_id": 0, "Developer": "$_id", "Títulos": "$titulos"}}
    ]
    devs = list(col.aggregate(pipeline_devs))
    for d in devs:
        print(f"  {d['Developer'][:40]:<40} {d['Títulos']:>4} títulos")

    # ── Q6: Búsqueda de texto completo ───────────────────────────
    print("\n[Q6] Búsqueda de texto: juegos relacionados con 'survival open world':")
    resultados_texto = list(col.find(
        {"$text": {"$search": "survival open world"}},
        {"score": {"$meta": "textScore"}, "name": 1, "taxonomy.genres": 1, "_id": 0}
    ).sort([("score", {"$meta": "textScore"})]).limit(5))
    for r in resultados_texto:
        print(f"  {r['name'][:50]:<50} Géneros: {r.get('taxonomy', {}).get('genres', [])[:2]}")


# ─────────────────────────────────────────────
#  ESTADÍSTICAS GENERALES
# ─────────────────────────────────────────────

def mostrar_estadisticas(col):
    """Muestra un resumen del estado de la colección."""
    sep = "─" * 60
    print(f"\n{sep}")
    print("  ESTADÍSTICAS DE LA COLECCIÓN")
    print(sep)

    total       = col.count_documents({})
    gratuitos   = col.count_documents({"is_free": True})
    con_reviews = col.count_documents({"reviews.total": {"$gt": 0}})
    con_meta    = col.count_documents({"metacritic": {"$ne": None}})

    stats = col.aggregate([
        {"$match": {"is_free": False, "price_usd": {"$gt": 0}}},
        {"$group": {
            "_id":        None,
            "precio_avg": {"$avg": "$price_usd"},
            "precio_max": {"$max": "$price_usd"},
            "precio_min": {"$min": "$price_usd"},
        }}
    ])
    precio_stats = list(stats)

    print(f"\n  Total de documentos:        {total:>8,}")
    print(f"  Juegos gratuitos:           {gratuitos:>8,}  ({gratuitos/total*100:.1f}%)")
    print(f"  Juegos con reseñas:         {con_reviews:>8,}")
    print(f"  Juegos con Metacritic:      {con_meta:>8,}")

    if precio_stats:
        ps = precio_stats[0]
        print(f"\n  Precio promedio (de pago):  ${ps['precio_avg']:>7.2f}")
        print(f"  Precio máximo:              ${ps['precio_max']:>7.2f}")
        print(f"  Precio mínimo (de pago):    ${ps['precio_min']:>7.2f}")

    # Índices activos
    indices = list(col.list_indexes())
    print(f"\n  Índices activos:            {len(indices):>8}")
    for idx in indices:
        print(f"    · {idx['name']}")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  GAMENEXUS — MongoDB: Catálogo de Videojuegos")
    print("  TP Integrador — ID II — UADE 2026")
    print("=" * 60)

    t0 = time.time()

    # 1. Conectar
    client, col = conectar()

    # 2. Cargar dataset
    n = cargar_dataset(col)
    if n == 0:
        print("No se cargaron documentos. Verificá el dataset y reintentá.")
        return

    # 3. Crear índices
    print("→ Creando índices...")
    crear_indices(col)

    # 4. Operaciones CRUD
    demo_crud(col)

    # 5. Queries avanzadas
    demo_queries_avanzadas(col)

    # 6. Estadísticas finales
    mostrar_estadisticas(col)

    elapsed = time.time() - t0
    print(f"\n{'─'*60}")
    print(f"  ✓ Ejecución completa en {elapsed:.1f}s")
    print(f"{'─'*60}\n")

    client.close()


if __name__ == "__main__":
    main()
