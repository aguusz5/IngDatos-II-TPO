"""
================================================================================
  GAMENEXUS — Módulo Redis: Estado Online en Tiempo Real y Leaderboards
  Trabajo Práctico Integrador — Ingeniería de Datos II — UADE 2026
  Motor: Redis  |  Lenguaje: Python 3.12  |  Librería: redis-py
================================================================================

Rol de Redis en GameNexus:
  Redis vive en memoria RAM y responde en milisegundos. Es el motor más
  crítico durante una partida activa. Gestiona tres estructuras:

  1. SESIÓN DE JUGADOR (Hash + TTL)
     Clave: session:<jugador_id>
     TTL: 1800 segundos. Si el jugador cierra el cliente sin desconectarse,
     la sesión expira sola — comportamiento imposible de replicar en SQL
     sin un job programado externo.

  2. SALA DE PARTIDA (Hash + Set)
     Clave: sala:<sala_id>  /  sala:<sala_id>:jugadores

  3. LEADERBOARD GLOBAL (Sorted Set)
     Clave: leaderboard:global
     ZADD actualiza en O(log N); ZREVRANGE recupera el top sin ningún
     ORDER BY ni lectura de tabla completa.

Integración con los otros motores:
  - MongoDB  → provee los juegos reales del catálogo Steam (nombre, appid, género)
  - Neo4j    → provee los IDs de amigos reales de cada jugador ([:AMIGO_DE])
  Redis combina ambas fuentes: sabe en qué juego (MongoDB) está cada sesión
  y puede filtrar quiénes de la red social (Neo4j) están online ahora.

Requisitos:
  pip install redis pymongo neo4j faker

  - Redis   en localhost:6379
    Docker: docker run --name redis-gamenexus -p 6379:6379 -d redis:latest
  - MongoDB en localhost:27017  (gamenexus_mongodb.py ejecutado primero)
  - Neo4j   en localhost:7687   (gamenexus_neo4j.py ejecutado primero)

================================================================================
"""

import redis
import random
from datetime import datetime
from faker import Faker
from pymongo import MongoClient
from neo4j import GraphDatabase

fake = Faker("es_ES")

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

REDIS_HOST  = "localhost"
REDIS_PORT  = 6379
REDIS_DB    = 0

MONGO_URI   = "mongodb://localhost:27017/"
MONGO_DB    = "gamenexus"
MONGO_COL   = "games"

NEO4J_URI   = "bolt://localhost:7687"
NEO4J_USER  = "neo4j"
NEO4J_PASS  = "password"          # ajustar si la contraseña es distinta

N_JUGADORES = 20
N_SALAS     = 5
PLATAFORMAS = ["PC", "PS5", "Xbox Series X", "Steam Deck"]


# ─────────────────────────────────────────────
# CONEXIONES
# ─────────────────────────────────────────────

def conectar_redis():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                    decode_responses=True)
    r.ping()
    print(f"[OK] Redis   → {REDIS_HOST}:{REDIS_PORT}")
    return r

def conectar_mongo():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    db.command("ping")
    print(f"[OK] MongoDB → {MONGO_URI}  db: {MONGO_DB}")
    return db

def conectar_neo4j():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    driver.verify_connectivity()
    print(f"[OK] Neo4j   → {NEO4J_URI}")
    return driver


# ─────────────────────────────────────────────
# FUENTE 1 — JUEGOS REALES DESDE MONGODB
# ─────────────────────────────────────────────

def cargar_juegos_desde_mongo(db, n=20):
    """
    Lee juegos reales del catálogo Steam desde MongoDB.
    Misma colección y estructura que gamenexus_mongodb.py.
    Redis usa estos datos para las salas y sesiones — garantiza
    coherencia de nombres y appids entre los tres motores.
    """
    pipeline = [
        {"$match": {
            "name":   {"$exists": True, "$ne": ""},
            "genres": {"$exists": True, "$ne": []}
        }},
        {"$sample": {"size": n}},
        {"$project": {"_id": 0, "steam_appid": 1, "name": 1, "genres": 1}}
    ]
    juegos = list(db[MONGO_COL].aggregate(pipeline))
    print(f"  [MongoDB→Redis] {len(juegos)} juegos reales cargados.")
    return juegos


# ─────────────────────────────────────────────
# FUENTE 2 — JUGADORES Y AMIGOS REALES DESDE NEO4J
# ─────────────────────────────────────────────

def cargar_jugadores_desde_neo4j(driver):
    """
    Lee todos los nodos :Jugador que existen en el grafo Neo4j.
    Sus IDs son los mismos que usará Redis para las sesiones,
    garantizando que la integración amigos_online() devuelva
    resultados reales y no IDs inventados.
    """
    with driver.session() as session:
        resultado = session.run("""
            MATCH (j:Jugador)
            RETURN j.id AS id, j.nombre AS nombre, j.apellido AS apellido
            ORDER BY j.id
        """)
        jugadores = [{"id": r["id"],
                      "nombre": f"{r['nombre']} {r['apellido']}",
                      "xp": random.randint(500, 50000)}
                     for r in resultado]
    print(f"  [Neo4j→Redis] {len(jugadores)} jugadores reales cargados.")
    return jugadores

def obtener_amigos_desde_neo4j(driver, jugador_id):
    """
    Consulta Neo4j para obtener los amigos directos (:AMIGO_DE)
    de un jugador dado. Devuelve lista de IDs.

    Este es el punto central de integración:
      Neo4j  → traversal del grafo → IDs de amigos
      Redis  → confirma cuáles de esos IDs tienen sesión activa ahora

    En SQL esto requeriría un self-JOIN sobre una tabla de amistades.
    En Neo4j es un traversal directo de un solo salto.
    """
    with driver.session() as session:
        resultado = session.run("""
            MATCH (j:Jugador {id: $jugador_id})-[:AMIGO_DE]-(amigo:Jugador)
            RETURN amigo.id AS id, amigo.nombre AS nombre, amigo.apellido AS apellido
        """, jugador_id=jugador_id)
        amigos = [{"id": r["id"], "nombre": f"{r['nombre']} {r['apellido']}"}
                  for r in resultado]
    return amigos


# ─────────────────────────────────────────────
# GENERACIÓN DE DATOS AUXILIARES
# ─────────────────────────────────────────────

def generar_salas(juegos, n=N_SALAS):
    """Salas de partida usando juegos reales del catálogo Steam."""
    salas = []
    for i in range(1, n + 1):
        juego = random.choice(juegos)
        salas.append({
            "id":           f"sala_{i:03d}",
            "steam_appid":  str(juego.get("steam_appid", f"app_{i}")),
            "juego_nombre": juego.get("name", "Desconocido"),
            "genero":       juego.get("genres", ["N/A"])[0],
            "mapa":         fake.city(),
            "max_jugadores": random.choice([2, 4, 8, 16]),
            "inicio":       datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        })
    return salas


# ─────────────────────────────────────────────
# MÓDULO 1: SESIONES DE JUGADORES
# ─────────────────────────────────────────────

def crear_sesion(r, jugador_id, nombre, steam_appid, juego_nombre, sala_id, ttl=1800):
    """
    Registra jugador activo en una partida.

    Comparación SQL:
      SQL: UPDATE jugadores SET estado='online', juego=... WHERE id=...
           + job externo para limpiar sesiones expiradas
      Redis: HSET + EXPIRE — atómico, expira solo, sin mantenimiento.
    """
    key = f"session:{jugador_id}"
    r.hset(key, mapping={
        "jugador_id":   jugador_id,
        "nombre":       nombre,
        "steam_appid":  steam_appid,
        "juego_nombre": juego_nombre,
        "sala_id":      sala_id,
        "plataforma":   random.choice(PLATAFORMAS),
        "inicio":       datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    })
    r.expire(key, ttl)
    r.sadd(f"sala:{sala_id}:jugadores", jugador_id)

def cerrar_sesion(r, jugador_id):
    key   = f"session:{jugador_id}"
    datos = r.hgetall(key)
    if datos:
        r.srem(f"sala:{datos.get('sala_id', '')}:jugadores", jugador_id)
        r.delete(key)
        print(f"  [session] Sesión cerrada: {jugador_id}")
    else:
        print(f"  [session] Sin sesión activa para {jugador_id}")

def ver_sesion(r, jugador_id):
    key = f"session:{jugador_id}"
    return r.hgetall(key), r.ttl(key)

def listar_jugadores_online(r):
    jugadores = []
    for key in r.keys("session:*"):
        datos = r.hgetall(key)
        datos["ttl_segundos"] = r.ttl(key)
        jugadores.append(datos)
    return jugadores


# ─────────────────────────────────────────────
# MÓDULO 2: SALAS DE PARTIDA
# ─────────────────────────────────────────────

def crear_sala(r, sala):
    r.hset(f"sala:{sala['id']}", mapping={
        "sala_id":       sala["id"],
        "steam_appid":   sala["steam_appid"],
        "juego_nombre":  sala["juego_nombre"],
        "genero":        sala["genero"],
        "mapa":          sala["mapa"],
        "max_jugadores": sala["max_jugadores"],
        "inicio":        sala["inicio"],
        "estado":        "activa",
    })

def ver_sala(r, sala_id):
    return r.hgetall(f"sala:{sala_id}"), r.smembers(f"sala:{sala_id}:jugadores")

def listar_salas_activas(r):
    salas = []
    for key in r.keys("sala:*"):
        if ":jugadores" in key:
            continue
        sala_id    = key.split(":")[1]
        sala, jugs = ver_sala(r, sala_id)
        if sala:
            sala["jugadores_conectados"] = len(jugs)
            salas.append(sala)
    return salas


# ─────────────────────────────────────────────
# MÓDULO 3: LEADERBOARD GLOBAL (Sorted Set)
# ─────────────────────────────────────────────

def inicializar_leaderboard(r, jugadores):
    """
    Carga XP de todos los jugadores en el Sorted Set.

    Comparación SQL:
      SELECT nombre, xp FROM jugadores ORDER BY xp DESC LIMIT 10
      → scan de tabla + sort en memoria cada vez que se consulta
      Redis: ZREVRANGE → O(log N + M), siempre precalculado en RAM.
    """
    pipe = r.pipeline()
    for j in jugadores:
        pipe.zadd("leaderboard:global", {j["id"]: j["xp"]})
        pipe.set(f"nombre:{j['id']}", j["nombre"])
    pipe.execute()

def actualizar_xp(r, jugador_id, xp_ganado):
    """ZINCRBY incrementa el score y reordena el Sorted Set automáticamente."""
    return r.zincrby("leaderboard:global", xp_ganado, jugador_id)

def top_jugadores(r, n=10):
    top = []
    for jugador_id, xp in r.zrevrange("leaderboard:global", 0, n - 1, withscores=True):
        nombre = r.get(f"nombre:{jugador_id}") or jugador_id
        top.append({"posicion": len(top) + 1, "id": jugador_id,
                    "nombre": nombre, "xp": int(xp)})
    return top

def ranking_jugador(r, jugador_id):
    rank   = r.zrevrank("leaderboard:global", jugador_id)
    xp     = r.zscore("leaderboard:global", jugador_id)
    nombre = r.get(f"nombre:{jugador_id}") or jugador_id
    if rank is None:
        return None
    return {"jugador_id": jugador_id, "nombre": nombre,
            "posicion": rank + 1, "xp": int(xp)}

def top_por_juego(r, jugadores_online, steam_appid, n=5):
    """
    Valor agregado: leaderboard filtrado por jugadores activos en un juego real.
    Cruza el Sorted Set global con las sesiones activas — sin consultar Cassandra.
    """
    activos = [j["jugador_id"] for j in jugadores_online
               if j.get("steam_appid") == steam_appid]
    if not activos:
        return []
    resultados = []
    for jid in activos:
        xp     = r.zscore("leaderboard:global", jid)
        nombre = r.get(f"nombre:{jid}") or jid
        if xp:
            resultados.append({"id": jid, "nombre": nombre, "xp": int(xp)})
    return sorted(resultados, key=lambda x: x["xp"], reverse=True)[:n]


# ─────────────────────────────────────────────
# MÓDULO 4: INTEGRACIÓN Neo4j → Redis
# ─────────────────────────────────────────────

def amigos_online(r, driver, jugador_id):
    """
    Flujo completo de integración Neo4j → Redis:

      1. Neo4j: traversal [:AMIGO_DE] → lista de IDs de amigos reales
      2. Redis: para cada ID, verifica si existe session:<id> activa

    Resultado: amigos que están jugando en este momento exacto.
    Ningún motor por separado puede responder esta pregunta:
      - Neo4j sabe quiénes son amigos, pero no sabe quién está online.
      - Redis sabe quién está online, pero no sabe quiénes son amigos.
    """
    amigos = obtener_amigos_desde_neo4j(driver, jugador_id)
    print(f"  [Neo4j] {len(amigos)} amigos encontrados en el grafo para {jugador_id}")

    online = []
    for amigo in amigos:
        sesion = r.hgetall(f"session:{amigo['id']}")
        if sesion:
            sesion["ttl_segundos"] = r.ttl(f"session:{amigo['id']}")
            online.append(sesion)
    return online


# ─────────────────────────────────────────────
# LIMPIEZA
# ─────────────────────────────────────────────

def limpiar_datos(r):
    claves = (r.keys("session:*") + r.keys("sala:*") +
              r.keys("leaderboard:*") + r.keys("nombre:*"))
    if claves:
        r.delete(*claves)
    print(f"  [limpieza] {len(claves)} claves eliminadas.")


# ─────────────────────────────────────────────
# DEMO PRINCIPAL
# ─────────────────────────────────────────────

def demo():
    print("=" * 65)
    print("  GAMENEXUS — Redis: Estado Online en Tiempo Real")
    print("  TP Integrador — Ingeniería de Datos II — UADE 2026")
    print("=" * 65)

    r      = conectar_redis()
    db     = conectar_mongo()
    driver = conectar_neo4j()

    limpiar_datos(r)

    # ── [0] Cargar datos reales desde MongoDB y Neo4j ─────────────
    print("\n[0] CARGANDO DATOS REALES DESDE MONGODB Y NEO4J")
    juegos    = cargar_juegos_desde_mongo(db, n=20)
    jugadores = cargar_jugadores_desde_neo4j(driver)
    salas     = generar_salas(juegos, N_SALAS)

    # ── [1] Leaderboard inicial ────────────────────────────────────
    print("\n[1] INICIALIZANDO LEADERBOARD GLOBAL")
    inicializar_leaderboard(r, jugadores)
    print("  Top 5 jugadores (XP inicial):")
    for j in top_jugadores(r, 5):
        print(f"    #{j['posicion']:>2}  {j['nombre']:<30}  {j['xp']:>6} XP")

    # ── [2] Crear salas con juegos reales de MongoDB ───────────────
    print("\n[2] CREANDO SALAS (juegos reales del catálogo Steam)")
    for sala in salas:
        crear_sala(r, sala)
        print(f"  {sala['id']}  →  {sala['juego_nombre'][:35]:<35}  "
              f"[appid: {sala['steam_appid']}]")

    # ── [3] Simular jugadores entrando a partidas ──────────────────
    print("\n[3] JUGADORES ENTRANDO A PARTIDAS")
    n_activos = min(12, len(jugadores))
    activos   = random.sample(jugadores, n_activos)
    for jugador in activos:
        sala = random.choice(salas)
        crear_sesion(r, jugador["id"], jugador["nombre"],
                     sala["steam_appid"], sala["juego_nombre"], sala["id"])
    print(f"  {n_activos} sesiones creadas (TTL: 1800s)")

    # ── [4] Jugadores online ───────────────────────────────────────
    print("\n[4] JUGADORES ONLINE EN ESTE MOMENTO")
    online = listar_jugadores_online(r)
    for j in online:
        print(f"  {j.get('nombre','?'):<30} → "
              f"{j.get('juego_nombre','?')[:30]:<30}  TTL: {j.get('ttl_segundos','?')}s")

    # ── [5] Salas activas ──────────────────────────────────────────
    print("\n[5] SALAS ACTIVAS Y OCUPACIÓN")
    for sala in listar_salas_activas(r):
        print(f"  {sala.get('sala_id','?'):<10}  "
              f"{sala.get('juego_nombre','?')[:30]:<30}  "
              f"{sala.get('jugadores_conectados',0)}/{sala.get('max_jugadores','?')} jugadores")

    # ── [6] Actualizar XP al cerrar partidas ───────────────────────
    print("\n[6] FIN DE PARTIDAS — ACTUALIZANDO XP EN VIVO")
    for jugador in activos[:6]:
        xp_ganado = random.randint(100, 1500)
        nuevo_xp  = actualizar_xp(r, jugador["id"], xp_ganado)
        print(f"  {jugador['nombre']:<30}  +{xp_ganado:>4} XP  →  total: {int(nuevo_xp):>6} XP")

    print("\n  Top 5 (XP actualizado en vivo):")
    for j in top_jugadores(r, 5):
        print(f"    #{j['posicion']:>2}  {j['nombre']:<30}  {j['xp']:>6} XP")

    # ── [7] Ranking individual ─────────────────────────────────────
    jugador_test = jugadores[0]
    print(f"\n[7] POSICIÓN DE {jugador_test['nombre'].upper()}")
    rank = ranking_jugador(r, jugador_test["id"])
    if rank:
        print(f"  Posición #{rank['posicion']}  —  {rank['xp']} XP")

    # ── [8] Leaderboard filtrado por juego real ────────────────────
    sala_filtro = random.choice(salas)
    print(f"\n[8] TOP JUGADORES ACTIVOS EN: {sala_filtro['juego_nombre'][:40]}")
    top_juego = top_por_juego(r, online, sala_filtro["steam_appid"], n=5)
    if top_juego:
        for i, j in enumerate(top_juego, 1):
            print(f"  #{i}  {j['nombre']:<30}  {j['xp']:>6} XP")
    else:
        print("  Sin jugadores activos en ese juego en este momento.")

    # ── [9] Integración real Neo4j → Redis ────────────────────────
    print("\n[9] INTEGRACIÓN Neo4j → Redis: AMIGOS ONLINE")
    jugador_consulta = jugadores[0]
    print(f"  Consultando amigos de: {jugador_consulta['nombre']} ({jugador_consulta['id']})")
    amigos_activos = amigos_online(r, driver, jugador_consulta["id"])
    print(f"  [Redis] De esos amigos, {len(amigos_activos)} están online ahora:")
    for a in amigos_activos:
        print(f"    {a.get('nombre','?'):<30}  jugando  {a.get('juego_nombre','?')[:30]}")
    if not amigos_activos:
        print("  (ningún amigo online en este momento — "
              "normal si la sesión es nueva y los IDs no coinciden aún)")

    # ── [10] Cerrar sesión ─────────────────────────────────────────
    print("\n[10] CERRAR SESIÓN DE UN JUGADOR")
    saliente = activos[0]
    print(f"  {saliente['nombre']} se desconecta...")
    cerrar_sesion(r, saliente["id"])
    sesion, _ = ver_sesion(r, saliente["id"])
    print(f"  Sesión activa: {'Sí' if sesion else 'No (eliminada correctamente)'}")

    driver.close()

    print("\n" + "=" * 65)
    print("  Demo completada. MongoDB + Neo4j + Redis operando juntos.")
    print("=" * 65)


if __name__ == "__main__":
    demo()