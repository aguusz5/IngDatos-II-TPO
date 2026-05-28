    """
================================================================================
  GAMENEXUS — Módulo Cassandra: Historial de Partidas y Estadísticas
  Trabajo Práctico Integrador — Ingeniería de Datos II — UADE 2026
  Motor: Apache Cassandra  |  Lenguaje: Python 3.12  |  Librería: cassandra-driver
================================================================================

Rol de Cassandra en GameNexus:
  Cada partida finalizada genera un registro: quién jugó, cuándo, en qué
  juego, duración, kills, resultado, XP ganado. Con millones de jugadores
  activos esto es escritura masiva y continua.

  Cassandra resuelve esto con modelado orientado a consultas (Chebotko):
  no se parte de entidades sino de las preguntas que la app necesita
  responder. Cada tabla existe para servir una consulta específica.

  Tablas diseñadas:

  1. partidas_por_jugador
     PRIMARY KEY (jugador_id, jugado_en)  — clustering DESC
     Consulta: "dame el historial reciente de un jugador"
     Partición por jugador → todas sus partidas en el mismo nodo.

  2. partidas_por_juego
     PRIMARY KEY (steam_appid, jugado_en)  — clustering DESC
     Consulta: "últimas partidas jugadas en un juego específico"
     Desnormalización: mismo evento guardado dos veces, tabla diferente.

  3. estadisticas_jugador
     PRIMARY KEY (jugador_id)
     Acumula totales: partidas jugadas, XP total, kills totales, minutos.
     Se actualiza con cada partida usando COUNTER o UPDATE con aritmética.

  Integración con los otros motores:
    - MongoDB  → juegos reales (nombre, steam_appid, género)
    - Neo4j    → jugadores reales (mismos IDs que usan Redis y Neo4j)
    - Redis    → al finalizar una partida, Redis actualiza el leaderboard
                 en vivo y Cassandra persiste el registro histórico.
                 Dos motores con roles distintos sobre el mismo evento.

Requisitos:
  pip install cassandra-driver pymongo neo4j faker

  - Cassandra en localhost:9042
    Docker: docker run --name cassandra-gamenexus -p 9042:9042 -d cassandra:latest
    (esperar ~30s a que Cassandra levante antes de conectar)
  - MongoDB  en localhost:27017  (gamenexus_mongodb.py  ejecutado primero)
  - Neo4j    en localhost:7687   (gamenexus_neo4j.py    ejecutado primero)

Comparación SQL relevante para la presentación:
  SQL: una tabla "partidas" con FK a jugadores y juegos, JOINs para consultar.
  Cassandra: dos tablas desnormalizadas (por jugador, por juego). Sin JOINs,
  sin FK. La misma partida se escribe dos veces a propósito — escritura barata,
  lectura directa sin traversal.


"""

import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pymongo import MongoClient
from neo4j import GraphDatabase

fake = Faker("es_ES")

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

CASSANDRA_HOST = "127.0.0.1"
CASSANDRA_PORT = 9042
KEYSPACE       = "gamenexus"

MONGO_URI      = "mongodb://localhost:27017/"
MONGO_DB       = "gamenexus"
MONGO_COL      = "games"

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASS     = "password"        # ajustar si es distinta

N_PARTIDAS     = 50                # partidas a simular


# ─────────────────────────────────────────────
# CONEXIONES
# ─────────────────────────────────────────────

def conectar_cassandra():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    print(f"[OK] Cassandra → {CASSANDRA_HOST}:{CASSANDRA_PORT}")
    return cluster, session

def conectar_mongo():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    db.command("ping")
    print(f"[OK] MongoDB  → {MONGO_URI}  db: {MONGO_DB}")
    return db

def conectar_neo4j():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    driver.verify_connectivity()
    print(f"[OK] Neo4j    → {NEO4J_URI}")
    return driver

### Comentario de prueba

# ─────────────────────────────────────────────
# KEYSPACE Y TABLAS
# ─────────────────────────────────────────────

def crear_schema(session):
    """
    Crea el keyspace y las tres tablas del módulo.

    SimpleStrategy con replication_factor=1 es suficiente para desarrollo
    local. En producción se usaría NetworkTopologyStrategy.

    Diseño Chebotko:
      Cada tabla responde exactamente una consulta.
      No hay JOINs — los datos se desnormalizan a propósito.
    """
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    """)
    session.set_keyspace(KEYSPACE)

    # Tabla 1: historial de un jugador — partition key = jugador_id
    # clustering key = jugado_en DESC → últimas partidas primero
    session.execute("""
        CREATE TABLE IF NOT EXISTS partidas_por_jugador (
            jugador_id   text,
            jugado_en    timestamp,
            partida_id   uuid,
            steam_appid  text,
            juego_nombre text,
            duracion_min int,
            kills        int,
            resultado    text,
            xp_ganado    int,
            plataforma   text,
            PRIMARY KEY (jugador_id, jugado_en)
        ) WITH CLUSTERING ORDER BY (jugado_en DESC)
    """)

    # Tabla 2: partidas por juego — partition key = steam_appid
    # clustering key = jugado_en DESC
    # Misma información, distinta partición — desnormalización intencional
    session.execute("""
        CREATE TABLE IF NOT EXISTS partidas_por_juego (
            steam_appid  text,
            jugado_en    timestamp,
            partida_id   uuid,
            jugador_id   text,
            juego_nombre text,
            duracion_min int,
            kills        int,
            resultado    text,
            xp_ganado    int,
            PRIMARY KEY (steam_appid, jugado_en)
        ) WITH CLUSTERING ORDER BY (jugado_en DESC)
    """)

    # Tabla 3: estadísticas acumuladas por jugador
    # PRIMARY KEY simple — una fila por jugador, se actualiza con UPDATE
    session.execute("""
        CREATE TABLE IF NOT EXISTS estadisticas_jugador (
            jugador_id      text PRIMARY KEY,
            nombre          text,
            partidas_jugadas int,
            xp_total        int,
            kills_total     int,
            minutos_totales int,
            ultimo_juego    text,
            ultima_partida  timestamp
        )
    """)

    print("  [schema] Keyspace y 3 tablas creadas/verificadas.")


# ─────────────────────────────────────────────
# FUENTE 1 — JUEGOS REALES DESDE MONGODB
# ─────────────────────────────────────────────

def cargar_juegos_desde_mongo(db, n=15):
    """
    Lee juegos reales del catálogo Steam desde MongoDB.
    Cassandra los usa como contexto de las partidas históricas,
    manteniendo coherencia de nombres y appids entre los cuatro motores.
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
    print(f"  [MongoDB→Cassandra] {len(juegos)} juegos reales cargados.")
    return juegos


# ─────────────────────────────────────────────
# FUENTE 2 — JUGADORES REALES DESDE NEO4J
# ─────────────────────────────────────────────

def cargar_jugadores_desde_neo4j(driver):
    """
    Lee los nodos :Jugador del grafo Neo4j.
    Los mismos IDs que usa Redis para sesiones activas.
    Cassandra los usa para persistir el historial histórico de esos
    mismos jugadores — el ciclo de vida completo de una partida pasa
    por Redis (en vivo) y termina en Cassandra (persistido).
    """
    with driver.session() as session:
        resultado = session.run("""
            MATCH (j:Jugador)
            RETURN j.id AS id, j.nombre AS nombre, j.apellido AS apellido
            ORDER BY j.id
        """)
        jugadores = [{"id": r["id"],
                      "nombre": f"{r['nombre']} {r['apellido']}"}
                     for r in resultado]
    print(f"  [Neo4j→Cassandra] {len(jugadores)} jugadores reales cargados.")
    return jugadores


# ─────────────────────────────────────────────
# INSERCIÓN DE PARTIDAS
# ─────────────────────────────────────────────

RESULTADOS   = ["victoria", "derrota", "empate"]
PLATAFORMAS  = ["PC", "PS5", "Xbox Series X", "Steam Deck"]

def generar_partida(jugador, juego):
    """Genera los datos de una partida finalizada."""
    return {
        "partida_id":   uuid.uuid4(),
        "jugador_id":   jugador["id"],
        "nombre":       jugador["nombre"],
        "steam_appid":  str(juego.get("steam_appid", "0")),
        "juego_nombre": juego.get("name", "Desconocido"),
        "jugado_en":    datetime.now() - timedelta(
                            days=random.randint(0, 90),
                            hours=random.randint(0, 23),
                            minutes=random.randint(0, 59)
                        ),
        "duracion_min": random.randint(5, 120),
        "kills":        random.randint(0, 40),
        "resultado":    random.choice(RESULTADOS),
        "xp_ganado":    random.randint(50, 1500),
        "plataforma":   random.choice(PLATAFORMAS),
    }

def insertar_partida(session, p):
    """
    Escribe una partida en las dos tablas desnormalizadas.

    Comparación SQL:
      SQL: INSERT en tabla "partidas" + JOIN posterior para consultar.
      Cassandra: dos INSERT independientes, una por tabla de consulta.
      Sin FK, sin JOIN, sin índice secundario. La misma fila se duplica
      a propósito — escritura barata compensa lectura directa sin traversal.
    """
    # Tabla 1: por jugador
    session.execute("""
        INSERT INTO partidas_por_jugador
        (jugador_id, jugado_en, partida_id, steam_appid, juego_nombre,
         duracion_min, kills, resultado, xp_ganado, plataforma)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (p["jugador_id"], p["jugado_en"], p["partida_id"],
          p["steam_appid"], p["juego_nombre"], p["duracion_min"],
          p["kills"], p["resultado"], p["xp_ganado"], p["plataforma"]))

    # Tabla 2: por juego
    session.execute("""
        INSERT INTO partidas_por_juego
        (steam_appid, jugado_en, partida_id, jugador_id, juego_nombre,
         duracion_min, kills, resultado, xp_ganado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (p["steam_appid"], p["jugado_en"], p["partida_id"],
          p["jugador_id"], p["juego_nombre"], p["duracion_min"],
          p["kills"], p["resultado"], p["xp_ganado"]))

def actualizar_estadisticas(session, p):
    """
    Acumula estadísticas del jugador con UPDATE aritmético.
    Si el jugador no existe aún, Cassandra hace un upsert automático
    (INSERT + UPDATE son equivalentes en Cassandra para filas inexistentes).

    Comparación SQL:
      SQL: UPDATE jugadores SET xp = xp + ? WHERE id = ?
           → requiere read-before-write para chequear existencia
      Cassandra: UPDATE directo, el motor maneja la concurrencia internamente.
    """
    session.execute("""
        UPDATE estadisticas_jugador
        SET nombre           = %s,
            partidas_jugadas = partidas_jugadas + 1,
            xp_total         = xp_total         + %s,
            kills_total      = kills_total       + %s,
            minutos_totales  = minutos_totales   + %s,
            ultimo_juego     = %s,
            ultima_partida   = %s
        WHERE jugador_id = %s
    """, (p["nombre"], p["xp_ganado"], p["kills"],
          p["duracion_min"], p["juego_nombre"], p["jugado_en"],
          p["jugador_id"]))


# ─────────────────────────────────────────────
# CONSULTAS
# ─────────────────────────────────────────────

def historial_jugador(session, jugador_id, limite=10):
    """
    Q1: últimas N partidas de un jugador.
    Partition key = jugador_id → una sola partición, sin full scan.
    CLUSTERING ORDER BY jugado_en DESC → ya viene ordenado del disco.

    Comparación SQL:
      SELECT * FROM partidas
      JOIN jugadores ON ... JOIN juegos ON ...
      WHERE jugador_id = ? ORDER BY jugado_en DESC LIMIT ?
      → tres tablas, dos JOINs, sort en memoria.

      Cassandra: SELECT directo, sin JOIN, orden garantizado por el schema.
    """
    rows = session.execute("""
        SELECT jugador_id, jugado_en, juego_nombre, duracion_min,
               kills, resultado, xp_ganado, plataforma
        FROM partidas_por_jugador
        WHERE jugador_id = %s
        LIMIT %s
    """, (jugador_id, limite))
    return list(rows)

def ultimas_partidas_juego(session, steam_appid, limite=10):
    """
    Q2: últimas N partidas de un juego específico.
    Usa partidas_por_juego — tabla diseñada para esta consulta exacta.
    Sin esta tabla, habría que hacer un ALLOW FILTERING (full scan),
    que Cassandra desaconseja para producción.
    """
    rows = session.execute("""
        SELECT steam_appid, jugado_en, jugador_id, juego_nombre,
               duracion_min, kills, resultado, xp_ganado
        FROM partidas_por_juego
        WHERE steam_appid = %s
        LIMIT %s
    """, (steam_appid, limite))
    return list(rows)

def estadisticas_jugador(session, jugador_id):
    """Q3: perfil acumulado de un jugador — una sola fila, lectura O(1)."""
    row = session.execute("""
        SELECT * FROM estadisticas_jugador
        WHERE jugador_id = %s
    """, (jugador_id,)).one()
    return row

def top_jugadores_por_xp(session, limite=10):
    """
    Q4: top jugadores por XP total acumulado.
    Requiere ALLOW FILTERING porque xp_total no es partition key.
    Esto está bien para una demo; en producción se usaría una tabla
    adicional desnormalizada con xp_total como clustering key DESC,
    o se delega a Redis (que ya tiene el leaderboard en tiempo real).

    Este es el punto donde Redis y Cassandra se complementan:
    Redis sirve el top en milisegundos durante la partida;
    Cassandra lo respalda con el histórico persistido.
    """
    rows = session.execute("""
        SELECT jugador_id, nombre, xp_total, partidas_jugadas,
               kills_total, minutos_totales
        FROM estadisticas_jugador
        LIMIT %s
        ALLOW FILTERING
    """, (limite,))
    resultados = sorted(rows, key=lambda r: r.xp_total or 0, reverse=True)
    return resultados[:limite]

def promedio_duracion_por_juego(session, steam_appid):
    """
    Q5 (valor agregado): duración promedio de partidas en un juego.
    Lee las últimas 100 partidas del juego y calcula el promedio en Python.
    Cassandra no tiene AVG() nativo — el cálculo se hace en la capa de app,
    que es el patrón esperado en bases orientadas a escritura masiva.
    """
    rows = session.execute("""
        SELECT duracion_min FROM partidas_por_juego
        WHERE steam_appid = %s
        LIMIT 100
    """, (steam_appid,))
    duraciones = [r.duracion_min for r in rows if r.duracion_min]
    if not duraciones:
        return None
    return round(sum(duraciones) / len(duraciones), 1)


# ─────────────────────────────────────────────
# INTEGRACIÓN: fin de partida completo
# ─────────────────────────────────────────────

def registrar_fin_de_partida(session, redis_client, jugador, juego):
    """
    Flujo completo de fin de partida — integración Redis + Cassandra:

      1. Se genera el resultado de la partida.
      2. Cassandra persiste el registro histórico (permanente).
      3. Redis actualiza el leaderboard en vivo (en memoria).

    Redis y Cassandra tienen roles distintos sobre el mismo evento:
      Redis  → velocidad, memoria, tiempo real (se puede perder al reiniciar)
      Cassandra → durabilidad, historial, análisis posterior

    Este patrón es write-through: se escribe en los dos motores
    sincrónicamente para garantizar consistencia inmediata.
    """
    p = generar_partida(jugador, juego)
    insertar_partida(session, p)
    actualizar_estadisticas(session, p)

    if redis_client:
        try:
            redis_client.zincrby("leaderboard:global", p["xp_ganado"], jugador["id"])
        except Exception:
            pass  # Redis opcional — Cassandra persiste igual

    return p


# ─────────────────────────────────────────────
# LIMPIEZA
# ─────────────────────────────────────────────

def limpiar_datos(session):
    """Trunca las tres tablas para empezar la demo desde cero."""
    for tabla in ["partidas_por_jugador", "partidas_por_juego",
                  "estadisticas_jugador"]:
        session.execute(f"TRUNCATE {tabla}")
    print("  [limpieza] Tablas truncadas.")


# ─────────────────────────────────────────────
# DEMO PRINCIPAL
# ─────────────────────────────────────────────

def demo():
    print("=" * 65)
    print("  GAMENEXUS — Cassandra: Historial de Partidas")
    print("  TP Integrador — Ingeniería de Datos II — UADE 2026")
    print("=" * 65)

    cluster, session = conectar_cassandra()
    db               = conectar_mongo()
    driver           = conectar_neo4j()

    # Intentar conectar Redis para el flujo integrado (opcional)
    redis_client = None
    try:
        import redis as redis_lib
        redis_client = redis_lib.Redis(host="localhost", port=6379,
                                       decode_responses=True)
        redis_client.ping()
        print("[OK] Redis    → localhost:6379 (integración activa)")
    except Exception:
        print("[--] Redis no disponible — se omite actualización de leaderboard")

    # ── Schema ────────────────────────────────────────────────────
    print("\n[0] CREANDO SCHEMA EN CASSANDRA")
    crear_schema(session)

    # ── Datos reales de MongoDB y Neo4j ───────────────────────────
    print("\n[1] CARGANDO DATOS REALES DESDE MONGODB Y NEO4J")
    juegos    = cargar_juegos_desde_mongo(db, n=15)
    jugadores = cargar_jugadores_desde_neo4j(driver)

    limpiar_datos(session)

    # ── Simular partidas finalizadas ───────────────────────────────
    print(f"\n[2] INSERTANDO {N_PARTIDAS} PARTIDAS HISTÓRICAS")
    print("    (desnormalizadas en partidas_por_jugador y partidas_por_juego)")
    for i in range(N_PARTIDAS):
        jugador = random.choice(jugadores)
        juego   = random.choice(juegos)
        registrar_fin_de_partida(session, redis_client, jugador, juego)
    print(f"  {N_PARTIDAS} partidas insertadas en 2 tablas c/u = {N_PARTIDAS * 2} filas totales.")

    # ── Q1: Historial de un jugador ────────────────────────────────
    jugador_test = jugadores[0]
    print(f"\n[3] HISTORIAL DE {jugador_test['nombre'].upper()} (últimas 5 partidas)")
    historial = historial_jugador(session, jugador_test["id"], limite=5)
    if historial:
        for r in historial:
            ts = r.jugado_en.strftime("%Y-%m-%d %H:%M") if r.jugado_en else "?"
            print(f"  {ts}  {r.juego_nombre[:28]:<28}  "
                  f"{r.resultado:<8}  {r.kills:>2} kills  {r.xp_ganado:>5} XP")
    else:
        print(f"  (sin partidas registradas para {jugador_test['id']})")

    # ── Q2: Últimas partidas de un juego ───────────────────────────
    juego_test = random.choice(juegos)
    print(f"\n[4] ÚLTIMAS PARTIDAS EN: {juego_test.get('name','?')[:40]}")
    partidas_juego = ultimas_partidas_juego(session, str(juego_test.get("steam_appid")), limite=5)
    if partidas_juego:
        for r in partidas_juego:
            ts = r.jugado_en.strftime("%Y-%m-%d %H:%M") if r.jugado_en else "?"
            print(f"  {ts}  jugador: {r.jugador_id:<15}  "
                  f"{r.resultado:<8}  {r.kills:>2} kills  {r.duracion_min:>3} min")
    else:
        print("  (sin partidas registradas para ese juego)")

    # ── Q3: Estadísticas de un jugador ────────────────────────────
    print(f"\n[5] ESTADÍSTICAS ACUMULADAS — {jugador_test['nombre'].upper()}")
    stats = estadisticas_jugador(session, jugador_test["id"])
    if stats:
        print(f"  Partidas jugadas : {stats.partidas_jugadas}")
        print(f"  XP total         : {stats.xp_total}")
        print(f"  Kills totales    : {stats.kills_total}")
        print(f"  Minutos totales  : {stats.minutos_totales}")
        print(f"  Último juego     : {stats.ultimo_juego}")
    else:
        print("  (sin estadísticas aún)")

    # ── Q4: Top jugadores por XP ───────────────────────────────────
    print("\n[6] TOP 5 JUGADORES POR XP HISTÓRICO (Cassandra)")
    top = top_jugadores_por_xp(session, limite=5)
    for i, r in enumerate(top, 1):
        print(f"  #{i}  {(r.nombre or r.jugador_id):<30}  "
              f"XP: {r.xp_total or 0:>6}  "
              f"partidas: {r.partidas_jugadas or 0:>3}")

    # ── Q5: Duración promedio por juego ───────────────────────────
    print(f"\n[7] DURACIÓN PROMEDIO EN: {juego_test.get('name','?')[:40]}")
    promedio = promedio_duracion_por_juego(session, str(juego_test.get("steam_appid")))
    if promedio:
        print(f"  Promedio: {promedio} minutos por partida")
    else:
        print("  Sin datos suficientes.")

    # ── Integración: Redis vs Cassandra ───────────────────────────
    print("\n[8] COMPARATIVA REDIS vs CASSANDRA — mismo evento, roles distintos")
    print("  Redis    → leaderboard en vivo, respuesta < 1ms, volátil")
    print("  Cassandra→ historial persistido, consultas analíticas, durable")
    print("  Al finalizar una partida, el sistema escribe en ambos (write-through).")

    if redis_client:
        top_redis = redis_client.zrevrange("leaderboard:global", 0, 2, withscores=True)
        if top_redis:
            print("\n  Top 3 en Redis (tiempo real):")
            for jid, xp in top_redis:
                nombre = redis_client.get(f"nombre:{jid}") or jid
                print(f"    {nombre:<30}  {int(xp):>6} XP")

    print("\n" + "=" * 65)
    print("  Demo completada. MongoDB + Neo4j + Redis + Cassandra operando.")
    print("=" * 65)

    driver.close()
    cluster.shutdown()


if __name__ == "__main__":
    demo()
