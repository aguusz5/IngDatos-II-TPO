"""
================================================================================
  GAMENEXUS — Módulo Cassandra: Historial de Partidas y Estadísticas
  Trabajo Práctico Integrador — Ingeniería de Datos II — UADE 2026
  Motor: Apache Cassandra  |  Lenguaje: Python 3.12  |  Librería: cassandra-driver
================================================================================

Rol de Cassandra en GameNexus:
  Cada partida finalizada genera un registro: quién jugó, cuándo, en qué
  juego, duración, kills, assists, resultado, puntaje obtenido.
  Con millones de jugadores activos esto es escritura masiva y continua.

  Cassandra resuelve esto con modelado orientado a consultas (Chebotko):
  no se parte de entidades sino de las preguntas que la app necesita
  responder. Cada tabla existe para servir una consulta específica.

  ─── Diagrama Chebotko — 7 tablas, 7 consultas ───────────────────────────

  Q1 → partidas_por_jugador
       ¿Historial reciente de un jugador?
       PRIMARY KEY (id_jugador) — clustering: fecha_partida DESC, id_partida ASC

  Q2 → partidas_por_jugador_y_juego
       ¿Partidas de un jugador en un juego específico?
       PRIMARY KEY ((id_jugador, id_juego)) — clave de partición COMPUESTA.
       clustering: fecha_partida DESC, id_partida ASC

  Q3 → detalle_partida
       ¿Detalle completo de una partida puntual?
       PRIMARY KEY (id_partida) — lookup directo por ID, sin clustering.

  Q4 → estadisticas_jugador_por_juego
       ¿Estadísticas acumuladas del jugador por juego? (victorias, tiempo, etc.)
       PRIMARY KEY (id_jugador, id_juego) — tipo COUNTER en todas las columnas.
       COUNTER: único tipo de Cassandra que permite col = col + n de forma
       atómica y distribuida, sin read-before-write. Las tablas COUNTER solo
       pueden tener columnas COUNTER además de la PK — no se puede mezclar
       con TEXT o INT. Por eso nombre_juego NO está en esta tabla.

  Q5 → ranking_puntajes_por_juego
       ¿Ranking histórico de mejores puntajes en un juego?
       PRIMARY KEY (id_juego) — clustering: puntaje DESC, fecha_partida ASC.
       El disco guarda las filas ya en orden → top-N sin sort en memoria.

  Q6 → partidas_recientes_jugador
       ¿Actividad reciente ligera de un jugador (para pantalla de ranking)?
       PRIMARY KEY (id_jugador) — misma partición que Q1, proyección sin
       duracion_seg. Tabla separada = consulta separada (principio Chebotko).

  Q7 → partidas_por_juego_y_dia
       ¿Partidas de un juego en un día determinado? (analítica)
       PRIMARY KEY ((id_juego, fecha_dia DATE)) — bucketing temporal.
       DATE en la partición evita particiones calientes con millones de filas
       por un solo juego popular.

  ─── Flujo Chebotko (encadenamiento de queries) ──────────────────────────
    Q1 extrae id_juego   → alimenta Q2
    Q2 extrae id_partida → alimenta Q3
    Q5 extrae id_jugador → alimenta Q6
    Q5 extrae id_juego   → alimenta Q7

  ─── Comparación SQL ─────────────────────────────────────────────────────
    SQL: una tabla "partidas" con FK; JOINs para cada consulta.
    Cassandra: 7 tablas desnormalizadas. El mismo evento se escribe 6 veces
    en tablas regulares + 1 UPDATE COUNTER. Sin JOINs, sin FK, sin índices
    secundarios. Escritura barata compensa lectura directa sin traversal.

  ─── Integración con los otros motores ───────────────────────────────────
    - MongoDB  → juegos reales (nombre, steam_appid, género)
    - Neo4j    → jugadores reales (mismos IDs que usan Redis y Neo4j)
    - Redis    → al finalizar una partida, Redis actualiza el leaderboard
                 en vivo y Cassandra persiste el registro histórico.
                 Dos motores con roles distintos sobre el mismo evento.

  ─── Nota sobre UUIDs ────────────────────────────────────────────────────
    El diagrama Chebotko usa UUID para id_jugador e id_juego.
    Los IDs de Neo4j (strings "player_001") y MongoDB (steam_appid numérico)
    se convierten a UUID determinístico via uuid5(). La misma entrada siempre
    produce el mismo UUID → coherencia cross-BD sin mapeo extra.

Requisitos:
  pip install cassandra-driver pymongo neo4j faker

  - Cassandra en localhost:9042
    Docker: docker run --name cassandra-gamenexus -p 9042:9042 -d cassandra:latest
  - MongoDB  en localhost:27017  (gamenexus_mongodb.py  ejecutado primero)
  - Neo4j    en localhost:7687   (gamenexus_neo4j.py    ejecutado primero)
"""

import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from cassandra.cluster import Cluster
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
NEO4J_PASS     = "password"        # ← Cambiá por la contraseña que configuraste en Neo4j
                                   #   Docker default: "password"  (NEO4J_AUTH=neo4j/password)

N_PARTIDAS     = 50


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


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

_UUID_NS = uuid.NAMESPACE_OID

def str_a_uuid(s: str) -> uuid.UUID:
    """
    Convierte un string (p.ej. "player_001" o "730") a UUID determinístico
    via uuid5. La misma entrada siempre produce el mismo UUID — garantiza
    coherencia entre inserciones y consultas sin almacenar un mapeo extra.
    """
    return uuid.uuid5(_UUID_NS, str(s))


# ─────────────────────────────────────────────
# KEYSPACE Y TABLAS
# ─────────────────────────────────────────────

def crear_schema(session):
    """
    Crea el keyspace y las 7 tablas del módulo según el diagrama Chebotko.

    Notas de implementación:
      - estadisticas_jugador_por_juego usa COUNTER. Restricción del motor:
        todas las columnas no-PK deben ser COUNTER. No se puede mezclar
        TEXT/INT con COUNTER en la misma tabla. Por eso nombre_juego no
        aparece aquí — se puede obtener de cualquier otra tabla via id_juego.
      - partidas_por_jugador_y_juego y partidas_por_juego_y_dia usan clave
        de partición compuesta (dos columnas entre paréntesis dobles).
      - SimpleStrategy replication_factor=1 es suficiente para desarrollo
        local. En producción se usaría NetworkTopologyStrategy.
    """
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    """)
    session.set_keyspace(KEYSPACE)

    # ── Tabla 1 — Q1: historial reciente de un jugador ─────────────────────
    session.execute("""
        CREATE TABLE IF NOT EXISTS partidas_por_jugador (
            id_jugador    uuid,
            fecha_partida timestamp,
            id_partida    uuid,
            id_juego      uuid,
            nombre_juego  text,
            resultado     text,
            duracion_seg  int,
            puntaje       int,
            PRIMARY KEY (id_jugador, fecha_partida, id_partida)
        ) WITH CLUSTERING ORDER BY (fecha_partida DESC, id_partida ASC)
    """)

    # ── Tabla 2 — Q2: partidas de un jugador en un juego específico ────────
    # Partición compuesta (id_jugador, id_juego): la consulta siempre
    # filtra por ambos → una sola partición accedida, sin scatter.
    session.execute("""
        CREATE TABLE IF NOT EXISTS partidas_por_jugador_y_juego (
            id_jugador    uuid,
            id_juego      uuid,
            fecha_partida timestamp,
            id_partida    uuid,
            nombre_juego  text,
            resultado     text,
            duracion_seg  int,
            puntaje       int,
            PRIMARY KEY ((id_jugador, id_juego), fecha_partida, id_partida)
        ) WITH CLUSTERING ORDER BY (fecha_partida DESC, id_partida ASC)
    """)

    # ── Tabla 3 — Q3: detalle completo de una partida puntual ──────────────
    # Lookup directo por id_partida — lectura O(1) sin clustering.
    # Incluye kills y assists que no están en las tablas de historial.
    session.execute("""
        CREATE TABLE IF NOT EXISTS detalle_partida (
            id_partida    uuid PRIMARY KEY,
            id_jugador    uuid,
            id_juego      uuid,
            nombre_juego  text,
            fecha_partida timestamp,
            resultado     text,
            duracion_seg  int,
            puntaje       int,
            kills         int,
            assists       int
        )
    """)

    # ── Tabla 4 — Q4: estadísticas acumuladas por jugador y juego ──────────
    # COUNTER: tipo especial de Cassandra para acumuladores distribuidos.
    # col = col + n es atómico sin read-before-write → seguro bajo concurrencia.
    # Restricción del motor: en una tabla COUNTER, todas las columnas no-PK
    # deben ser COUNTER. Mezclar TEXT o INT con COUNTER genera InvalidRequest.
    # Por eso nombre_juego está ausente de esta tabla.
    session.execute("""
        CREATE TABLE IF NOT EXISTS estadisticas_jugador_por_juego (
            id_jugador      uuid,
            id_juego        uuid,
            total_partidas  counter,
            total_victorias counter,
            total_derrotas  counter,
            horas_jugadas   counter,
            PRIMARY KEY (id_jugador, id_juego)
        )
    """)

    # ── Tabla 5 — Q5: ranking histórico de puntajes en un juego ───────────
    # Clustering: puntaje DESC, fecha_partida ASC, id_partida ASC.
    # Los datos se guardan en disco ya en ese orden → LIMIT n sin sort.
    session.execute("""
        CREATE TABLE IF NOT EXISTS ranking_puntajes_por_juego (
            id_juego       uuid,
            puntaje        int,
            fecha_partida  timestamp,
            id_partida     uuid,
            id_jugador     uuid,
            nombre_jugador text,
            duracion_seg   int,
            PRIMARY KEY (id_juego, puntaje, fecha_partida, id_partida)
        ) WITH CLUSTERING ORDER BY (puntaje DESC, fecha_partida ASC, id_partida ASC)
    """)

    # ── Tabla 6 — Q6: actividad reciente ligera de un jugador ─────────────
    # Misma partición que Q1 pero sin duracion_seg — proyección ligera para
    # mostrar el perfil del rival desde la pantalla del ranking.
    # Dos tablas con el mismo partition key pero distinto propósito y columnas
    # es un patrón válido en Chebotko cuando las vistas sirven UIs distintas.
    session.execute("""
        CREATE TABLE IF NOT EXISTS partidas_recientes_jugador (
            id_jugador    uuid,
            fecha_partida timestamp,
            id_partida    uuid,
            id_juego      uuid,
            nombre_juego  text,
            resultado     text,
            puntaje       int,
            PRIMARY KEY (id_jugador, fecha_partida, id_partida)
        ) WITH CLUSTERING ORDER BY (fecha_partida DESC, id_partida ASC)
    """)

    # ── Tabla 7 — Q7: partidas de un juego en un día (analítica) ──────────
    # Partición compuesta (id_juego, fecha_dia DATE): bucketing temporal.
    # Sin el DATE, todas las partidas de un juego popular irían a una sola
    # partición → partición caliente con millones de filas.
    session.execute("""
        CREATE TABLE IF NOT EXISTS partidas_por_juego_y_dia (
            id_juego       uuid,
            fecha_dia      date,
            fecha_partida  timestamp,
            id_partida     uuid,
            id_jugador     uuid,
            nombre_jugador text,
            resultado      text,
            puntaje        int,
            PRIMARY KEY ((id_juego, fecha_dia), fecha_partida, id_partida)
        ) WITH CLUSTERING ORDER BY (fecha_partida DESC, id_partida ASC)
    """)

    print("  [schema] Keyspace y 7 tablas creadas/verificadas.")


# ─────────────────────────────────────────────
# FUENTES DE DATOS
# ─────────────────────────────────────────────

def cargar_juegos_desde_mongo(db, n=15):
    """
    Lee juegos reales del catálogo Steam desde MongoDB.
    Cassandra los usa como contexto de las partidas históricas,
    manteniendo coherencia de nombres y appids entre los cuatro motores.
    """
    pipeline = [
        {"$match": {
            "name":            {"$exists": True, "$ne": ""},
            "taxonomy.genres": {"$exists": True, "$not": {"$size": 0}}
        }},
        {"$sample": {"size": n}},
        {"$project": {"_id": 0, "steam_appid": 1, "name": 1}}
    ]
    juegos = list(db[MONGO_COL].aggregate(pipeline))
    print(f"  [MongoDB→Cassandra] {len(juegos)} juegos reales cargados.")
    return juegos


def cargar_jugadores_desde_neo4j(driver):
    """
    Lee los nodos :Jugador del grafo Neo4j.
    Los mismos IDs que usa Redis para sesiones activas.
    Cassandra los usa para persistir el historial histórico — el ciclo de
    vida completo de una partida pasa por Redis (en vivo) y termina en
    Cassandra (persistido).
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
# GENERACIÓN E INSERCIÓN
# ─────────────────────────────────────────────

RESULTADOS  = ["victoria", "derrota", "empate"]
PLATAFORMAS = ["PC", "PS5", "Xbox Series X", "Steam Deck"]


def generar_partida(jugador, juego):
    """Genera los datos de una partida finalizada."""
    fecha = datetime.now() - timedelta(
        days=random.randint(0, 90),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    return {
        "id_partida":     uuid.uuid4(),
        "id_jugador":     str_a_uuid(jugador["id"]),
        "nombre_jugador": jugador["nombre"],
        "id_juego":       str_a_uuid(juego.get("steam_appid", "0")),
        "nombre_juego":   juego.get("name", "Desconocido"),
        "fecha_partida":  fecha,
        "fecha_dia":      fecha.date(),
        "duracion_seg":   random.randint(5, 120) * 60,
        "kills":          random.randint(0, 40),
        "assists":        random.randint(0, 20),
        "resultado":      random.choice(RESULTADOS),
        "puntaje":        random.randint(50, 1500),
        "plataforma":     random.choice(PLATAFORMAS),
    }


def insertar_partida(session, p):
    """
    Escribe una partida en las 6 tablas desnormalizadas regulares.
    La tabla COUNTER (estadisticas_jugador_por_juego) se actualiza
    por separado en actualizar_contador() con UPDATE — las tablas COUNTER
    no aceptan INSERT, solo UPDATE con sintaxis col = col + n.

    Comparación SQL:
      SQL: un INSERT en "partidas" + JOINs posteriores.
      Cassandra: 6 INSERTs independientes, una por tabla de consulta.
      Sin FK, sin JOIN. La misma partida se duplica a propósito.
    """
    # Tabla 1
    session.execute("""
        INSERT INTO partidas_por_jugador
        (id_jugador, fecha_partida, id_partida, id_juego,
         nombre_juego, resultado, duracion_seg, puntaje)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (p["id_jugador"], p["fecha_partida"], p["id_partida"], p["id_juego"],
          p["nombre_juego"], p["resultado"], p["duracion_seg"], p["puntaje"]))

    # Tabla 2 — clave de partición compuesta (id_jugador, id_juego)
    session.execute("""
        INSERT INTO partidas_por_jugador_y_juego
        (id_jugador, id_juego, fecha_partida, id_partida,
         nombre_juego, resultado, duracion_seg, puntaje)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (p["id_jugador"], p["id_juego"], p["fecha_partida"], p["id_partida"],
          p["nombre_juego"], p["resultado"], p["duracion_seg"], p["puntaje"]))

    # Tabla 3 — detalle completo incluyendo kills y assists
    session.execute("""
        INSERT INTO detalle_partida
        (id_partida, id_jugador, id_juego, nombre_juego,
         fecha_partida, resultado, duracion_seg, puntaje, kills, assists)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (p["id_partida"], p["id_jugador"], p["id_juego"], p["nombre_juego"],
          p["fecha_partida"], p["resultado"], p["duracion_seg"], p["puntaje"],
          p["kills"], p["assists"]))

    # Tabla 5 — ranking por puntaje; el clustering DESC ya ordena en disco
    session.execute("""
        INSERT INTO ranking_puntajes_por_juego
        (id_juego, puntaje, fecha_partida, id_partida,
         id_jugador, nombre_jugador, duracion_seg)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (p["id_juego"], p["puntaje"], p["fecha_partida"], p["id_partida"],
          p["id_jugador"], p["nombre_jugador"], p["duracion_seg"]))

    # Tabla 6 — proyección ligera sin duracion_seg
    session.execute("""
        INSERT INTO partidas_recientes_jugador
        (id_jugador, fecha_partida, id_partida, id_juego,
         nombre_juego, resultado, puntaje)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (p["id_jugador"], p["fecha_partida"], p["id_partida"], p["id_juego"],
          p["nombre_juego"], p["resultado"], p["puntaje"]))

    # Tabla 7 — bucketing por DATE; fecha_dia es datetime.date, no timestamp
    session.execute("""
        INSERT INTO partidas_por_juego_y_dia
        (id_juego, fecha_dia, fecha_partida, id_partida,
         id_jugador, nombre_jugador, resultado, puntaje)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (p["id_juego"], p["fecha_dia"], p["fecha_partida"], p["id_partida"],
          p["id_jugador"], p["nombre_jugador"], p["resultado"], p["puntaje"]))


def actualizar_contador(session, p):
    """
    Actualiza los COUNTERs de estadisticas_jugador_por_juego.

    Por qué COUNTER y no INT con acumulación en Python (workaround anterior):
      En un sistema distribuido, varios nodos pueden recibir partidas del
      mismo jugador en paralelo. Con INT necesitarías leer el valor actual
      antes de escribir (read-before-write), lo cual Cassandra no garantiza
      seguro bajo concurrencia. COUNTER resuelve esto: el incremento
      col = col + n es atómico a nivel de motor, sin leer antes.

    Sintaxis obligatoria para COUNTER: UPDATE ... SET col = col + n
    No existe INSERT para tablas COUNTER. Si el WHERE no matchea ninguna
    fila, Cassandra crea la fila con el incremento como valor inicial.
    """
    es_victoria = 1 if p["resultado"] == "victoria" else 0
    es_derrota  = 1 if p["resultado"] == "derrota"  else 0

    session.execute("""
        UPDATE estadisticas_jugador_por_juego
        SET total_partidas  = total_partidas  + %s,
            total_victorias = total_victorias + %s,
            total_derrotas  = total_derrotas  + %s,
            horas_jugadas   = horas_jugadas   + %s
        WHERE id_jugador = %s AND id_juego = %s
    """, (1, es_victoria, es_derrota, p["duracion_seg"],
          p["id_jugador"], p["id_juego"]))


# ─────────────────────────────────────────────
# CONSULTAS Q1 – Q7
# ─────────────────────────────────────────────

def q1_historial_jugador(session, id_jugador, limite=10):
    """
    Q1: últimas N partidas de un jugador.
    Partition key = id_jugador → una sola partición, sin full scan.
    CLUSTERING ORDER BY fecha_partida DESC → ya viene ordenado del disco.

    SQL equivalente (tres tablas, dos JOINs, sort en memoria):
      SELECT * FROM partidas JOIN jugadores ON ... JOIN juegos ON ...
      WHERE jugador_id = ? ORDER BY fecha_partida DESC LIMIT ?
    """
    return list(session.execute("""
        SELECT id_jugador, fecha_partida, id_partida, id_juego,
               nombre_juego, resultado, duracion_seg, puntaje
        FROM partidas_por_jugador
        WHERE id_jugador = %s
        LIMIT %s
    """, (id_jugador, limite)))


def q2_partidas_por_jugador_y_juego(session, id_jugador, id_juego, limite=10):
    """
    Q2: partidas de un jugador en un juego específico.
    Clave compuesta (id_jugador, id_juego) → una sola partición con ambas claves.
    Sin esta tabla sería ALLOW FILTERING (full scan desaconsejado en producción).
    """
    return list(session.execute("""
        SELECT id_jugador, id_juego, fecha_partida, id_partida,
               nombre_juego, resultado, duracion_seg, puntaje
        FROM partidas_por_jugador_y_juego
        WHERE id_jugador = %s AND id_juego = %s
        LIMIT %s
    """, (id_jugador, id_juego, limite)))


def q3_detalle_partida(session, id_partida):
    """Q3: detalle completo de una partida puntual — lectura O(1)."""
    return session.execute("""
        SELECT * FROM detalle_partida WHERE id_partida = %s
    """, (id_partida,)).one()


def q4_estadisticas_jugador(session, id_jugador):
    """
    Q4: estadísticas acumuladas de un jugador, desglosadas por juego.
    Devuelve una fila por juego con los COUNTERs actualizados.
    """
    return list(session.execute("""
        SELECT id_jugador, id_juego,
               total_partidas, total_victorias, total_derrotas, horas_jugadas
        FROM estadisticas_jugador_por_juego
        WHERE id_jugador = %s
    """, (id_jugador,)))


def q5_ranking_puntajes_juego(session, id_juego, limite=10):
    """
    Q5: ranking histórico de mejores puntajes en un juego.
    Datos en disco ya ordenados por puntaje DESC → LIMIT sin sort en memoria.
    """
    return list(session.execute("""
        SELECT id_juego, puntaje, fecha_partida, id_partida,
               id_jugador, nombre_jugador, duracion_seg
        FROM ranking_puntajes_por_juego
        WHERE id_juego = %s
        LIMIT %s
    """, (id_juego, limite)))


def q6_partidas_recientes(session, id_jugador, limite=5):
    """
    Q6: actividad reciente ligera de un jugador.
    Proyección sin duracion_seg para la pantalla del ranking donde se
    quiere mostrar el historial del rival sin datos de rendimiento pesados.
    """
    return list(session.execute("""
        SELECT id_jugador, fecha_partida, id_partida,
               id_juego, nombre_juego, resultado, puntaje
        FROM partidas_recientes_jugador
        WHERE id_jugador = %s
        LIMIT %s
    """, (id_jugador, limite)))


def q7_partidas_por_juego_y_dia(session, id_juego, fecha_dia, limite=20):
    """
    Q7: partidas de un juego en un día determinado.
    Clave compuesta (id_juego, fecha_dia) → bucketing temporal.
    Fecha_dia es datetime.date — el driver mapea a CQL DATE automáticamente.
    """
    return list(session.execute("""
        SELECT id_juego, fecha_dia, fecha_partida, id_partida,
               id_jugador, nombre_jugador, resultado, puntaje
        FROM partidas_por_juego_y_dia
        WHERE id_juego = %s AND fecha_dia = %s
        LIMIT %s
    """, (id_juego, fecha_dia, limite)))


# ─────────────────────────────────────────────
# INTEGRACIÓN: fin de partida completo
# ─────────────────────────────────────────────

def registrar_fin_de_partida(session, redis_client, jugador, juego):
    """
    Flujo completo de fin de partida — integración Redis + Cassandra:

      1. Se genera el resultado de la partida.
      2. Cassandra persiste en 6 tablas regulares (INSERT).
      3. Cassandra incrementa los COUNTERs de estadísticas (UPDATE atómico).
      4. Redis actualiza el leaderboard en vivo (ZINCRBY).

    Redis y Cassandra tienen roles distintos sobre el mismo evento:
      Redis     → velocidad, en memoria, tiempo real (volátil al reiniciar)
      Cassandra → durabilidad, historial, análisis posterior (7 tablas)
    """
    p = generar_partida(jugador, juego)
    insertar_partida(session, p)
    actualizar_contador(session, p)

    if redis_client:
        try:
            redis_client.zincrby("leaderboard:global", p["puntaje"], jugador["id"])
        except Exception:
            pass

    return p


# ─────────────────────────────────────────────
# LIMPIEZA
# ─────────────────────────────────────────────

def limpiar_datos(session):
    """Trunca las 7 tablas para empezar la demo desde cero."""
    tablas = [
        "partidas_por_jugador",
        "partidas_por_jugador_y_juego",
        "detalle_partida",
        "estadisticas_jugador_por_juego",
        "ranking_puntajes_por_juego",
        "partidas_recientes_jugador",
        "partidas_por_juego_y_dia",
    ]
    for tabla in tablas:
        session.execute(f"TRUNCATE {tabla}")
    print("  [limpieza] 7 tablas truncadas.")


# ─────────────────────────────────────────────
# DEMO PRINCIPAL
# ─────────────────────────────────────────────

def demo():
    print("=" * 65)
    print("  GAMENEXUS — Cassandra: Historial de Partidas (Chebotko)")
    print("  TP Integrador — Ingeniería de Datos II — UADE 2026")
    print("=" * 65)

    cluster, session = conectar_cassandra()
    db               = conectar_mongo()
    driver           = conectar_neo4j()

    redis_client = None
    try:
        import redis as redis_lib
        redis_client = redis_lib.Redis(host="localhost", port=6379,
                                       decode_responses=True)
        redis_client.ping()
        print("[OK] Redis    → localhost:6379 (integración activa)")
    except Exception:
        print("[--] Redis no disponible — se omite actualización de leaderboard")

    # ── Schema ─────────────────────────────────────────────────────────────
    print("\n[0] CREANDO SCHEMA EN CASSANDRA (7 tablas Chebotko)")
    crear_schema(session)

    # ── Datos reales ───────────────────────────────────────────────────────
    print("\n[1] CARGANDO DATOS REALES DESDE MONGODB Y NEO4J")
    juegos    = cargar_juegos_desde_mongo(db, n=15)
    jugadores = cargar_jugadores_desde_neo4j(driver)

    limpiar_datos(session)

    # ── Simular partidas finalizadas ───────────────────────────────────────
    print(f"\n[2] INSERTANDO {N_PARTIDAS} PARTIDAS HISTÓRICAS")
    print("    (6 INSERTs en tablas regulares + 1 UPDATE COUNTER por partida)")
    partidas_insertadas = []
    for _ in range(N_PARTIDAS):
        jugador = random.choice(jugadores)
        juego   = random.choice(juegos)
        p = registrar_fin_de_partida(session, redis_client, jugador, juego)
        partidas_insertadas.append(p)
    print(f"  {N_PARTIDAS} partidas → {N_PARTIDAS * 6} filas en tablas regulares "
          f"+ {N_PARTIDAS} updates COUNTER.")

    # ── Preparar IDs de referencia para el flujo Chebotko ─────────────────
    jugador_test    = jugadores[0]
    id_jugador_test = str_a_uuid(jugador_test["id"])

    # ── Q1: Historial de un jugador ────────────────────────────────────────
    print(f"\n[3] Q1 — HISTORIAL DE {jugador_test['nombre'].upper()} (últimas 5)")
    print(f"    partidas_por_jugador  |  WHERE id_jugador = ?")
    historial = q1_historial_jugador(session, id_jugador_test, limite=5)
    id_juego_q2    = partidas_insertadas[0]["id_juego"]
    id_partida_q3  = partidas_insertadas[0]["id_partida"]
    if historial:
        for r in historial:
            ts = r.fecha_partida.strftime("%Y-%m-%d %H:%M") if r.fecha_partida else "?"
            print(f"  {ts}  {r.nombre_juego[:28]:<28}  {r.resultado:<8}  {r.puntaje:>5} pts")
        # Extraemos id_juego del primer resultado para alimentar Q2 (flujo Chebotko)
        id_juego_q2   = historial[0].id_juego
        id_partida_q3 = historial[0].id_partida
    else:
        print(f"  (sin partidas registradas para {jugador_test['id']})")

    # ── Q2: Partidas del jugador en ese juego (feed desde Q1) ─────────────
    print(f"\n[4] Q2 — {jugador_test['nombre'].split()[0].upper()} EN JUEGO "
          f"{str(id_juego_q2)[:8]}...  (id_juego extraído de Q1)")
    print(f"    partidas_por_jugador_y_juego  |  WHERE id_jugador = ? AND id_juego = ?")
    partidas_jxj = q2_partidas_por_jugador_y_juego(
        session, id_jugador_test, id_juego_q2, limite=5)
    if partidas_jxj:
        for r in partidas_jxj:
            ts = r.fecha_partida.strftime("%Y-%m-%d %H:%M") if r.fecha_partida else "?"
            print(f"  {ts}  {r.resultado:<8}  {r.duracion_seg // 60:>3} min  {r.puntaje:>5} pts")
        id_partida_q3 = partidas_jxj[0].id_partida
    else:
        print("  (sin partidas en esa combinación jugador+juego)")

    # ── Q3: Detalle de la partida (feed desde Q2) ─────────────────────────
    print(f"\n[5] Q3 — DETALLE DE PARTIDA {str(id_partida_q3)[:8]}...  "
          f"(id_partida extraído de Q2)")
    print(f"    detalle_partida  |  WHERE id_partida = ?")
    detalle = q3_detalle_partida(session, id_partida_q3)
    if detalle:
        ts = detalle.fecha_partida.strftime("%Y-%m-%d %H:%M") if detalle.fecha_partida else "?"
        print(f"  Juego     : {detalle.nombre_juego}")
        print(f"  Fecha     : {ts}")
        print(f"  Resultado : {detalle.resultado}")
        print(f"  Duración  : {detalle.duracion_seg // 60} min")
        print(f"  Puntaje   : {detalle.puntaje}")
        print(f"  Kills     : {detalle.kills}   Assists: {detalle.assists}")
    else:
        print("  (partida no encontrada)")

    # ── Q4: Estadísticas con COUNTERs ─────────────────────────────────────
    print(f"\n[6] Q4 — ESTADÍSTICAS ACUMULADAS DE {jugador_test['nombre'].split()[0].upper()} "
          f"(COUNTERs)")
    print(f"    estadisticas_jugador_por_juego  |  WHERE id_jugador = ?  [tipo COUNTER]")
    stats = q4_estadisticas_jugador(session, id_jugador_test)
    if stats:
        for r in stats:
            seg = r.horas_jugadas or 0
            print(f"  juego {str(r.id_juego)[:8]}...  "
                  f"partidas: {r.total_partidas or 0:>3}  "
                  f"victorias: {r.total_victorias or 0:>3}  "
                  f"derrotas: {r.total_derrotas or 0:>3}  "
                  f"tiempo: {seg // 3600}h {(seg % 3600) // 60}m")
    else:
        print("  (sin estadísticas aún)")

    # ── Q5: Ranking de un juego ────────────────────────────────────────────
    juego_rank    = random.choice(juegos)
    id_juego_rank = str_a_uuid(juego_rank.get("steam_appid", "0"))
    print(f"\n[7] Q5 — TOP 5 EN: {juego_rank.get('name', '?')[:40]}")
    print(f"    ranking_puntajes_por_juego  |  ORDER BY puntaje DESC (en disco)")
    ranking = q5_ranking_puntajes_juego(session, id_juego_rank, limite=5)
    if ranking:
        for i, r in enumerate(ranking, 1):
            ts = r.fecha_partida.strftime("%Y-%m-%d") if r.fecha_partida else "?"
            print(f"  #{i}  {(r.nombre_jugador or '?'):<28}  {r.puntaje:>5} pts  {ts}")
    else:
        print("  (sin partidas para ese juego aún — prueba con otro juego)")

    # ── Q6: Vista ligera (feed desde Q5: id_jugador del ranking) ──────────
    print(f"\n[8] Q6 — ACTIVIDAD RECIENTE LIGERA DE {jugador_test['nombre'].split()[0].upper()} "
          f"(id_jugador extraído de Q5)")
    print(f"    partidas_recientes_jugador  |  proyección sin duracion_seg")
    recientes = q6_partidas_recientes(session, id_jugador_test, limite=3)
    if recientes:
        for r in recientes:
            ts = r.fecha_partida.strftime("%Y-%m-%d %H:%M") if r.fecha_partida else "?"
            print(f"  {ts}  {r.nombre_juego[:28]:<28}  {r.resultado:<8}  {r.puntaje:>5} pts")
    else:
        print("  (sin partidas recientes)")

    # ── Q7: Analítica por día (feed desde Q5: id_juego del ranking) ───────
    p_ejemplo    = partidas_insertadas[0]
    id_juego_q7  = p_ejemplo["id_juego"]
    fecha_dia_q7 = p_ejemplo["fecha_dia"]
    print(f"\n[9] Q7 — JUEGO {str(id_juego_q7)[:8]}... EL {fecha_dia_q7}  "
          f"(id_juego extraído de Q5, bucketing DATE)")
    print(f"    partidas_por_juego_y_dia  |  WHERE (id_juego, fecha_dia) = ?")
    dia_partidas = q7_partidas_por_juego_y_dia(
        session, id_juego_q7, fecha_dia_q7, limite=5)
    if dia_partidas:
        for r in dia_partidas:
            ts = r.fecha_partida.strftime("%H:%M:%S") if r.fecha_partida else "?"
            print(f"  {ts}  {(r.nombre_jugador or '?'):<28}  {r.resultado:<8}  {r.puntaje:>5} pts")
    else:
        print("  (sin partidas en esa fecha para ese juego)")

    # ── Comparativa Redis vs Cassandra ─────────────────────────────────────
    print("\n[10] COMPARATIVA REDIS vs CASSANDRA — mismo evento, roles distintos")
    print("  Redis    → leaderboard en vivo, < 1ms, volátil")
    print("  Cassandra→ historial persistido, 7 tablas Chebotko, durable")
    print("  Fin de partida: 6 INSERTs + 1 UPDATE COUNTER + 1 ZINCRBY Redis.")

    if redis_client:
        top_redis = redis_client.zrevrange("leaderboard:global", 0, 2, withscores=True)
        if top_redis:
            print("\n  Top 3 en Redis (tiempo real):")
            for jid, pts in top_redis:
                print(f"    {jid:<25}  {int(pts):>6} pts")

    print("\n" + "=" * 65)
    print("  Demo completada. MongoDB + Neo4j + Redis + Cassandra operando.")
    print("=" * 65)

    driver.close()
    cluster.shutdown()


if __name__ == "__main__":
    demo()
