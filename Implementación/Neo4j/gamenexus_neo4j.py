"""
================================================================================
  GAMENEXUS — Módulo Neo4j: Red Social de Jugadores
  Trabajo Práctico Integrador — Ingeniería de Datos II — UADE 2026
  Motor: Neo4j  |  Lenguaje: Python 3.12  |  Librería: neo4j (driver oficial)
  Versión demo: expone únicamente Q3 (amigos de amigos) y Q4 (recomendación).
================================================================================

Rol de Neo4j en GameNexus:
  Neo4j es el motor de red social del sistema. Modela las relaciones entre
  jugadores y juegos como un grafo nativo — algo imposible de representar
  eficientemente en una base de datos relacional.

  El dataset de Steam no incluye relaciones sociales explícitas entre usuarios.
  El grafo se construye INFIRIENDO afinidad: si dos jugadores tienen muchas
  horas en los mismos títulos, se los conecta con una relación [:AFINIDAD]
  ponderada. Esto se justifica porque en la realidad las plataformas sociales
  de gaming también usan co-ocurrencia de biblioteca para sugerir amigos.

  ─── Queries demostradas ─────────────────────────────────────────────────

  Q3 → ¿Con quién debería jugar esta noche? (traversal 2 saltos)
       Navega AMIGO_DE → AMIGO_DE para encontrar candidatos fuera de la
       lista de amigos directos, luego cruza con la biblioteca para filtrar
       solo los que tienen juegos en común. Dos patrones en cascada.
       En SQL: self-JOIN recursivo + dos JOINs adicionales con biblioteca
       + GROUP BY + HAVING. Cuatro JOINs mínimo, O(N²) en el peor caso.
       En Neo4j: traversal de dos saltos — la complejidad depende del grado
       del nodo, no del tamaño total del grafo.

  Q4 → Recomendación de juegos por la red social
       "¿Qué juegos tienen mis amigos que yo no tengo?"
       El filtro NOT ()-[:TIENE_EN_BIBLIOTECA]->() descarta juegos que el
       jugador ya posee, usando negación de patrón — sintaxis nativa de
       grafo que no existe en SQL estándar (requiere NOT EXISTS o LEFT JOIN
       + IS NULL). Pondera por horas promedio de los amigos, no solo por
       popularidad.

  ─── Comparación SQL ─────────────────────────────────────────────────────
    SQL: múltiples JOINs, JOIN recursivo para amigos de amigos, NOT EXISTS
    para negación de patrón. Costo crece con el tamaño total de las tablas.
    Neo4j: traversal directo sobre el grafo. Costo proporcional al grado
    del nodo (sus conexiones directas), independiente del total del grafo.

  ─── Integración con los otros motores ───────────────────────────────────
    - MongoDB  → juegos reales del catálogo Steam (nombre, steam_appid, género)
                 Neo4j crea un nodo :Juego por cada juego de MongoDB.
    - Redis    → usa los IDs de jugadores de Neo4j para sesiones en tiempo real.
                 Neo4j responde "quiénes son los amigos", Redis responde
                 "cuáles de esos están online ahora". Dos motores, un resultado.

Requisitos:
  pip install neo4j faker pymongo

Neo4j debe estar corriendo antes de ejecutar.
  - Neo4j Desktop: iniciar el proyecto y hacer Start en la DB
  - Docker: docker run --name neo4j-gamenexus -p 7474:7474 -p 7687:7687 \
            -e NEO4J_AUTH=neo4j/password -d neo4j:latest
  - URI por defecto: bolt://localhost:7687  |  user: neo4j  |  pass: password
  - Ajustá NEO4J_PASS en la sección CONFIGURACIÓN si usás otra contraseña.
================================================================================
"""

import random
from neo4j import GraphDatabase
from faker import Faker
from pymongo import MongoClient

fake = Faker("es_ES")
random.seed(42)


# ─────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────

MONGO_URI  = "mongodb://localhost:27017/"   # URI de MongoDB local (default)

NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"       # ← Cambiá por la contraseña que configuraste en Neo4j
                               #   Docker default: "password"  (NEO4J_AUTH=neo4j/password)

# Cantidad de jugadores y juegos a generar
N_JUGADORES = 50
N_JUEGOS    = 20

# Umbral mínimo de juegos en común para crear relación de afinidad
UMBRAL_AFINIDAD = 2


# ─────────────────────────────────────────────
#  CONEXIÓN
# ─────────────────────────────────────────────

def conectar():
    """Establece conexión con Neo4j y retorna el driver."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    driver.verify_connectivity()
    print(f"✓ Conectado a Neo4j → {NEO4J_URI}")
    return driver


# ─────────────────────────────────────────────
#  DATOS DESDE MONGODB
# ─────────────────────────────────────────────

def obtener_juegos_desde_mongodb(limit=20):
    """
    Lee los juegos del catálogo de MongoDB para usarlos como
    nodos :Juego en Neo4j. Así ambos motores comparten la misma
    fuente de verdad — MongoDB es el catálogo base.
    """
    client = MongoClient(MONGO_URI)
    col = client["gamenexus"]["games"]

    juegos = []
    cursor = col.find(
        {"reviews.total": {"$gte": 100}},   # solo juegos con reviews reales
        {"steam_appid": 1, "name": 1, "taxonomy.genres": 1, "_id": 0}
    ).sort("reviews.rating_pct", -1).limit(limit)

    for doc in cursor:
        genero = doc.get("taxonomy", {}).get("genres", ["Unknown"])
        juegos.append({
            "id":     str(doc["steam_appid"]),
            "nombre": doc["name"],
            "genero": genero[0] if genero else "Unknown"
        })

    client.close()
    return juegos


# ─────────────────────────────────────────────
#  DATOS SINTÉTICOS
# ─────────────────────────────────────────────

def generar_jugadores(juegos_catalogo):
    """
    Genera jugadores sintéticos con biblioteca de juegos y horas jugadas.
    Simula el patrón del Steam User Data del dataset.
    """
    if not juegos_catalogo:
        raise ValueError("El catálogo de juegos no puede estar vacío")

    jugadores = []
    for i in range(N_JUGADORES):
        # Cada jugador tiene entre 3 y 12 juegos en su biblioteca
        n_juegos = random.randint(3, 12)
        biblioteca = {}
        juegos_elegidos = random.sample(juegos_catalogo, min(n_juegos, len(juegos_catalogo)))
        for juego in juegos_elegidos:
            # Horas jugadas: distribución realista (mayoría pocas horas, algunos muchas)
            horas = round(random.expovariate(0.05), 1)  # media ~20hs, cola larga
            horas = min(horas, 2000)
            biblioteca[juego["id"]] = horas

        jugadores.append({
            "id":        f"player_{i+1:03d}",
            "nombre":    fake.first_name(),
            "apellido":  fake.last_name(),
            "pais":      random.choice(["AR", "BR", "MX", "CL", "CO", "ES"]),
            "nivel":     random.randint(1, 100),
            "biblioteca": biblioteca,   # {steam_appid: horas}
        })
    return jugadores


# ─────────────────────────────────────────────
#  CONSTRUCCIÓN DEL GRAFO
# ─────────────────────────────────────────────

def limpiar_grafo(driver):
    """Elimina todos los nodos y relaciones para empezar desde cero."""
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("✓ Grafo limpiado.")


def crear_constraints_e_indices(driver):
    """
    Crea constraints de unicidad e índices para optimizar traversals.

    Valor para el TP: en Neo4j los índices aceleran el lookup inicial
    (encontrar el nodo de entrada) pero los traversals del grafo son
    O(1) por relación — no dependen del tamaño total del grafo como
    los JOINs en SQL.
    """
    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT jugador_id IF NOT EXISTS
            FOR (j:Jugador) REQUIRE j.id IS UNIQUE
        """)
        session.run("""
            CREATE CONSTRAINT juego_id IF NOT EXISTS
            FOR (g:Juego) REQUIRE g.steam_appid IS UNIQUE
        """)
    print("✓ Constraints e índices creados.")


def cargar_juegos(driver, juegos):
    """Crea nodos :Juego en el grafo."""
    with driver.session() as session:
        for juego in juegos:
            session.run("""
                MERGE (g:Juego {steam_appid: $id})
                SET g.nombre = $nombre,
                    g.genero = $genero
            """, id=juego["id"], nombre=juego["nombre"], genero=juego["genero"])
    print(f"✓ {len(juegos)} nodos :Juego creados.")


def cargar_jugadores(driver, jugadores):
    """
    Crea nodos :Jugador y relaciones [:TIENE_EN_BIBLIOTECA] hacia :Juego.

    La relación lleva la propiedad 'horas' — eso permite ponderar
    la afinidad entre jugadores según cuánto tiempo comparten en un título.
    """
    with driver.session() as session:
        for j in jugadores:
            session.run("""
                MERGE (j:Jugador {id: $id})
                SET j.nombre   = $nombre,
                    j.apellido = $apellido,
                    j.pais     = $pais,
                    j.nivel    = $nivel
            """, **{k: v for k, v in j.items() if k != "biblioteca"})

            for steam_appid, horas in j["biblioteca"].items():
                session.run("""
                    MATCH (j:Jugador {id: $jugador_id})
                    MATCH (g:Juego   {steam_appid: $appid})
                    MERGE (j)-[r:TIENE_EN_BIBLIOTECA]->(g)
                    SET r.horas = $horas
                """, jugador_id=j["id"], appid=steam_appid, horas=horas)

    print(f"✓ {len(jugadores)} nodos :Jugador creados con sus bibliotecas.")


def inferir_afinidad(driver, jugadores):
    """
    Construye relaciones [:AFINIDAD] entre jugadores que comparten
    al menos UMBRAL_AFINIDAD juegos en común.

    Decisión de diseño: el peso de la afinidad es la suma de horas
    compartidas en los juegos en común — no solo el conteo.
    Esto premia a jugadores que comparten tiempo de juego real,
    no solo que tienen el mismo juego sin jugarlo.
    """
    relaciones = 0
    ids = [j["id"] for j in jugadores]

    with driver.session() as session:
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                result = session.run("""
                    MATCH (a:Jugador {id: $id_a})-[ra:TIENE_EN_BIBLIOTECA]->(g:Juego)
                          <-[rb:TIENE_EN_BIBLIOTECA]-(b:Jugador {id: $id_b})
                    WITH a, b,
                         count(g)                    AS juegos_en_comun,
                         sum(ra.horas + rb.horas)    AS horas_compartidas
                    WHERE juegos_en_comun >= $umbral
                    MERGE (a)-[af:AFINIDAD]-(b)
                    SET af.juegos_en_comun   = juegos_en_comun,
                        af.horas_compartidas = round(horas_compartidas)
                    RETURN juegos_en_comun
                """, id_a=ids[i], id_b=ids[j], umbral=UMBRAL_AFINIDAD)

                if result.single():
                    relaciones += 1

    print(f"✓ {relaciones} relaciones [:AFINIDAD] inferidas (umbral: {UMBRAL_AFINIDAD} juegos en común).")


def crear_amistades_explicitas(driver, jugadores):
    """
    Crea un subconjunto de relaciones [:AMIGO_DE] explícitas.
    Simula las amistades que los jugadores agregaron manualmente.
    """
    amistades = 0
    ids = [j["id"] for j in jugadores]

    with driver.session() as session:
        for jugador_id in ids:
            n_amigos = random.randint(1, 5)
            candidatos = [x for x in ids if x != jugador_id]
            amigos = random.sample(candidatos, min(n_amigos, len(candidatos)))
            for amigo_id in amigos:
                result = session.run("""
                    MATCH (a:Jugador {id: $id_a})
                    MATCH (b:Jugador {id: $id_b})
                    MERGE (a)-[r:AMIGO_DE]-(b)
                    RETURN r
                """, id_a=jugador_id, id_b=amigo_id)
                if result.single():
                    amistades += 1

    print(f"✓ {amistades} relaciones [:AMIGO_DE] creadas.")


# ─────────────────────────────────────────────
#  OPERACIONES CRUD
# ─────────────────────────────────────────────

def demo_crud(driver):
    sep = "─" * 60
    print(f"\n{sep}")
    print("  OPERACIONES CRUD")
    print(sep)

    with driver.session() as session:

        # ── CREATE: agregar un jugador nuevo ─────────────────────
        print("\n[CREATE] Insertando jugador 'GamerTest'...")
        session.run("""
            MERGE (j:Jugador {id: 'test_001'})
            SET j.nombre   = 'GamerTest',
                j.apellido = 'UADE',
                j.pais     = 'AR',
                j.nivel    = 99
        """)
        session.run("""
            MATCH (j:Jugador {id: 'test_001'})
            MATCH (g:Juego   {steam_appid: '570'})
            MERGE (j)-[:TIENE_EN_BIBLIOTECA {horas: 500.0}]->(g)
        """)
        print("  → Jugador 'GamerTest' creado y conectado a Dota 2.")

        # ── READ: buscar jugador por id ──────────────────────────
        print("\n[READ] Buscando jugador 'test_001'...")
        result = session.run("""
            MATCH (j:Jugador {id: 'test_001'})
            RETURN j.nombre AS nombre, j.nivel AS nivel, j.pais AS pais
        """)
        row = result.single()
        print(f"  → Encontrado: {row['nombre']} | Nivel {row['nivel']} | País {row['pais']}")

        # ── UPDATE: actualizar nivel ─────────────────────────────
        print("\n[UPDATE] Subiendo nivel a 100...")
        session.run("""
            MATCH (j:Jugador {id: 'test_001'})
            SET j.nivel = 100
        """)
        result = session.run("""
            MATCH (j:Jugador {id: 'test_001'})
            RETURN j.nivel AS nivel
        """)
        print(f"  → Nivel actualizado: {result.single()['nivel']}")

        # ── DELETE: eliminar jugador de prueba ───────────────────
        print("\n[DELETE] Eliminando jugador de prueba...")
        session.run("""
            MATCH (j:Jugador {id: 'test_001'})
            DETACH DELETE j
        """)
        print("  → Eliminado correctamente (DETACH DELETE elimina nodo + relaciones).")


# ─────────────────────────────────────────────
#  CONSULTAS DE DEMO — LAS 2 QUERIES ELEGIDAS
# ─────────────────────────────────────────────

def demo_queries(driver):
    """
    Las dos queries que mejor demuestran el valor de Neo4j para el TP:

      Q3 — Amigos de amigos con juegos en común (traversal 2 saltos)
           Demuestra la ventaja estructural sobre SQL: cero overhead extra
           por agregar un salto más en el traversal.

      Q4 — Recomendación colaborativa por la red social
           Demuestra negación de patrón nativa (NOT ()-[]->()) y agregación
           ponderada por horas — imposible en SQL sin subconsultas.

    Comparación SQL (para mencionar en la presentación):
      Q3: self-JOIN recursivo + dos JOINs con biblioteca + GROUP BY + HAVING
          → cuatro JOINs mínimo, O(N²) en el peor caso.
      Q4: NOT EXISTS o LEFT JOIN + IS NULL para la negación de patrón.
          → En Neo4j es un predicado de una línea.
    """
    sep = "─" * 60
    print(f"\n{sep}")
    print("  QUERIES DEMO — TRAVERSAL DE GRAFO")
    print(sep)

    with driver.session() as session:

        # Tomar un jugador de referencia que tenga amigos
        ref = session.run("""
            MATCH (j:Jugador)-[:AMIGO_DE]-()
            RETURN j.id AS id, j.nombre AS nombre
            LIMIT 1
        """).single()

        if not ref:
            print("  (No hay jugadores con amigos para hacer traversals)")
            return

        jugador_ref_id  = ref["id"]
        jugador_ref_nom = ref["nombre"]
        print(f"\n  Jugador de referencia: {jugador_ref_nom} ({jugador_ref_id})\n")

        # ── Q3: ¿Con quién debería jugar esta noche? ─────────────
        # Traversal de 2 saltos: amigos de amigos con juegos en común
        print(f"[Q3] ¿Con quién debería jugar {jugador_ref_nom} esta noche?")
        print("     (Amigos de amigos con juegos en común — traversal 2 saltos)")
        print()
        result = session.run("""
            MATCH (j:Jugador {id: $id})-[:AMIGO_DE]-(:Jugador)
                  -[:AMIGO_DE]-(candidato:Jugador)
            WHERE candidato.id <> $id
              AND NOT (j)-[:AMIGO_DE]-(candidato)
            MATCH (j)-[:TIENE_EN_BIBLIOTECA]->(juego:Juego)
                  <-[:TIENE_EN_BIBLIOTECA]-(candidato)
            WITH candidato, count(DISTINCT juego) AS juegos_comunes
            WHERE juegos_comunes >= 1
            RETURN candidato.nombre    AS nombre,
                   candidato.apellido  AS apellido,
                   candidato.nivel     AS nivel,
                   juegos_comunes
            ORDER BY juegos_comunes DESC, candidato.nivel DESC
            LIMIT 5
        """, id=jugador_ref_id)

        rows = result.data()
        if rows:
            print(f"  {'NOMBRE':<25} {'NIVEL':>6}  JUEGOS EN COMÚN")
            print(f"  {'─'*25} {'─'*6}  {'─'*15}")
            for r in rows:
                print(f"  {r['nombre']} {r['apellido']:<20} "
                      f"Nv {r['nivel']:>3}  {r['juegos_comunes']} juego(s) en común")
        else:
            print("  (No se encontraron candidatos con los datos actuales)")

        print()
        print("  SQL equivalente: self-JOIN recursivo sobre tabla amistades")
        print("                 + JOIN biblioteca × 2 + GROUP BY + HAVING")
        print("                 → 4 JOINs mínimo, O(N²) en el peor caso.")
        print("  Neo4j:          dos líneas extra en el MATCH. Sin overhead adicional.")

        # ── Q4: Recomendación de juegos por la red ────────────────
        # Juegos que tienen los amigos pero el jugador no tiene
        print(f"\n{'─'*60}")
        print(f"[Q4] Juegos recomendados para {jugador_ref_nom}:")
        print("     (Juegos que tienen sus amigos pero él no tiene)")
        print()
        result = session.run("""
            MATCH (j:Jugador {id: $id})-[:AMIGO_DE]-(amigo:Jugador)
                  -[r:TIENE_EN_BIBLIOTECA]->(juego:Juego)
            WHERE NOT (j)-[:TIENE_EN_BIBLIOTECA]->(juego)
            WITH juego,
                 count(DISTINCT amigo)    AS amigos_que_lo_tienen,
                 avg(r.horas)             AS horas_promedio_amigos
            ORDER BY amigos_que_lo_tienen DESC, horas_promedio_amigos DESC
            LIMIT 6
            RETURN juego.nombre              AS juego,
                   juego.genero              AS genero,
                   amigos_que_lo_tienen,
                   round(horas_promedio_amigos) AS horas_prom
        """, id=jugador_ref_id)

        rows = result.data()
        if rows:
            print(f"  {'JUEGO':<30} {'GÉNERO':<14} {'AMIGOS':>6}  HS PROM")
            print(f"  {'─'*30} {'─'*14} {'─'*6}  {'─'*7}")
            for r in rows:
                print(f"  {r['juego']:<30} [{r['genero']:<12}] "
                      f"{r['amigos_que_lo_tienen']:>4} amigo(s)  "
                      f"~{r['horas_prom']:.0f}hs")
        else:
            print("  (No hay recomendaciones con los datos actuales)")

        print()
        print("  Negación de patrón: NOT (j)-[:TIENE_EN_BIBLIOTECA]->(juego)")
        print("  → Descarta juegos que ya tiene. Sintaxis nativa de grafo.")
        print("  SQL equivalente:    NOT EXISTS (...) o LEFT JOIN + IS NULL")
        print("  Ponderación:        horas_promedio premia tiempo real de juego,")
        print("                      no solo popularidad por número de amigos.")


# ─────────────────────────────────────────────
#  ESTADÍSTICAS DEL GRAFO
# ─────────────────────────────────────────────

def mostrar_estadisticas(driver):
    sep = "─" * 60
    print(f"\n{sep}")
    print("  ESTADÍSTICAS DEL GRAFO")
    print(sep)

    with driver.session() as session:
        stats = session.run("""
            MATCH (j:Jugador) WITH count(j) AS jugadores
            MATCH (g:Juego)   WITH jugadores, count(g) AS juegos
            MATCH ()-[r:TIENE_EN_BIBLIOTECA]->() WITH jugadores, juegos, count(r) AS rel_biblioteca
            MATCH ()-[a:AFINIDAD]-()    WITH jugadores, juegos, rel_biblioteca, count(a)/2 AS rel_afinidad
            MATCH ()-[am:AMIGO_DE]-()   WITH jugadores, juegos, rel_biblioteca, rel_afinidad, count(am)/2 AS rel_amigos
            RETURN jugadores, juegos, rel_biblioteca, rel_afinidad, rel_amigos
        """).single()

        print(f"\n  Nodos :Jugador:                  {stats['jugadores']:>6,}")
        print(f"  Nodos :Juego:                    {stats['juegos']:>6,}")
        print(f"  Relaciones :TIENE_EN_BIBLIOTECA: {stats['rel_biblioteca']:>6,}")
        print(f"  Relaciones :AFINIDAD:            {stats['rel_afinidad']:>6,}")
        print(f"  Relaciones :AMIGO_DE:            {stats['rel_amigos']:>6,}")

        # Jugador más conectado
        top = session.run("""
            MATCH (j:Jugador)-[:AMIGO_DE]-()
            RETURN j.nombre AS nombre, j.apellido AS apellido,
                   count(*) AS conexiones
            ORDER BY conexiones DESC
            LIMIT 1
        """).single()
        if top:
            print(f"\n  Jugador más conectado: {top['nombre']} {top['apellido']} "
                  f"({top['conexiones']} amigos)")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  GAMENEXUS — Neo4j: Red Social de Jugadores")
    print("  TP Integrador — ID II — UADE 2026")
    print("  Versión DEMO: Q3 (amigos de amigos) + Q4 (recomendación)")
    print("=" * 60)

    driver = conectar()

    # 1. Limpiar y preparar grafo
    limpiar_grafo(driver)
    crear_constraints_e_indices(driver)

    # 2. Cargar datos (juegos desde MongoDB + jugadores sintéticos)
    juegos = obtener_juegos_desde_mongodb(limit=N_JUEGOS)
    jugadores = generar_jugadores(juegos)

    print(f"\n→ Cargando {len(juegos)} juegos y {len(jugadores)} jugadores...")
    cargar_juegos(driver, juegos)
    cargar_jugadores(driver, jugadores)

    # 3. Inferir relaciones de afinidad (núcleo del módulo)
    print("→ Infiriendo relaciones de afinidad...")
    inferir_afinidad(driver, jugadores)
    crear_amistades_explicitas(driver, jugadores)

    # 4. CRUD (demostración rápida)
    demo_crud(driver)

    # 5. Las dos queries de la demo
    demo_queries(driver)

    # 6. Estadísticas del grafo
    mostrar_estadisticas(driver)

    print(f"\n{'─'*60}")
    print("  ✓ Demo Neo4j completa.")
    print(f"{'─'*60}\n")

    driver.close()


if __name__ == "__main__":
    main()
