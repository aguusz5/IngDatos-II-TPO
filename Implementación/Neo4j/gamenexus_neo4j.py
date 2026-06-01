"""
================================================================================
  GAMENEXUS — Módulo Neo4j: Red Social de Jugadores
  Trabajo Práctico Integrador — Ingeniería de Datos II — UADE 2026
  Motor: Neo4j  |  Lenguaje: Python 3.12  |  Librería: neo4j (driver oficial)
================================================================================

Decisión de diseño (documentar en el TP):
  El dataset de Steam no incluye relaciones sociales explícitas entre usuarios.
  El grafo se construye INFIRIENDO afinidad: si dos jugadores tienen muchas
  horas en los mismos títulos, se los conecta con una relación [:AFINIDAD]
  ponderada. Esto se justifica porque en la realidad las plataformas sociales
  de gaming también usan co-ocurrencia de biblioteca para sugerir amigos.

Requisitos:
  pip install neo4j faker

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
#  CONFIGURACIÓN
# ─────────────────────────────────────────────

MONGO_URI  = "mongodb://localhost:27017/"   # URI de MongoDB local (default)

NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"       # ← Cambiá por la contraseña que configuraste en Neo4j
                               #   Docker default: "password"  (NEO4J_AUTH=neo4j/password)

# Cantidad de jugadores y juegos sintéticos a generar
N_JUGADORES = 50
N_JUEGOS    = 20

# Umbral mínimo de juegos en común para crear relación de afinidad
UMBRAL_AFINIDAD = 3


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
        # Constraint de unicidad — garantiza que no haya jugadores duplicados
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
            # Crear nodo jugador
            session.run("""
                MERGE (j:Jugador {id: $id})
                SET j.nombre   = $nombre,
                    j.apellido = $apellido,
                    j.pais     = $pais,
                    j.nivel    = $nivel
            """, **{k: v for k, v in j.items() if k != "biblioteca"})

            # Crear relación con cada juego de su biblioteca
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
                         count(g)          AS juegos_en_comun,
                         sum(ra.horas + rb.horas) AS horas_compartidas
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
        # Cada jugador tiene entre 1 y 5 amigos explícitos
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
        # Conectarlo a un juego
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
#  CONSULTAS AVANZADAS — TRAVERSALS DE GRAFO
# ─────────────────────────────────────────────

def demo_queries_avanzadas(driver):
    """
    Consultas de traversal que demuestran el valor de Neo4j.

    Comparación con SQL (útil para la presentación):
    En SQL, "amigos de amigos" requiere un JOIN recursivo sobre
    tablas de millones de filas. En Neo4j es un traversal directo
    sobre el grafo — O(1) por relación sin importar el tamaño total.
    """
    sep = "─" * 60
    print(f"\n{sep}")
    print("  CONSULTAS AVANZADAS — TRAVERSAL DE GRAFO")
    print(sep)

    with driver.session() as session:

        # Tomar un jugador de referencia para las demos
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

        # ── Q1: Biblioteca de un jugador ─────────────────────────
        print(f"[Q1] Biblioteca de {jugador_ref_nom}:")
        result = session.run("""
            MATCH (j:Jugador {id: $id})-[r:TIENE_EN_BIBLIOTECA]->(g:Juego)
            RETURN g.nombre AS juego, r.horas AS horas
            ORDER BY r.horas DESC
            LIMIT 8
        """, id=jugador_ref_id)
        for row in result:
            print(f"  {row['juego']:<30} {row['horas']:.1f} hs")

        # ── Q2: Amigos directos ──────────────────────────────────
        print(f"\n[Q2] Amigos directos de {jugador_ref_nom}:")
        result = session.run("""
            MATCH (j:Jugador {id: $id})-[:AMIGO_DE]-(amigo:Jugador)
            RETURN amigo.nombre AS nombre, amigo.apellido AS apellido,
                   amigo.nivel AS nivel, amigo.pais AS pais
            ORDER BY amigo.nivel DESC
        """, id=jugador_ref_id)
        rows = result.data()
        for r in rows:
            print(f"  {r['nombre']} {r['apellido']:<20} Nivel {r['nivel']}  [{r['pais']}]")
        print(f"  Total: {len(rows)} amigos")

        # ── Q3: ¿Con quién debería jugar esta noche? ─────────────
        # Amigos de amigos que tienen juegos en común (traversal 2 saltos)
        print(f"\n[Q3] ¿Con quién debería jugar {jugador_ref_nom} esta noche?")
        print("     (Amigos de amigos con juegos en común — 2 saltos)")
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
            for r in rows:
                print(f"  {r['nombre']} {r['apellido']:<20} "
                      f"Nivel {r['nivel']}  {r['juegos_comunes']} juegos en común")
        else:
            print("  (No se encontraron candidatos con los datos actuales)")

        # ── Q4: Recomendación de juegos por la red ───────────────
        # Juegos que tienen los amigos pero el jugador no tiene
        print(f"\n[Q4] Juegos recomendados para {jugador_ref_nom}:")
        print("     (Juegos que tienen sus amigos pero él no tiene)")
        result = session.run("""
            MATCH (j:Jugador {id: $id})-[:AMIGO_DE]-(amigo:Jugador)
                  -[r:TIENE_EN_BIBLIOTECA]->(juego:Juego)
            WHERE NOT (j)-[:TIENE_EN_BIBLIOTECA]->(juego)
            WITH juego, count(DISTINCT amigo) AS amigos_que_lo_tienen,
                 avg(r.horas) AS horas_promedio_amigos
            ORDER BY amigos_que_lo_tienen DESC, horas_promedio_amigos DESC
            LIMIT 6
            RETURN juego.nombre              AS juego,
                   juego.genero              AS genero,
                   amigos_que_lo_tienen,
                   round(horas_promedio_amigos) AS horas_prom
        """, id=jugador_ref_id)
        rows = result.data()
        if rows:
            for r in rows:
                print(f"  {r['juego']:<30} [{r['genero']:<12}] "
                      f"{r['amigos_que_lo_tienen']} amigos lo tienen "
                      f"(~{r['horas_prom']:.0f}hs)")
        else:
            print("  (No hay recomendaciones con los datos actuales)")

        # ── Q5: Top jugadores por afinidad con el jugador ref ────
        print(f"\n[Q5] Jugadores con mayor afinidad con {jugador_ref_nom}:")
        result = session.run("""
            MATCH (j:Jugador {id: $id})-[af:AFINIDAD]-(otro:Jugador)
            RETURN otro.nombre            AS nombre,
                   otro.apellido          AS apellido,
                   af.juegos_en_comun     AS juegos,
                   af.horas_compartidas   AS horas
            ORDER BY af.juegos_en_comun DESC, af.horas_compartidas DESC
            LIMIT 8
        """, id=jugador_ref_id)
        rows = result.data()
        if rows:
            for r in rows:
                print(f"  {r['nombre']} {r['apellido']:<20} "
                      f"{r['juegos']} juegos en común  |  {r['horas']} hs compartidas")
        else:
            print("  (No hay relaciones de afinidad para este jugador)")

        # ── Q6: Comunidades por género de juego ──────────────────
        print("\n[Q6] Top géneros por jugadores activos en la red:")
        result = session.run("""
            MATCH (j:Jugador)-[r:TIENE_EN_BIBLIOTECA]->(g:Juego)
            WITH g.genero AS genero,
                 count(DISTINCT j) AS jugadores,
                 sum(r.horas)      AS horas_totales
            ORDER BY jugadores DESC
            RETURN genero,
                   jugadores,
                   round(horas_totales) AS horas_totales
        """)
        for row in result:
            barra = "█" * row["jugadores"]
            print(f"  {row['genero']:<15} {row['jugadores']:>3} jugadores  "
                  f"{row['horas_totales']:>8.0f} hs  {barra}")

        # ── Q7: Grado de separación entre dos jugadores ──────────
        # Equivalente al "6 degrees of separation" en la red
        print("\n[Q7] Grado de separación entre dos jugadores:")
        ids_result = session.run("""
            MATCH (j:Jugador) RETURN j.id AS id LIMIT 2
        """).data()
        if len(ids_result) >= 2:
            id_a, id_b = ids_result[0]["id"], ids_result[1]["id"]
            result = session.run("""
                MATCH path = shortestPath(
                    (a:Jugador {id: $id_a})-[:AMIGO_DE*]-(b:Jugador {id: $id_b})
                )
                RETURN length(path) AS grado,
                       [n IN nodes(path) | n.nombre] AS camino
            """, id_a=id_a, id_b=id_b)
            row = result.single()
            if row:
                print(f"  Camino: {' → '.join(row['camino'])}")
                print(f"  Grado de separación: {row['grado']} saltos")
            else:
                print("  (No hay camino entre estos jugadores — grafo desconectado)")


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

        print(f"\n  Nodos :Jugador:               {stats['jugadores']:>6,}")
        print(f"  Nodos :Juego:                 {stats['juegos']:>6,}")
        print(f"  Relaciones :TIENE_EN_BIBLIOTECA: {stats['rel_biblioteca']:>4,}")
        print(f"  Relaciones :AFINIDAD:            {stats['rel_afinidad']:>4,}")
        print(f"  Relaciones :AMIGO_DE:            {stats['rel_amigos']:>4,}")

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
    print("=" * 60)

    driver = conectar()

    # 1. Limpiar y preparar grafo
    limpiar_grafo(driver)
    crear_constraints_e_indices(driver)

    # 2. Cargar datos
    juegos = obtener_juegos_desde_mongodb(limit=N_JUEGOS)
    jugadores = generar_jugadores(juegos)

    print(f"\n→ Cargando {len(juegos)} juegos y {len(jugadores)} jugadores...")
    cargar_juegos(driver, juegos)
    cargar_jugadores(driver, jugadores)

    # 3. Inferir relaciones de afinidad (núcleo del módulo)
    print("→ Infiriendo relaciones de afinidad...")
    inferir_afinidad(driver, jugadores)
    crear_amistades_explicitas(driver, jugadores)

    # 4. CRUD
    demo_crud(driver)

    # 5. Traversals avanzados
    demo_queries_avanzadas(driver)

    # 6. Estadísticas
    mostrar_estadisticas(driver)

    print(f"\n{'─'*60}")
    print("  ✓ Módulo Neo4j completo.")
    print(f"{'─'*60}\n")

    driver.close()


if __name__ == "__main__":
    main()
