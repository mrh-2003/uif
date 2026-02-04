from database import get_db
from datetime import datetime
from sqlalchemy import text

def crear_caso(nombre, descripcion='', usuario='SYSTEM', prioridad='MEDIA', tipo_caso='INVESTIGACION'):
    with get_db() as db:
        query = text("""
            INSERT INTO casos (nombre_caso, descripcion, usuario_creador, prioridad, tipo_caso)
            VALUES (:nombre, :desc, :usuario, :prioridad, :tipo)
            RETURNING caso_id
        """)
        result = db.execute(query, {
            'nombre': nombre,
            'desc': descripcion,
            'usuario': usuario,
            'prioridad': prioridad,
            'tipo': tipo_caso
        })
        return result.fetchone()[0]

def listar_casos():
    with get_db() as db:
        query = text("""
            SELECT c.caso_id, c.nombre_caso, c.descripcion, c.estado, 
                   c.fecha_creacion, c.prioridad, c.tipo_caso,
                   COUNT(DISTINCT cp.persona_id) as total_personas
            FROM casos c
            LEFT JOIN casos_personas cp ON c.caso_id = cp.caso_id
            GROUP BY c.caso_id
            ORDER BY c.fecha_creacion DESC
        """)
        # CORRECCIÓN: Retornar lista de diccionarios
        return [dict(row._mapping) for row in db.execute(query).fetchall()]

def obtener_caso(caso_id):
    with get_db() as db:
        query = text("SELECT * FROM casos WHERE caso_id = :caso_id")
        result = db.execute(query, {'caso_id': caso_id}).fetchone()
        # CORRECCIÓN: Retornar diccionario
        if result:
            return dict(result._mapping)
        return None

def agregar_persona_a_caso(caso_id, persona_id, rol='INVESTIGADO', motivo=''):
    with get_db() as db:
        query = text("""
            INSERT INTO casos_personas (caso_id, persona_id, rol_en_caso, motivo_inclusion)
            VALUES (:caso_id, :persona_id, :rol, :motivo)
            ON CONFLICT (caso_id, persona_id) DO NOTHING
            RETURNING caso_persona_id
        """)
        result = db.execute(query, {
            'caso_id': caso_id,
            'persona_id': persona_id,
            'rol': rol,
            'motivo': motivo
        })
        return result.fetchone()

def obtener_personas_caso(caso_id):
    with get_db() as db:
        query = text("""
            SELECT p.persona_id, p.documento_encriptado, p.tipo_persona, 
                   p.descripcion_ocupacion, p.total_operaciones, p.monto_total,
                   cp.rol_en_caso, cp.fecha_inclusion
            FROM casos_personas cp
            JOIN personas p ON cp.persona_id = p.persona_id
            WHERE cp.caso_id = :caso_id
            ORDER BY p.monto_total DESC
        """)
        return [dict(row._mapping) for row in db.execute(query, {'caso_id': caso_id}).fetchall()]

def obtener_transacciones_caso(caso_id):
    with get_db() as db:
        query = text("""
            SELECT DISTINCT t.*
            FROM transacciones t
            JOIN casos_personas cp ON (
                t.ordenante_id = cp.persona_id OR 
                t.beneficiario_id = cp.persona_id OR
                t.ejecutante_id = cp.persona_id
            )
            WHERE cp.caso_id = :caso_id
            ORDER BY t.fecha_operacion DESC, t.hora_operacion DESC
        """)
        return [dict(row._mapping) for row in db.execute(query, {'caso_id': caso_id}).fetchall()]

def buscar_personas(termino_busqueda='', limit=100):
    with get_db() as db:
        query = text("""
            SELECT persona_id, documento_encriptado, tipo_persona, tipo_documento,
                   descripcion_ocupacion, total_operaciones, monto_total,
                   fecha_primera_operacion, fecha_ultima_operacion
            FROM personas
            WHERE 
                (:termino = '' OR 
                 documento_encriptado ILIKE :termino_like OR
                 descripcion_ocupacion ILIKE :termino_like)
            ORDER BY monto_total DESC
            LIMIT :limit
        """)
        return [dict(row._mapping) for row in db.execute(query, {
            'termino': termino_busqueda,
            'termino_like': f'%{termino_busqueda}%',
            'limit': limit
        }).fetchall()]

def actualizar_estado_caso(caso_id, nuevo_estado):
    with get_db() as db:
        query = text("""
            UPDATE casos 
            SET estado = :estado, fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE caso_id = :caso_id
        """)
        db.execute(query, {'estado': nuevo_estado, 'caso_id': caso_id})

def eliminar_persona_de_caso(caso_id, persona_id):
    with get_db() as db:
        query = text("DELETE FROM casos_personas WHERE caso_id = :caso_id AND persona_id = :persona_id")
        db.execute(query, {'caso_id': caso_id, 'persona_id': persona_id})