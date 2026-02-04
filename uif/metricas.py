from database import get_db
from datetime import datetime, timedelta

def calcular_metricas_persona(persona_id):
    with get_db() as db:
        query_ordenante = """
            SELECT 
                COUNT(*) as num_operaciones,
                SUM(monto) as monto_total,
                AVG(monto) as monto_promedio,
                MIN(monto) as monto_minimo,
                MAX(monto) as monto_maximo,
                MIN(fecha_operacion) as primera_fecha,
                MAX(fecha_operacion) as ultima_fecha,
                COUNT(DISTINCT beneficiario_id) as beneficiarios_unicos,
                COUNT(DISTINCT DATE_TRUNC('day', fecha_operacion)) as dias_activos
            FROM transacciones
            WHERE ordenante_id = :persona_id
        """
        
        query_beneficiario = """
            SELECT 
                COUNT(*) as num_operaciones,
                SUM(monto) as monto_total,
                AVG(monto) as monto_promedio,
                COUNT(DISTINCT ordenante_id) as ordenantes_unicos
            FROM transacciones
            WHERE beneficiario_id = :persona_id
        """
        
        metricas_ord = db.execute(query_ordenante, {'persona_id': persona_id}).fetchone()
        metricas_ben = db.execute(query_beneficiario, {'persona_id': persona_id}).fetchone()
        
        return {
            'como_ordenante': dict(metricas_ord) if metricas_ord else {},
            'como_beneficiario': dict(metricas_ben) if metricas_ben else {}
        }

def calcular_velocidad_transaccional(persona_id, ventana_dias=30):
    with get_db() as db:
        query = """
            WITH periodos AS (
                SELECT 
                    DATE_TRUNC('week', fecha_operacion) as periodo,
                    COUNT(*) as num_operaciones,
                    SUM(monto) as monto_total
                FROM transacciones
                WHERE ordenante_id = :persona_id
                    AND fecha_operacion >= CURRENT_DATE - INTERVAL ':ventana days'
                GROUP BY DATE_TRUNC('week', fecha_operacion)
            )
            SELECT 
                periodo,
                num_operaciones,
                monto_total,
                LAG(num_operaciones) OVER (ORDER BY periodo) as operaciones_periodo_anterior,
                LAG(monto_total) OVER (ORDER BY periodo) as monto_periodo_anterior
            FROM periodos
            ORDER BY periodo DESC
        """
        
        return db.execute(query, {'persona_id': persona_id, 'ventana': ventana_dias}).fetchall()

def calcular_diversificacion(persona_id):
    with get_db() as db:
        query = """
            SELECT 
                COUNT(DISTINCT beneficiario_id) as num_beneficiarios,
                COUNT(DISTINCT cuenta_beneficiario) as num_cuentas_destino,
                COUNT(DISTINCT tipo_operacion_sbs) as num_tipos_operacion,
                COUNT(DISTINCT canal) as num_canales
            FROM transacciones
            WHERE ordenante_id = :persona_id
        """
        
        return dict(db.execute(query, {'persona_id': persona_id}).fetchone())

def calcular_patron_temporal(persona_id):
    with get_db() as db:
        query = """
            SELECT 
                EXTRACT(DOW FROM fecha_operacion) as dia_semana,
                EXTRACT(HOUR FROM hora_operacion::time) as hora,
                COUNT(*) as num_operaciones,
                SUM(monto) as monto_total
            FROM transacciones
            WHERE ordenante_id = :persona_id
                AND hora_operacion IS NOT NULL
            GROUP BY EXTRACT(DOW FROM fecha_operacion), EXTRACT(HOUR FROM hora_operacion::time)
            ORDER BY num_operaciones DESC
        """
        
        return db.execute(query, {'persona_id': persona_id}).fetchall()

def calcular_concentracion_geografica(persona_id):
    with get_db() as db:
        query = """
            SELECT 
                dep_beneficiario,
                prov_beneficiario,
                COUNT(*) as num_operaciones,
                SUM(monto) as monto_total
            FROM transacciones
            WHERE ordenante_id = :persona_id
                AND dep_beneficiario IS NOT NULL
            GROUP BY dep_beneficiario, prov_beneficiario
            ORDER BY monto_total DESC
            LIMIT 10
        """
        
        return db.execute(query, {'persona_id': persona_id}).fetchall()

def identificar_relaciones_recurrentes(persona_id, min_operaciones=3):
    with get_db() as db:
        query = """
            SELECT 
                pb.persona_id,
                pb.documento_encriptado,
                pb.descripcion_ocupacion,
                COUNT(*) as num_operaciones,
                SUM(t.monto) as monto_total,
                AVG(t.monto) as monto_promedio,
                MIN(t.fecha_operacion) as primera_operacion,
                MAX(t.fecha_operacion) as ultima_operacion
            FROM transacciones t
            JOIN personas pb ON t.beneficiario_id = pb.persona_id
            WHERE t.ordenante_id = :persona_id
            GROUP BY pb.persona_id
            HAVING COUNT(*) >= :min_ops
            ORDER BY num_operaciones DESC, monto_total DESC
        """
        
        return db.execute(query, {'persona_id': persona_id, 'min_ops': min_operaciones}).fetchall()

def calcular_indice_sospecha(persona_id):
    metricas = calcular_metricas_persona(persona_id)
    diversificacion = calcular_diversificacion(persona_id)
    
    puntuacion = 0
    
    if metricas['como_ordenante']:
        monto_total = float(metricas['como_ordenante'].get('monto_total', 0))
        num_ops = int(metricas['como_ordenante'].get('num_operaciones', 0))
        
        if monto_total > 1000000:
            puntuacion += 30
        elif monto_total > 500000:
            puntuacion += 20
        elif monto_total > 100000:
            puntuacion += 10
        
        if num_ops > 100:
            puntuacion += 20
        elif num_ops > 50:
            puntuacion += 10
        
        if diversificacion['num_beneficiarios'] > 50:
            puntuacion += 20
        elif diversificacion['num_beneficiarios'] > 20:
            puntuacion += 10
    
    if metricas['como_beneficiario']:
        num_ordenantes = int(metricas['como_beneficiario'].get('ordenantes_unicos', 0))
        
        if num_ordenantes > 20:
            puntuacion += 20
        elif num_ordenantes > 10:
            puntuacion += 10
    
    return min(puntuacion, 100)

def generar_perfil_completo(persona_id):
    return {
        'metricas': calcular_metricas_persona(persona_id),
        'diversificacion': calcular_diversificacion(persona_id),
        'patron_temporal': calcular_patron_temporal(persona_id),
        'concentracion_geografica': calcular_concentracion_geografica(persona_id),
        'relaciones_recurrentes': identificar_relaciones_recurrentes(persona_id),
        'indice_sospecha': calcular_indice_sospecha(persona_id)
    }

def comparar_periodos(persona_id, periodo1_inicio, periodo1_fin, periodo2_inicio, periodo2_fin):
    with get_db() as db:
        query = """
            SELECT 
                CASE 
                    WHEN fecha_operacion BETWEEN :p1_inicio AND :p1_fin THEN 'Periodo 1'
                    WHEN fecha_operacion BETWEEN :p2_inicio AND :p2_fin THEN 'Periodo 2'
                END as periodo,
                COUNT(*) as num_operaciones,
                SUM(monto) as monto_total,
                AVG(monto) as monto_promedio,
                COUNT(DISTINCT beneficiario_id) as beneficiarios_unicos
            FROM transacciones
            WHERE ordenante_id = :persona_id
                AND (
                    fecha_operacion BETWEEN :p1_inicio AND :p1_fin
                    OR fecha_operacion BETWEEN :p2_inicio AND :p2_fin
                )
            GROUP BY periodo
        """
        
        return db.execute(query, {
            'persona_id': persona_id,
            'p1_inicio': periodo1_inicio,
            'p1_fin': periodo1_fin,
            'p2_inicio': periodo2_inicio,
            'p2_fin': periodo2_fin
        }).fetchall()
