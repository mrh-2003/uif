from database import get_db
import json
from analisis import (
    detectar_pitufeo, detectar_concentracion_montos,
    detectar_montos_similares, detectar_ventanas_cortas,
    detectar_cadenas_transferencia, detectar_circularidad,
    detectar_frecuencia_inusual
)

def ejecutar_deteccion_tipologias(caso_id):
    tipologias = obtener_tipologias_activas()
    resultados = []
    
    for tipologia in tipologias:
        codigo = tipologia['codigo']
        params = json.loads(tipologia['parametros']) if tipologia['parametros'] else {}
        
        detecciones = None
        
        if codigo == 'TIP001':
            detecciones = detectar_pitufeo(
                caso_id,
                params.get('umbral_monto', 10000),
                params.get('ventana_dias', 30),
                params.get('min_operaciones', 5)
            )
        elif codigo == 'TIP002':
            detecciones = detectar_concentracion_beneficiarios(caso_id, params)
        elif codigo == 'TIP003':
            detecciones = detectar_concentracion_ordenantes(caso_id, params)
        elif codigo == 'TIP004':
            detecciones = detectar_circularidad(
                caso_id,
                params.get('max_saltos', 5)
            )
        elif codigo == 'TIP005':
            detecciones = detectar_montos_similares(
                caso_id,
                params.get('tolerancia_porcentual', 5),
                params.get('min_repeticiones', 3)
            )
        elif codigo == 'TIP006':
            detecciones = detectar_ventanas_cortas(
                caso_id,
                params.get('ventana_horas', 2),
                params.get('min_operaciones', 5)
            )
        elif codigo == 'TIP007':
            detecciones = detectar_cadenas_transferencia(
                caso_id,
                params.get('min_eslabones', 3),
                params.get('ventana_dias', 7)
            )
        elif codigo == 'TIP008':
            detecciones = detectar_transferencias_inmediatas(caso_id, params)
        elif codigo == 'TIP009':
            detecciones = detectar_frecuencia_inusual(
                caso_id,
                params.get('ventana_dias', 7),
                params.get('factor_incremento', 3)
            )
        elif codigo == 'TIP010':
            detecciones = detectar_montos_redondos(caso_id, params)
        
        if detecciones:
            resultados.extend(
                procesar_detecciones(caso_id, tipologia, detecciones)
            )
    
    return resultados

def obtener_tipologias_activas():
    with get_db() as db:
        query = "SELECT * FROM catalogos_tipologias WHERE activo = TRUE ORDER BY nivel_riesgo DESC"
        return [dict(row) for row in db.execute(query).fetchall()]

def procesar_detecciones(caso_id, tipologia, detecciones):
    resultados = []
    
    with get_db() as db:
        for deteccion in detecciones:
            persona_id = extraer_persona_id(deteccion)
            evidencias = construir_evidencias(deteccion)
            transacciones_ids = extraer_transacciones_ids(deteccion)
            nivel_confianza = calcular_nivel_confianza(tipologia, deteccion)
            
            query = """
                INSERT INTO tipologias_detectadas 
                (caso_id, tipologia_id, persona_id, nivel_confianza, evidencias, transacciones_relacionadas)
                VALUES (:caso_id, :tipologia_id, :persona_id, :confianza, :evidencias, :transacciones)
                RETURNING deteccion_id
            """
            
            result = db.execute(query, {
                'caso_id': caso_id,
                'tipologia_id': tipologia['tipologia_id'],
                'persona_id': persona_id,
                'confianza': nivel_confianza,
                'evidencias': json.dumps(evidencias),
                'transacciones': transacciones_ids
            })
            
            resultados.append({
                'deteccion_id': result.fetchone()[0],
                'tipologia': tipologia['nombre'],
                'persona_id': persona_id,
                'nivel_confianza': nivel_confianza
            })
    
    return resultados

def extraer_persona_id(deteccion):
    if isinstance(deteccion, dict):
        return deteccion.get('persona_id') or deteccion.get('ordenante_id')
    elif hasattr(deteccion, 'persona_id'):
        return deteccion.persona_id
    elif hasattr(deteccion, 'ordenante_id'):
        return deteccion.ordenante_id
    return None

def extraer_transacciones_ids(deteccion):
    if isinstance(deteccion, dict):
        ids = deteccion.get('transacciones_ids') or deteccion.get('todas_transacciones', [])
    elif hasattr(deteccion, 'transacciones_ids'):
        ids = deteccion.transacciones_ids
    else:
        return []
    
    if isinstance(ids, list):
        flat_ids = []
        for item in ids:
            if isinstance(item, list):
                flat_ids.extend(item)
            else:
                flat_ids.append(item)
        return flat_ids
    return []

def construir_evidencias(deteccion):
    if isinstance(deteccion, dict):
        return deteccion
    else:
        return {k: v for k, v in deteccion._asdict().items() if not k.startswith('_')}

def calcular_nivel_confianza(tipologia, deteccion):
    nivel_base = tipologia['nivel_riesgo'] * 10
    
    if isinstance(deteccion, dict):
        num_operaciones = deteccion.get('total_operaciones', 0) or deteccion.get('num_operaciones', 0)
        monto_total = deteccion.get('monto_total', 0) or deteccion.get('monto_acumulado', 0)
    else:
        num_operaciones = getattr(deteccion, 'total_operaciones', 0) or getattr(deteccion, 'num_operaciones', 0)
        monto_total = getattr(deteccion, 'monto_total', 0) or getattr(deteccion, 'monto_acumulado', 0)
    
    if num_operaciones > 10:
        nivel_base += 10
    if num_operaciones > 50:
        nivel_base += 10
    
    if monto_total > 100000:
        nivel_base += 10
    if monto_total > 1000000:
        nivel_base += 10
    
    return min(nivel_base, 100)

def detectar_concentracion_beneficiarios(caso_id, params):
    with get_db() as db:
        query = """
            SELECT 
                t.beneficiario_id as persona_id,
                pb.documento_encriptado,
                COUNT(DISTINCT t.ordenante_id) as num_ordenantes,
                COUNT(DISTINCT t.transaccion_id) as total_operaciones,
                SUM(t.monto) as monto_total,
                ARRAY_AGG(DISTINCT t.transaccion_id) as transacciones_ids
            FROM transacciones t
            JOIN personas pb ON t.beneficiario_id = pb.persona_id
            JOIN casos_personas cp ON t.beneficiario_id = cp.persona_id
            WHERE cp.caso_id = :caso_id
                AND t.fecha_operacion >= CURRENT_DATE - INTERVAL ':ventana days'
            GROUP BY t.beneficiario_id, pb.documento_encriptado
            HAVING COUNT(DISTINCT t.ordenante_id) >= :min_ordenantes
            ORDER BY monto_total DESC
        """
        return [dict(row) for row in db.execute(query, {
            'caso_id': caso_id,
            'ventana': params.get('ventana_dias', 30),
            'min_ordenantes': params.get('min_ordenantes', 5)
        }).fetchall()]

def detectar_concentracion_ordenantes(caso_id, params):
    with get_db() as db:
        query = """
            SELECT 
                t.ordenante_id as persona_id,
                po.documento_encriptado,
                COUNT(DISTINCT t.beneficiario_id) as num_beneficiarios,
                COUNT(DISTINCT t.transaccion_id) as total_operaciones,
                SUM(t.monto) as monto_total,
                ARRAY_AGG(DISTINCT t.transaccion_id) as transacciones_ids
            FROM transacciones t
            JOIN personas po ON t.ordenante_id = po.persona_id
            JOIN casos_personas cp ON t.ordenante_id = cp.persona_id
            WHERE cp.caso_id = :caso_id
                AND t.fecha_operacion >= CURRENT_DATE - INTERVAL ':ventana days'
            GROUP BY t.ordenante_id, po.documento_encriptado
            HAVING COUNT(DISTINCT t.beneficiario_id) >= :min_beneficiarios
            ORDER BY monto_total DESC
        """
        return [dict(row) for row in db.execute(query, {
            'caso_id': caso_id,
            'ventana': params.get('ventana_dias', 30),
            'min_beneficiarios': params.get('min_beneficiarios', 10)
        }).fetchall()]

def detectar_transferencias_inmediatas(caso_id, params):
    with get_db() as db:
        query = """
            WITH transacciones_ordenadas AS (
                SELECT 
                    t1.transaccion_id as trx_recibida_id,
                    t1.beneficiario_id as intermediario_id,
                    t1.fecha_operacion as fecha_recepcion,
                    t1.hora_operacion as hora_recepcion,
                    t1.monto as monto_recibido,
                    t2.transaccion_id as trx_enviada_id,
                    t2.beneficiario_id as beneficiario_final_id,
                    t2.fecha_operacion as fecha_envio,
                    t2.hora_operacion as hora_envio,
                    t2.monto as monto_enviado,
                    EXTRACT(EPOCH FROM (
                        (t2.fecha_operacion::timestamp + COALESCE(t2.hora_operacion::time, '00:00:00'::time)) -
                        (t1.fecha_operacion::timestamp + COALESCE(t1.hora_operacion::time, '00:00:00'::time))
                    )) / 60 as minutos_diferencia
                FROM transacciones t1
                JOIN transacciones t2 ON t1.beneficiario_id = t2.ordenante_id
                JOIN casos_personas cp ON t1.beneficiario_id = cp.persona_id
                WHERE cp.caso_id = :caso_id
            )
            SELECT 
                intermediario_id as persona_id,
                p.documento_encriptado,
                COUNT(*) as num_operaciones,
                SUM(monto_enviado) as monto_total,
                AVG(minutos_diferencia) as promedio_minutos,
                ARRAY_AGG(trx_recibida_id) || ARRAY_AGG(trx_enviada_id) as transacciones_ids
            FROM transacciones_ordenadas to_
            JOIN personas p ON to_.intermediario_id = p.persona_id
            WHERE minutos_diferencia <= :ventana_minutos
                AND ABS(monto_recibido - monto_enviado) / monto_recibido < 0.1
            GROUP BY intermediario_id, p.documento_encriptado
            HAVING COUNT(*) >= 3
            ORDER BY num_operaciones DESC
        """
        return [dict(row) for row in db.execute(query, {
            'caso_id': caso_id,
            'ventana_minutos': params.get('ventana_minutos', 30)
        }).fetchall()]

def detectar_montos_redondos(caso_id, params):
    with get_db() as db:
        query = """
            SELECT 
                t.ordenante_id as persona_id,
                p.documento_encriptado,
                COUNT(*) as num_operaciones,
                SUM(t.monto) as monto_total,
                ARRAY_AGG(t.transaccion_id) as transacciones_ids
            FROM transacciones t
            JOIN personas p ON t.ordenante_id = p.persona_id
            JOIN casos_personas cp ON t.ordenante_id = cp.persona_id
            WHERE cp.caso_id = :caso_id
                AND t.monto = ROUND(t.monto, -3)
            GROUP BY t.ordenante_id, p.documento_encriptado
            HAVING COUNT(*) >= :min_operaciones
            ORDER BY num_operaciones DESC
        """
        return [dict(row) for row in db.execute(query, {
            'caso_id': caso_id,
            'min_operaciones': params.get('min_operaciones', 5)
        }).fetchall()]

def obtener_tipologias_por_caso(caso_id):
    with get_db() as db:
        query = """
            SELECT 
                td.deteccion_id,
                ct.codigo,
                ct.nombre,
                ct.descripcion,
                ct.categoria,
                ct.nivel_riesgo,
                td.persona_id,
                p.documento_encriptado,
                td.nivel_confianza,
                td.evidencias,
                td.transacciones_relacionadas,
                td.fecha_deteccion,
                td.estado
            FROM tipologias_detectadas td
            JOIN catalogos_tipologias ct ON td.tipologia_id = ct.tipologia_id
            LEFT JOIN personas p ON td.persona_id = p.persona_id
            WHERE td.caso_id = :caso_id
            ORDER BY ct.nivel_riesgo DESC, td.nivel_confianza DESC
        """
        return [dict(row) for row in db.execute(query, {'caso_id': caso_id}).fetchall()]

def actualizar_estado_tipologia(deteccion_id, nuevo_estado, observaciones=''):
    with get_db() as db:
        query = """
            UPDATE tipologias_detectadas 
            SET estado = :estado, observaciones = :obs
            WHERE deteccion_id = :deteccion_id
        """
        db.execute(query, {
            'estado': nuevo_estado,
            'obs': observaciones,
            'deteccion_id': deteccion_id
        })
