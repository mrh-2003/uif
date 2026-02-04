from database import get_db
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

def analisis_principales_ordenantes(caso_id, top_n=10):
    with get_db() as db:
        query = """
            SELECT 
                p.persona_id,
                p.documento_encriptado,
                p.descripcion_ocupacion,
                COUNT(DISTINCT t.transaccion_id) as total_operaciones,
                SUM(t.monto) as monto_total,
                AVG(t.monto) as monto_promedio,
                MIN(t.fecha_operacion) as primera_operacion,
                MAX(t.fecha_operacion) as ultima_operacion,
                COUNT(DISTINCT t.beneficiario_id) as beneficiarios_unicos
            FROM transacciones t
            JOIN personas p ON t.ordenante_id = p.persona_id
            WHERE t.ordenante_id IN (
                SELECT persona_id FROM casos_personas WHERE caso_id = :caso_id
            )
            GROUP BY p.persona_id
            ORDER BY monto_total DESC
            LIMIT :top_n
        """
        return db.execute(query, {'caso_id': caso_id, 'top_n': top_n}).fetchall()

def analisis_principales_beneficiarios(caso_id, top_n=10):
    with get_db() as db:
        query = """
            SELECT 
                p.persona_id,
                p.documento_encriptado,
                p.descripcion_ocupacion,
                COUNT(DISTINCT t.transaccion_id) as total_operaciones,
                SUM(t.monto) as monto_total,
                AVG(t.monto) as monto_promedio,
                MIN(t.fecha_operacion) as primera_operacion,
                MAX(t.fecha_operacion) as ultima_operacion,
                COUNT(DISTINCT t.ordenante_id) as ordenantes_unicos
            FROM transacciones t
            JOIN personas p ON t.beneficiario_id = p.persona_id
            WHERE t.beneficiario_id IN (
                SELECT persona_id FROM casos_personas WHERE caso_id = :caso_id
            )
            GROUP BY p.persona_id
            ORDER BY monto_total DESC
            LIMIT :top_n
        """
        return db.execute(query, {'caso_id': caso_id, 'top_n': top_n}).fetchall()

def detectar_concentracion_montos(caso_id, umbral_porcentaje=70):
    with get_db() as db:
        query = """
            WITH totales AS (
                SELECT SUM(monto) as monto_total_caso
                FROM transacciones t
                JOIN casos_personas cp ON t.ordenante_id = cp.persona_id 
                    OR t.beneficiario_id = cp.persona_id
                WHERE cp.caso_id = :caso_id
            ),
            ordenantes_ranking AS (
                SELECT 
                    p.persona_id,
                    p.documento_encriptado,
                    SUM(t.monto) as monto_persona,
                    COUNT(*) as num_operaciones,
                    SUM(SUM(t.monto)) OVER (ORDER BY SUM(t.monto) DESC) as monto_acumulado,
                    (SELECT monto_total_caso FROM totales) as monto_total
                FROM transacciones t
                JOIN personas p ON t.ordenante_id = p.persona_id
                JOIN casos_personas cp ON p.persona_id = cp.persona_id
                WHERE cp.caso_id = :caso_id
                GROUP BY p.persona_id
            )
            SELECT 
                persona_id,
                documento_encriptado,
                monto_persona,
                num_operaciones,
                ROUND((monto_persona / monto_total * 100)::numeric, 2) as porcentaje_del_total,
                ROUND((monto_acumulado / monto_total * 100)::numeric, 2) as porcentaje_acumulado
            FROM ordenantes_ranking
            WHERE (monto_acumulado / monto_total * 100) <= :umbral
            ORDER BY monto_persona DESC
        """
        return db.execute(query, {'caso_id': caso_id, 'umbral': umbral_porcentaje}).fetchall()

def detectar_frecuencia_inusual(caso_id, ventana_dias=7, factor_incremento=3):
    with get_db() as db:
        query = """
            WITH operaciones_por_periodo AS (
                SELECT 
                    DATE_TRUNC('week', t.fecha_operacion) as periodo,
                    t.ordenante_id,
                    COUNT(*) as num_operaciones,
                    SUM(t.monto) as monto_total
                FROM transacciones t
                JOIN casos_personas cp ON t.ordenante_id = cp.persona_id
                WHERE cp.caso_id = :caso_id
                GROUP BY DATE_TRUNC('week', t.fecha_operacion), t.ordenante_id
            ),
            promedios AS (
                SELECT 
                    ordenante_id,
                    AVG(num_operaciones) as promedio_operaciones,
                    STDDEV(num_operaciones) as stddev_operaciones
                FROM operaciones_por_periodo
                GROUP BY ordenante_id
                HAVING COUNT(*) > 2
            )
            SELECT 
                op.periodo,
                p.persona_id,
                p.documento_encriptado,
                op.num_operaciones,
                op.monto_total,
                ROUND(pr.promedio_operaciones::numeric, 2) as promedio_historico,
                ROUND((op.num_operaciones / NULLIF(pr.promedio_operaciones, 0))::numeric, 2) as factor_incremento
            FROM operaciones_por_periodo op
            JOIN promedios pr ON op.ordenante_id = pr.ordenante_id
            JOIN personas p ON op.ordenante_id = p.persona_id
            WHERE op.num_operaciones > pr.promedio_operaciones * :factor
            ORDER BY factor_incremento DESC
        """
        return db.execute(query, {'caso_id': caso_id, 'factor': factor_incremento}).fetchall()

def detectar_ventanas_cortas(caso_id, ventana_horas=2, min_operaciones=5):
    with get_db() as db:
        query = """
            WITH transacciones_con_timestamp AS (
                SELECT 
                    t.transaccion_id,
                    t.ordenante_id,
                    t.beneficiario_id,
                    t.fecha_operacion,
                    t.hora_operacion,
                    t.monto,
                    (t.fecha_operacion::timestamp + 
                     COALESCE(t.hora_operacion::time, '00:00:00'::time)) as timestamp_completo
                FROM transacciones t
                JOIN casos_personas cp ON t.ordenante_id = cp.persona_id
                WHERE cp.caso_id = :caso_id
            ),
            ventanas AS (
                SELECT 
                    t1.ordenante_id,
                    t1.timestamp_completo as inicio_ventana,
                    COUNT(DISTINCT t2.transaccion_id) as operaciones_en_ventana,
                    SUM(t2.monto) as monto_total_ventana,
                    ARRAY_AGG(DISTINCT t2.beneficiario_id) as beneficiarios,
                    ARRAY_AGG(t2.transaccion_id ORDER BY t2.timestamp_completo) as transacciones_ids
                FROM transacciones_con_timestamp t1
                JOIN transacciones_con_timestamp t2 
                    ON t1.ordenante_id = t2.ordenante_id
                    AND t2.timestamp_completo >= t1.timestamp_completo
                    AND t2.timestamp_completo <= t1.timestamp_completo + INTERVAL ':ventana hours'
                GROUP BY t1.ordenante_id, t1.timestamp_completo
                HAVING COUNT(DISTINCT t2.transaccion_id) >= :min_ops
            )
            SELECT DISTINCT
                p.persona_id,
                p.documento_encriptado,
                v.inicio_ventana,
                v.operaciones_en_ventana,
                v.monto_total_ventana,
                ARRAY_LENGTH(v.beneficiarios, 1) as beneficiarios_distintos,
                v.transacciones_ids
            FROM ventanas v
            JOIN personas p ON v.ordenante_id = p.persona_id
            ORDER BY v.operaciones_en_ventana DESC, v.monto_total_ventana DESC
        """
        return db.execute(query, {
            'caso_id': caso_id,
            'ventana': ventana_horas,
            'min_ops': min_operaciones
        }).fetchall()

def detectar_montos_similares(caso_id, tolerancia_porcentual=5, min_repeticiones=3):
    with get_db() as db:
        query = """
            WITH montos_agrupados AS (
                SELECT 
                    t.ordenante_id,
                    t.beneficiario_id,
                    ROUND(t.monto::numeric, -2) as monto_redondeado,
                    COUNT(*) as repeticiones,
                    AVG(t.monto) as monto_promedio,
                    STDDEV(t.monto) as desviacion,
                    ARRAY_AGG(t.transaccion_id ORDER BY t.fecha_operacion) as transacciones_ids,
                    MIN(t.fecha_operacion) as primera_fecha,
                    MAX(t.fecha_operacion) as ultima_fecha
                FROM transacciones t
                JOIN casos_personas cp ON t.ordenante_id = cp.persona_id
                WHERE cp.caso_id = :caso_id
                GROUP BY t.ordenante_id, t.beneficiario_id, ROUND(t.monto::numeric, -2)
                HAVING COUNT(*) >= :min_rep
            )
            SELECT 
                po.persona_id as ordenante_id,
                po.documento_encriptado as ordenante_doc,
                pb.persona_id as beneficiario_id,
                pb.documento_encriptado as beneficiario_doc,
                m.monto_promedio,
                m.repeticiones,
                m.desviacion,
                m.primera_fecha,
                m.ultima_fecha,
                m.transacciones_ids
            FROM montos_agrupados m
            JOIN personas po ON m.ordenante_id = po.persona_id
            JOIN personas pb ON m.beneficiario_id = pb.persona_id
            WHERE (m.desviacion / NULLIF(m.monto_promedio, 0) * 100) <= :tolerancia
            ORDER BY m.repeticiones DESC, m.monto_promedio DESC
        """
        return db.execute(query, {
            'caso_id': caso_id,
            'min_rep': min_repeticiones,
            'tolerancia': tolerancia_porcentual
        }).fetchall()

def detectar_pitufeo(caso_id, umbral_monto=10000, ventana_dias=30, min_operaciones=5):
    with get_db() as db:
        query = """
            WITH operaciones_bajo_umbral AS (
                SELECT 
                    t.ordenante_id,
                    t.beneficiario_id,
                    DATE_TRUNC('day', t.fecha_operacion) as fecha,
                    COUNT(*) as num_operaciones,
                    SUM(t.monto) as monto_total_dia,
                    AVG(t.monto) as monto_promedio,
                    ARRAY_AGG(t.transaccion_id ORDER BY t.fecha_operacion, t.hora_operacion) as transacciones_ids
                FROM transacciones t
                JOIN casos_personas cp ON t.ordenante_id = cp.persona_id
                WHERE cp.caso_id = :caso_id
                    AND t.monto < :umbral
                GROUP BY t.ordenante_id, t.beneficiario_id, DATE_TRUNC('day', t.fecha_operacion)
            ),
            ventanas_sospechosas AS (
                SELECT 
                    o1.ordenante_id,
                    o1.beneficiario_id,
                    MIN(o1.fecha) as fecha_inicio,
                    MAX(o1.fecha) as fecha_fin,
                    SUM(o1.num_operaciones) as total_operaciones,
                    SUM(o1.monto_total_dia) as monto_acumulado,
                    ARRAY_AGG(o1.transacciones_ids) as todas_transacciones
                FROM operaciones_bajo_umbral o1
                JOIN operaciones_bajo_umbral o2 
                    ON o1.ordenante_id = o2.ordenante_id
                    AND o1.beneficiario_id = o2.beneficiario_id
                    AND o2.fecha >= o1.fecha
                    AND o2.fecha <= o1.fecha + INTERVAL ':ventana days'
                GROUP BY o1.ordenante_id, o1.beneficiario_id, o1.fecha
                HAVING SUM(o1.num_operaciones) >= :min_ops
            )
            SELECT DISTINCT
                po.persona_id as ordenante_id,
                po.documento_encriptado as ordenante_doc,
                pb.persona_id as beneficiario_id,
                pb.documento_encriptado as beneficiario_doc,
                v.fecha_inicio,
                v.fecha_fin,
                v.total_operaciones,
                v.monto_acumulado,
                v.todas_transacciones
            FROM ventanas_sospechosas v
            JOIN personas po ON v.ordenante_id = po.persona_id
            JOIN personas pb ON v.beneficiario_id = pb.persona_id
            ORDER BY v.monto_acumulado DESC, v.total_operaciones DESC
        """
        return db.execute(query, {
            'caso_id': caso_id,
            'umbral': umbral_monto,
            'ventana': ventana_dias,
            'min_ops': min_operaciones
        }).fetchall()

def detectar_cadenas_transferencia(caso_id, min_eslabones=3, ventana_dias=7):
    transacciones = obtener_transacciones_para_cadenas(caso_id, ventana_dias)
    
    grafo = defaultdict(list)
    for trx in transacciones:
        grafo[trx['ordenante_id']].append({
            'beneficiario_id': trx['beneficiario_id'],
            'transaccion_id': trx['transaccion_id'],
            'fecha': trx['fecha_operacion'],
            'monto': trx['monto']
        })
    
    cadenas_detectadas = []
    
    def buscar_cadenas(nodo_actual, camino, visitados, profundidad):
        if profundidad >= min_eslabones:
            cadenas_detectadas.append(camino.copy())
        
        if profundidad > 10:
            return
        
        for siguiente in grafo.get(nodo_actual, []):
            if siguiente['beneficiario_id'] not in visitados:
                nuevo_visitados = visitados.copy()
                nuevo_visitados.add(siguiente['beneficiario_id'])
                nuevo_camino = camino + [siguiente]
                buscar_cadenas(siguiente['beneficiario_id'], nuevo_camino, nuevo_visitados, profundidad + 1)
    
    for nodo_inicial in grafo.keys():
        buscar_cadenas(nodo_inicial, [], {nodo_inicial}, 0)
    
    return cadenas_detectadas

def obtener_transacciones_para_cadenas(caso_id, ventana_dias):
    with get_db() as db:
        query = """
            SELECT 
                t.transaccion_id,
                t.ordenante_id,
                t.beneficiario_id,
                t.fecha_operacion,
                t.monto
            FROM transacciones t
            JOIN casos_personas cp ON t.ordenante_id = cp.persona_id 
                OR t.beneficiario_id = cp.persona_id
            WHERE cp.caso_id = :caso_id
                AND t.fecha_operacion >= CURRENT_DATE - INTERVAL ':ventana days'
            ORDER BY t.fecha_operacion, t.hora_operacion
        """
        result = db.execute(query, {'caso_id': caso_id, 'ventana': ventana_dias}).fetchall()
        return [dict(row) for row in result]

def detectar_circularidad(caso_id, max_saltos=5):
    transacciones = obtener_transacciones_para_cadenas(caso_id, 90)
    
    grafo = defaultdict(list)
    for trx in transacciones:
        grafo[trx['ordenante_id']].append({
            'beneficiario_id': trx['beneficiario_id'],
            'transaccion_id': trx['transaccion_id'],
            'monto': trx['monto']
        })
    
    ciclos_detectados = []
    
    def buscar_ciclos(nodo_actual, nodo_origen, camino, profundidad):
        if profundidad > max_saltos:
            return
        
        for siguiente in grafo.get(nodo_actual, []):
            if siguiente['beneficiario_id'] == nodo_origen and profundidad >= 2:
                ciclos_detectados.append({
                    'origen': nodo_origen,
                    'camino': camino + [siguiente],
                    'longitud': profundidad + 1
                })
            elif siguiente['beneficiario_id'] not in [n['beneficiario_id'] for n in camino]:
                buscar_ciclos(
                    siguiente['beneficiario_id'],
                    nodo_origen,
                    camino + [siguiente],
                    profundidad + 1
                )
    
    for nodo in grafo.keys():
        buscar_ciclos(nodo, nodo, [], 0)
    
    return ciclos_detectados

def generar_resumen_analisis(caso_id):
    return {
        'principales_ordenantes': analisis_principales_ordenantes(caso_id, 10),
        'principales_beneficiarios': analisis_principales_beneficiarios(caso_id, 10),
        'concentracion_montos': detectar_concentracion_montos(caso_id),
        'frecuencia_inusual': detectar_frecuencia_inusual(caso_id),
        'ventanas_cortas': detectar_ventanas_cortas(caso_id),
        'montos_similares': detectar_montos_similares(caso_id),
        'pitufeo': detectar_pitufeo(caso_id),
        'cadenas': detectar_cadenas_transferencia(caso_id),
        'circularidad': detectar_circularidad(caso_id)
    }
