import pandas as pd
import numpy as np
from datetime import datetime
from database import get_db
import hashlib
from sqlalchemy import text

COLUMNAS_REQUERIDAS = [
    'busqueda', 'flgtipoclibusqueda', 'destipclasifpartyrelacionado',
    'num_registro_interno', 'descanal', 'codigo_ubigeo',
    'fec_operacion', 'hora_operacion',
    'tipo_ejecutante', 'tipo_doc_ejecutante', 'doc_ejecutante_encriptado',
    'CIIUOcupSol', 'DesOcupSOL',
    'tipo_ordenante', 'tipo_doc_ordenante', 'doc_ordenante_encriptado',
    'CIIUOcupOrd', 'DesOcupOrd', 'DepOrd', 'ProvOrd', 'DisOrd',
    'tipo_beneficiario', 'tipo_doc_beneficiario', 'doc_beneficiario_encriptado',
    'CIIUOcupBen', 'DesOcupBen', 'DepBen', 'ProvBen', 'DisBen',
    'tipopereportesbs', 'destipopereportesbs', 'desorigendinero',
    'codmonedadestino', 'nbrmonedadestino', 'mtotrx',
    'codcta20ordenante', 'codcta20beneficiario'
]

MONTO_MINIMO = 100

def normalizar_columnas(df):
    columnas_normalizadas = {}
    for col_req in COLUMNAS_REQUERIDAS:
        col_encontrada = None
        for col_df in df.columns:
            if col_df.lower() == col_req.lower():
                col_encontrada = col_df
                break
        if col_encontrada:
            columnas_normalizadas[col_encontrada] = col_req
    
    return df.rename(columns=columnas_normalizadas)

def validar_columnas(df):
    columnas_faltantes = set(COLUMNAS_REQUERIDAS) - set(df.columns)
    if columnas_faltantes:
        raise ValueError(f"Columnas faltantes: {columnas_faltantes}")
    return True

def limpiar_datos(df):
    df_limpio = df.copy()
    
    # Eliminar filas donde ordenante o beneficiario sean nulos (son obligatorios)
    df_limpio = df_limpio.dropna(subset=[
        'fec_operacion', 'mtotrx',
        'doc_ordenante_encriptado', 'doc_beneficiario_encriptado'
    ])
    
    df_limpio['mtotrx'] = pd.to_numeric(df_limpio['mtotrx'], errors='coerce')
    df_limpio = df_limpio[df_limpio['mtotrx'] >= MONTO_MINIMO]
    
    df_limpio['fec_operacion'] = pd.to_datetime(df_limpio['fec_operacion'], errors='coerce')
    df_limpio = df_limpio.dropna(subset=['fec_operacion'])
    
    # Limpiar espacios en columnas de texto
    for col in df_limpio.select_dtypes(include=['object']).columns:
        df_limpio[col] = df_limpio[col].str.strip()
        
    return df_limpio

def registrar_ro(nombre_archivo, total, validos, descartados, usuario='SYSTEM'):
    with get_db() as db:
        query = text("""
            INSERT INTO registros_operaciones 
            (nombre_archivo, total_registros, registros_validos, registros_descartados, 
             usuario_carga, estado_procesamiento)
            VALUES (:nombre, :total, :validos, :descartados, :usuario, 'PROCESADO')
            RETURNING ro_id
        """)
        result = db.execute(query, {
            'nombre': nombre_archivo,
            'total': total,
            'validos': validos,
            'descartados': descartados,
            'usuario': usuario
        })
        return result.fetchone()[0]

def safe_get(row, col):
    """Obtiene el valor de una columna y devuelve None si es NaN"""
    val = row.get(col)
    if pd.isna(val) or str(val).lower() == 'nan':
        return None
    return val

def procesar_persona(row, tipo_rol):
    tipo_map = {
        'ejecutante': ('tipo_ejecutante', 'tipo_doc_ejecutante', 'doc_ejecutante_encriptado', 
                      'CIIUOcupSol', 'DesOcupSOL', None, None, None),
        'ordenante': ('tipo_ordenante', 'tipo_doc_ordenante', 'doc_ordenante_encriptado',
                     'CIIUOcupOrd', 'DesOcupOrd', 'DepOrd', 'ProvOrd', 'DisOrd'),
        'beneficiario': ('tipo_beneficiario', 'tipo_doc_beneficiario', 'doc_beneficiario_encriptado',
                        'CIIUOcupBen', 'DesOcupBen', 'DepBen', 'ProvBen', 'DisBen')
    }
    
    campos = tipo_map[tipo_rol]
    return {
        'tipo_persona': safe_get(row, campos[0]),
        'tipo_documento': safe_get(row, campos[1]),
        'documento_encriptado': safe_get(row, campos[2]),
        'ciiu_ocupacion': safe_get(row, campos[3]) if campos[3] else None,
        'descripcion_ocupacion': safe_get(row, campos[4]) if campos[4] else None,
        'departamento': safe_get(row, campos[5]) if campos[5] else None,
        'provincia': safe_get(row, campos[6]) if campos[6] else None,
        'distrito': safe_get(row, campos[7]) if campos[7] else None
    }

def insertar_o_actualizar_persona(db, persona_data, fecha_op, monto, rol):
    doc_enc = persona_data['documento_encriptado']
    
    # CORRECCIÓN: Si no hay documento (es None o NaN), no intentamos crear la persona
    if not doc_enc:
        return None
        
    query_select = text("SELECT persona_id FROM personas WHERE documento_encriptado = :doc")
    result = db.execute(query_select, {'doc': doc_enc}).fetchone()
    
    if result:
        persona_id = result[0]
        query_update = text(f"""
            UPDATE personas SET
                fecha_ultima_operacion = GREATEST(fecha_ultima_operacion, :fecha),
                total_operaciones = total_operaciones + 1,
                monto_total = monto_total + :monto,
                monto_promedio = (monto_total + :monto) / (total_operaciones + 1),
                es_{rol} = TRUE
            WHERE persona_id = :persona_id
        """)
        db.execute(query_update, {'fecha': fecha_op, 'monto': monto, 'persona_id': persona_id})
    else:
        query_insert = text("""
            INSERT INTO personas (
                tipo_persona, tipo_documento, documento_encriptado,
                ciiu_ocupacion, descripcion_ocupacion,
                departamento, provincia, distrito,
                fecha_primera_operacion, fecha_ultima_operacion,
                total_operaciones, monto_total, monto_promedio,
                es_ejecutante, es_ordenante, es_beneficiario
            ) VALUES (
                :tipo_persona, :tipo_documento, :documento_encriptado,
                :ciiu, :ocupacion, :dep, :prov, :dist,
                :fecha, :fecha, 1, :monto, :monto,
                :es_ejecutante, :es_ordenante, :es_beneficiario
            ) RETURNING persona_id
        """)
        
        # Preparar parámetros asegurando que no vayan NaNs
        params = {k: (v if v is not None else None) for k, v in persona_data.items()}
        params.update({
            'ciiu': params['ciiu_ocupacion'],
            'ocupacion': params['descripcion_ocupacion'],
            'dep': params['departamento'],
            'prov': params['provincia'],
            'dist': params['distrito'],
            'fecha': fecha_op,
            'monto': monto,
            'es_ejecutante': rol == 'ejecutante',
            'es_ordenante': rol == 'ordenante',
            'es_beneficiario': rol == 'beneficiario'
        })
        
        result = db.execute(query_insert, params)
        persona_id = result.fetchone()[0]
    
    return persona_id

def cargar_transacciones(df, ro_id):
    with get_db() as db:
        query_trx = text("""
            INSERT INTO transacciones (
                ro_id, busqueda, flag_tipo_cli_busqueda, tipo_clasificacion_relacionado,
                num_registro_interno, canal, codigo_ubigeo, fecha_operacion, hora_operacion,
                ejecutante_id, tipo_ejecutante, tipo_doc_ejecutante, doc_ejecutante_encriptado,
                ordenante_id, tipo_ordenante, tipo_doc_ordenante, doc_ordenante_encriptado,
                ciiu_ordenante, ocupacion_ordenante, dep_ordenante, prov_ordenante, dist_ordenante, cuenta_ordenante,
                beneficiario_id, tipo_beneficiario, tipo_doc_beneficiario, doc_beneficiario_encriptado,
                ciiu_beneficiario, ocupacion_beneficiario, dep_beneficiario, prov_beneficiario, dist_beneficiario, cuenta_beneficiario,
                tipo_operacion_sbs, descripcion_operacion_sbs, origen_dinero,
                codigo_moneda, nombre_moneda, monto
            ) VALUES (
                :ro_id, :busqueda, :flag_tipo, :tipo_clasif, :num_reg, :canal, :ubigeo, :fecha, :hora,
                :ej_id, :ej_tipo, :ej_doc_tipo, :ej_doc,
                :ord_id, :ord_tipo, :ord_doc_tipo, :ord_doc, :ord_ciiu, :ord_ocup, :ord_dep, :ord_prov, :ord_dist, :ord_cta,
                :ben_id, :ben_tipo, :ben_doc_tipo, :ben_doc, :ben_ciiu, :ben_ocup, :ben_dep, :ben_prov, :ben_dist, :ben_cta,
                :tipo_op, :desc_op, :origen, :cod_mon, :nom_mon, :monto
            )
        """)
        
        for _, row in df.iterrows():
            ejecutante_data = procesar_persona(row, 'ejecutante')
            ordenante_data = procesar_persona(row, 'ordenante')
            beneficiario_data = procesar_persona(row, 'beneficiario')
            
            # Insertar personas (devuelve None si no hay documento válido)
            ejecutante_id = insertar_o_actualizar_persona(db, ejecutante_data, row['fec_operacion'], row['mtotrx'], 'ejecutante')
            ordenante_id = insertar_o_actualizar_persona(db, ordenante_data, row['fec_operacion'], row['mtotrx'], 'ordenante')
            beneficiario_id = insertar_o_actualizar_persona(db, beneficiario_data, row['fec_operacion'], row['mtotrx'], 'beneficiario')
            
            # Usar safe_get para asegurar que pasamos None en lugar de NaN a la BD
            db.execute(query_trx, {
                'ro_id': ro_id,
                'busqueda': safe_get(row, 'busqueda'),
                'flag_tipo': safe_get(row, 'flgtipoclibusqueda'),
                'tipo_clasif': safe_get(row, 'destipclasifpartyrelacionado'),
                'num_reg': safe_get(row, 'num_registro_interno'),
                'canal': safe_get(row, 'descanal'),
                'ubigeo': safe_get(row, 'codigo_ubigeo'),
                'fecha': row['fec_operacion'],
                'hora': safe_get(row, 'hora_operacion'),
                'ej_id': ejecutante_id,
                'ej_tipo': safe_get(row, 'tipo_ejecutante'),
                'ej_doc_tipo': safe_get(row, 'tipo_doc_ejecutante'),
                'ej_doc': safe_get(row, 'doc_ejecutante_encriptado'),
                'ord_id': ordenante_id,
                'ord_tipo': safe_get(row, 'tipo_ordenante'),
                'ord_doc_tipo': safe_get(row, 'tipo_doc_ordenante'),
                'ord_doc': safe_get(row, 'doc_ordenante_encriptado'),
                'ord_ciiu': safe_get(row, 'CIIUOcupOrd'),
                'ord_ocup': safe_get(row, 'DesOcupOrd'),
                'ord_dep': safe_get(row, 'DepOrd'),
                'ord_prov': safe_get(row, 'ProvOrd'),
                'ord_dist': safe_get(row, 'DisOrd'),
                'ord_cta': safe_get(row, 'codcta20ordenante'),
                'ben_id': beneficiario_id,
                'ben_tipo': safe_get(row, 'tipo_beneficiario'),
                'ben_doc_tipo': safe_get(row, 'tipo_doc_beneficiario'),
                'ben_doc': safe_get(row, 'doc_beneficiario_encriptado'),
                'ben_ciiu': safe_get(row, 'CIIUOcupBen'),
                'ben_ocup': safe_get(row, 'DesOcupBen'),
                'ben_dep': safe_get(row, 'DepBen'),
                'ben_prov': safe_get(row, 'ProvBen'),
                'ben_dist': safe_get(row, 'DisBen'),
                'ben_cta': safe_get(row, 'codcta20beneficiario'),
                'tipo_op': safe_get(row, 'tipopereportesbs'),
                'desc_op': safe_get(row, 'destipopereportesbs'),
                'origen': safe_get(row, 'desorigendinero'),
                'cod_mon': safe_get(row, 'codmonedadestino'),
                'nom_mon': safe_get(row, 'nbrmonedadestino'),
                'monto': row['mtotrx']
            })

def procesar_archivo_ro(archivo_path, nombre_archivo, usuario='SYSTEM'):
    df = pd.read_excel(archivo_path)
    total_inicial = len(df)
    
    df = normalizar_columnas(df)
    validar_columnas(df)
    
    df_limpio = limpiar_datos(df)
    total_valido = len(df_limpio)
    total_descartado = total_inicial - total_valido
    
    ro_id = registrar_ro(nombre_archivo, total_inicial, total_valido, total_descartado, usuario)
    
    cargar_transacciones(df_limpio, ro_id)
    
    return {
        'ro_id': ro_id,
        'total': total_inicial,
        'validos': total_valido,
        'descartados': total_descartado
    }