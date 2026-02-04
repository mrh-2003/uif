from datetime import datetime, timedelta
import hashlib
import json

def calcular_hash(texto):
    return hashlib.sha256(texto.encode()).hexdigest()

def formatear_monto(monto):
    return f"S/ {float(monto):,.2f}"

def formatear_fecha(fecha):
    if isinstance(fecha, str):
        fecha = datetime.strptime(fecha, '%Y-%m-%d')
    return fecha.strftime('%d/%m/%Y')

def calcular_dias_entre_fechas(fecha_inicio, fecha_fin):
    if isinstance(fecha_inicio, str):
        fecha_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, '%Y-%m-%d')
    return (fecha_fin - fecha_inicio).days

def normalizar_documento(documento):
    return documento.strip().upper()

def clasificar_riesgo_monto(monto):
    if monto < 10000:
        return "BAJO"
    elif monto < 50000:
        return "MEDIO"
    elif monto < 200000:
        return "ALTO"
    else:
        return "CRITICO"

def clasificar_riesgo_frecuencia(num_operaciones, dias):
    if dias == 0:
        return "CRITICO"
    
    frecuencia_diaria = num_operaciones / dias
    
    if frecuencia_diaria < 1:
        return "BAJO"
    elif frecuencia_diaria < 5:
        return "MEDIO"
    elif frecuencia_diaria < 10:
        return "ALTO"
    else:
        return "CRITICO"

def generar_codigo_caso(nombre):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    prefijo = ''.join([c for c in nombre.upper() if c.isalpha()])[:4]
    return f"{prefijo}-{timestamp}"

def safe_json_loads(json_str):
    try:
        if isinstance(json_str, str):
            return json.loads(json_str)
        return json_str
    except:
        return {}

def safe_json_dumps(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except:
        return "{}"

def agrupar_por_rangos_monto(transacciones, rangos=[1000, 10000, 50000, 100000]):
    grupos = {f"0-{rangos[0]}": []}
    
    for i in range(len(rangos)):
        if i < len(rangos) - 1:
            grupos[f"{rangos[i]}-{rangos[i+1]}"] = []
        else:
            grupos[f"{rangos[i]}+"] = []
    
    for trx in transacciones:
        monto = float(trx.get('monto', 0))
        
        if monto < rangos[0]:
            grupos[f"0-{rangos[0]}"].append(trx)
        else:
            for i in range(len(rangos)):
                if i < len(rangos) - 1:
                    if rangos[i] <= monto < rangos[i+1]:
                        grupos[f"{rangos[i]}-{rangos[i+1]}"].append(trx)
                        break
                else:
                    if monto >= rangos[i]:
                        grupos[f"{rangos[i]}+"].append(trx)
                        break
    
    return grupos

def calcular_estadisticas_basicas(valores):
    if not valores:
        return {}
    
    return {
        'min': min(valores),
        'max': max(valores),
        'promedio': sum(valores) / len(valores),
        'total': sum(valores),
        'count': len(valores)
    }

def detectar_outliers_iqr(valores, factor=1.5):
    if len(valores) < 4:
        return []
    
    valores_ordenados = sorted(valores)
    n = len(valores_ordenados)
    
    q1 = valores_ordenados[n // 4]
    q3 = valores_ordenados[(3 * n) // 4]
    iqr = q3 - q1
    
    limite_inferior = q1 - factor * iqr
    limite_superior = q3 + factor * iqr
    
    outliers = [v for v in valores if v < limite_inferior or v > limite_superior]
    
    return outliers

def generar_resumen_periodo(transacciones, campo_fecha='fecha_operacion'):
    if not transacciones:
        return {}
    
    fechas = [t[campo_fecha] for t in transacciones if campo_fecha in t]
    
    if not fechas:
        return {}
    
    fecha_min = min(fechas)
    fecha_max = max(fechas)
    
    return {
        'fecha_inicio': fecha_min,
        'fecha_fin': fecha_max,
        'dias_totales': calcular_dias_entre_fechas(fecha_min, fecha_max),
        'num_transacciones': len(transacciones)
    }
