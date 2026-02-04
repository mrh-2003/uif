import networkx as nx
from database import get_db
import pandas as pd
import json

def construir_grafo_caso(caso_id, incluir_cuentas=True):
    transacciones = obtener_transacciones_caso(caso_id)
    
    G = nx.DiGraph()
    
    for trx in transacciones:
        ordenante = trx['ordenante_id']
        beneficiario = trx['beneficiario_id']
        monto = float(trx['monto'])
        
        if not G.has_node(ordenante):
            G.add_node(ordenante, tipo='persona', documento=trx['doc_ordenante'])
        if not G.has_node(beneficiario):
            G.add_node(beneficiario, tipo='persona', documento=trx['doc_beneficiario'])
        
        if incluir_cuentas and trx['cuenta_ordenante']:
            cuenta_ord = f"CTA_{trx['cuenta_ordenante']}"
            G.add_node(cuenta_ord, tipo='cuenta', numero=trx['cuenta_ordenante'])
            G.add_edge(ordenante, cuenta_ord, tipo='titular')
        
        if incluir_cuentas and trx['cuenta_beneficiario']:
            cuenta_ben = f"CTA_{trx['cuenta_beneficiario']}"
            G.add_node(cuenta_ben, tipo='cuenta', numero=trx['cuenta_beneficiario'])
            G.add_edge(cuenta_ben, beneficiario, tipo='titular')
        
        if G.has_edge(ordenante, beneficiario):
            G[ordenante][beneficiario]['peso'] += monto
            G[ordenante][beneficiario]['num_transacciones'] += 1
        else:
            G.add_edge(ordenante, beneficiario, peso=monto, num_transacciones=1)
    
    return G

def obtener_transacciones_caso(caso_id):
    with get_db() as db:
        query = """
            SELECT 
                t.transaccion_id,
                t.ordenante_id,
                t.beneficiario_id,
                t.monto,
                t.fecha_operacion,
                po.documento_encriptado as doc_ordenante,
                pb.documento_encriptado as doc_beneficiario,
                t.cuenta_ordenante,
                t.cuenta_beneficiario
            FROM transacciones t
            JOIN personas po ON t.ordenante_id = po.persona_id
            JOIN personas pb ON t.beneficiario_id = pb.persona_id
            JOIN casos_personas cp ON (
                t.ordenante_id = cp.persona_id OR 
                t.beneficiario_id = cp.persona_id
            )
            WHERE cp.caso_id = :caso_id
        """
        return [dict(row) for row in db.execute(query, {'caso_id': caso_id}).fetchall()]

def calcular_metricas_centralidad(G):
    metricas = {}
    
    try:
        metricas['degree_centrality'] = nx.degree_centrality(G)
    except:
        metricas['degree_centrality'] = {}
    
    try:
        metricas['in_degree_centrality'] = nx.in_degree_centrality(G)
    except:
        metricas['in_degree_centrality'] = {}
    
    try:
        metricas['out_degree_centrality'] = nx.out_degree_centrality(G)
    except:
        metricas['out_degree_centrality'] = {}
    
    try:
        metricas['betweenness_centrality'] = nx.betweenness_centrality(G, weight='peso')
    except:
        metricas['betweenness_centrality'] = {}
    
    try:
        metricas['closeness_centrality'] = nx.closeness_centrality(G)
    except:
        metricas['closeness_centrality'] = {}
    
    try:
        metricas['pagerank'] = nx.pagerank(G, weight='peso')
    except:
        metricas['pagerank'] = {}
    
    return metricas

def identificar_intermediarios(G, top_n=10):
    betweenness = nx.betweenness_centrality(G, weight='peso')
    
    intermediarios = sorted(
        betweenness.items(),
        key=lambda x: x[1],
        reverse=True
    )[:top_n]
    
    resultados = []
    for nodo, score in intermediarios:
        if G.nodes[nodo].get('tipo') == 'persona':
            resultados.append({
                'nodo_id': nodo,
                'documento': G.nodes[nodo].get('documento'),
                'betweenness_score': score,
                'grado_entrada': G.in_degree(nodo),
                'grado_salida': G.out_degree(nodo)
            })
    
    return resultados

def detectar_comunidades(G):
    G_no_dirigido = G.to_undirected()
    
    try:
        comunidades = nx.community.louvain_communities(G_no_dirigido, weight='peso')
        return [list(c) for c in comunidades]
    except:
        try:
            comunidades = nx.community.greedy_modularity_communities(G_no_dirigido, weight='peso')
            return [list(c) for c in comunidades]
        except:
            return []

def encontrar_caminos_criticos(G, origen, destino, max_caminos=5):
    try:
        caminos = list(nx.all_simple_paths(G, origen, destino, cutoff=6))
        
        caminos_con_peso = []
        for camino in caminos[:max_caminos]:
            peso_total = sum(
                G[camino[i]][camino[i+1]]['peso'] 
                for i in range(len(camino)-1)
            )
            caminos_con_peso.append({
                'camino': camino,
                'peso_total': peso_total,
                'longitud': len(camino)
            })
        
        return sorted(caminos_con_peso, key=lambda x: x['peso_total'], reverse=True)
    except:
        return []

def analizar_componentes_conexas(G):
    componentes_debiles = list(nx.weakly_connected_components(G))
    componentes_fuertes = list(nx.strongly_connected_components(G))
    
    return {
        'num_componentes_debiles': len(componentes_debiles),
        'num_componentes_fuertes': len(componentes_fuertes),
        'componente_principal_tamano': len(max(componentes_debiles, key=len)) if componentes_debiles else 0,
        'componentes_debiles': [list(c) for c in componentes_debiles],
        'componentes_fuertes': [list(c) for c in componentes_fuertes]
    }

def calcular_densidad_red(G):
    return {
        'densidad': nx.density(G),
        'num_nodos': G.number_of_nodes(),
        'num_aristas': G.number_of_edges(),
        'grado_promedio': sum(dict(G.degree()).values()) / G.number_of_nodes() if G.number_of_nodes() > 0 else 0
    }

def exportar_para_visualizacion(G):
    nodos = []
    for nodo_id, datos in G.nodes(data=True):
        nodos.append({
            'id': str(nodo_id),
            'label': datos.get('documento', str(nodo_id))[:20],
            'tipo': datos.get('tipo', 'persona'),
            'size': G.degree(nodo_id) * 2 + 10
        })
    
    aristas = []
    for origen, destino, datos in G.edges(data=True):
        aristas.append({
            'source': str(origen),
            'target': str(destino),
            'value': float(datos.get('peso', 1)),
            'num_transacciones': datos.get('num_transacciones', 1)
        })
    
    return {
        'nodes': nodos,
        'links': aristas
    }

def generar_reporte_red(caso_id):
    G = construir_grafo_caso(caso_id)
    
    metricas = calcular_metricas_centralidad(G)
    intermediarios = identificar_intermediarios(G)
    comunidades = detectar_comunidades(G)
    componentes = analizar_componentes_conexas(G)
    densidad = calcular_densidad_red(G)
    
    return {
        'metricas_centralidad': metricas,
        'intermediarios': intermediarios,
        'comunidades': comunidades,
        'componentes': componentes,
        'densidad': densidad,
        'grafo_json': exportar_para_visualizacion(G)
    }

def buscar_nodos_criticos(G, percentil=90):
    metricas = calcular_metricas_centralidad(G)
    
    nodos_criticos = set()
    
    for metrica_nombre, valores in metricas.items():
        if not valores:
            continue
        
        umbral = pd.Series(list(valores.values())).quantile(percentil / 100)
        
        for nodo, valor in valores.items():
            if valor >= umbral:
                nodos_criticos.add(nodo)
    
    resultados = []
    for nodo in nodos_criticos:
        if G.nodes[nodo].get('tipo') == 'persona':
            resultados.append({
                'nodo_id': nodo,
                'documento': G.nodes[nodo].get('documento'),
                'metricas': {
                    nombre: metricas[nombre].get(nodo, 0)
                    for nombre in metricas.keys()
                }
            })
    
    return resultados
