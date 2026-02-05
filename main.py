import streamlit as st
import pandas as pd
from datetime import datetime
import os
import json

from database import get_db
from etl import procesar_archivo_ro
from casos import (
    crear_caso, listar_casos, obtener_caso, agregar_persona_a_caso,
    obtener_personas_caso, buscar_personas, actualizar_estado_caso,
    eliminar_persona_de_caso, listar_busquedas_disponibles,
    obtener_personas_por_busqueda, agregar_busqueda_a_caso
)
from analisis import generar_resumen_analisis
from tipologias import ejecutar_deteccion_tipologias, obtener_tipologias_por_caso
from redes import generar_reporte_red, exportar_para_visualizacion
from reportes import (
    generar_reporte_ejecutivo, exportar_transacciones_excel,
    exportar_tipologias_excel, generar_cronologia_transaccional
)
from grafo_viz import crear_grafo_interactivo

st.set_page_config(
    page_title="Sistema UIF - Análisis Financiero",
    layout="wide",
    initial_sidebar_state="expanded"
)

def sidebar_navigation():
    with st.sidebar:
        st.title("🔍 Sistema UIF")
        st.markdown("---")
        
        if 'caso_actual' in st.session_state and st.session_state['caso_actual']:
            caso = obtener_caso(st.session_state['caso_actual'])
            if caso:
                st.info(f"**Caso Activo:**\n{caso['nombre_caso']}")
                if st.button("✖ Cerrar Caso"):
                    del st.session_state['caso_actual']
                    st.rerun()
                st.markdown("---")
        
        st.subheader("Navegación")
        
        menu = st.radio(
            "Seleccionar Sección",
            ["🏠 Inicio", "📥 Carga de Datos", "📋 Gestión de Casos", 
             "📊 Análisis Transaccional", "⚠️ Detección de Tipologías", 
             "🕸️ Análisis de Redes", "📑 Reportes"],
            label_visibility="collapsed"
        )
        
        return menu.split(" ", 1)[1]

def main():
    menu = sidebar_navigation()
    
    if menu == "Inicio":
        pagina_inicio()
    elif menu == "Carga de Datos":
        pagina_carga_datos()
    elif menu == "Gestión de Casos":
        pagina_gestion_casos()
    elif menu == "Análisis Transaccional":
        pagina_analisis_transaccional()
    elif menu == "Detección de Tipologías":
        pagina_tipologias()
    elif menu == "Análisis de Redes":
        pagina_analisis_redes()
    elif menu == "Reportes":
        pagina_reportes()

def pagina_inicio():
    st.title("Sistema de Inteligencia Financiera - UIF")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Casos Activos", len([c for c in listar_casos() if c['estado'] == 'ACTIVO']))
    
    with col2:
        with get_db() as db:
            from sqlalchemy import text
            result = db.execute(text("SELECT COUNT(*) FROM personas")).fetchone()
            st.metric("Personas Registradas", result[0])
    
    with col3:
        with get_db() as db:
            from sqlalchemy import text
            result = db.execute(text("SELECT COUNT(*) FROM transacciones")).fetchone()
            st.metric("Transacciones", result[0])
    
    st.markdown("---")
    
    st.subheader("Casos Recientes")
    casos = listar_casos()[:5]
    
    if casos:
        df_casos = pd.DataFrame(casos)
        st.dataframe(df_casos[['caso_id', 'nombre_caso', 'estado', 'prioridad', 'fecha_creacion']], 
                    use_container_width=True)
    else:
        st.info("No hay casos registrados")

def pagina_carga_datos():
    st.header("📥 Carga de Registros de Operaciones")
    
    archivo = st.file_uploader("Seleccionar archivo Excel RO", type=['xlsx', 'xls'])
    
    if archivo:
        st.info(f"Archivo: {archivo.name}")
        
        if st.button("Procesar Archivo", type="primary"):
            with st.spinner("Procesando..."):
                try:
                    temp_path = f"/tmp/{archivo.name}"
                    with open(temp_path, "wb") as f:
                        f.write(archivo.getbuffer())
                    
                    resultado = procesar_archivo_ro(temp_path, archivo.name)
                    
                    os.remove(temp_path)
                    
                    st.success("✅ Archivo procesado exitosamente")
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Registros", resultado['total'])
                    col2.metric("Registros Válidos", resultado['validos'])
                    col3.metric("Registros Descartados", resultado['descartados'])
                    
                except Exception as e:
                    st.error(f"❌ Error al procesar archivo: {str(e)}")

def pagina_gestion_casos():
    st.header("📋 Gestión de Casos")
    
    tab1, tab2, tab3 = st.tabs(["Crear Caso", "Seleccionar Caso", "Administrar Caso"])
    
    with tab1:
        st.subheader("Crear Nuevo Caso")
        
        nombre_caso = st.text_input("Nombre del Caso")
        descripcion = st.text_area("Descripción")
        
        col1, col2 = st.columns(2)
        with col1:
            prioridad = st.selectbox("Prioridad", ["BAJA", "MEDIA", "ALTA", "URGENTE"])
        with col2:
            tipo_caso = st.selectbox("Tipo", ["INVESTIGACION", "SEGUIMIENTO", "PREVENTIVO"])
        
        if st.button("Crear Caso", type="primary"):
            if nombre_caso:
                caso_id = crear_caso(nombre_caso, descripcion, prioridad=prioridad, tipo_caso=tipo_caso)
                st.success(f"✅ Caso creado con ID: {caso_id}")
                st.session_state['caso_actual'] = caso_id
                st.rerun()
            else:
                st.error("Debe ingresar un nombre para el caso")
    
    with tab2:
        st.subheader("Seleccionar Caso para Trabajar")
        
        casos = listar_casos()
        
        if casos:
            df_casos = pd.DataFrame(casos)
            
            caso_seleccionado = st.selectbox(
                "Seleccionar Caso",
                df_casos['caso_id'].tolist(),
                format_func=lambda x: f"ID {x} - {df_casos[df_casos['caso_id']==x]['nombre_caso'].values[0]}",
                key="select_caso"
            )
            
            if st.button("Activar Caso", type="primary"):
                st.session_state['caso_actual'] = caso_seleccionado
                st.success(f"✅ Caso {caso_seleccionado} activado")
                st.rerun()
        else:
            st.info("No hay casos registrados")
    
    with tab3:
        if 'caso_actual' not in st.session_state:
            st.warning("Debe seleccionar un caso primero")
            return
        
        caso_id = st.session_state['caso_actual']
        caso = obtener_caso(caso_id)
        
        st.subheader(f"Caso: {caso['nombre_caso']}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Estado:** {caso['estado']}")
            st.write(f"**Prioridad:** {caso['prioridad']}")
        with col2:
            st.write(f"**Tipo:** {caso['tipo_caso']}")
            st.write(f"**Creación:** {caso['fecha_creacion']}")
        
        st.markdown("---")
        
        st.subheader("Personas en el Caso")
        personas_caso = obtener_personas_caso(caso_id)
        
        if personas_caso:
            df_personas = pd.DataFrame(personas_caso)
            
            for idx, persona in df_personas.iterrows():
                col1, col2, col3 = st.columns([3, 1, 1])
                with col1:
                    st.write(f"**{persona['documento_encriptado']}** - {persona['rol_en_caso']}")
                with col2:
                    st.write(f"Ops: {persona['total_operaciones']}")
                with col3:
                    if st.button("Eliminar", key=f"del_{persona['persona_id']}"):
                        eliminar_persona_de_caso(caso_id, persona['persona_id'])
                        st.success("Persona eliminada del caso")
                        st.rerun()
        else:
            st.info("No hay personas asignadas a este caso")
        
        st.markdown("---")
        
        st.subheader("Agregar Personas al Caso")
        
        metodo = st.radio("Método de Agregación", ["Por Búsqueda", "Individual"])
        
        if metodo == "Por Búsqueda":
            busquedas = listar_busquedas_disponibles()
            
            if busquedas:
                df_busquedas = pd.DataFrame(busquedas)
                
                busqueda_sel = st.selectbox(
                    "Seleccionar Búsqueda",
                    df_busquedas['busqueda'].tolist(),
                    format_func=lambda x: f"{x} ({df_busquedas[df_busquedas['busqueda']==x]['num_transacciones'].values[0]} transacciones)"
                )
                
                if st.button("Vista Previa"):
                    personas = obtener_personas_por_busqueda(busqueda_sel)
                    st.write(f"Se agregarán {len(personas)} personas:")
                    st.dataframe(pd.DataFrame(personas))
                
                rol_principal = st.selectbox("Rol del Principal", ["INVESTIGADO", "RELACIONADO", "TESTIGO"])
                
                if st.button("Agregar Búsqueda Completa", type="primary"):
                    contador = agregar_busqueda_a_caso(caso_id, busqueda_sel, rol_principal)
                    st.success(f"✅ Se agregaron {contador} personas al caso")
                    st.rerun()
            else:
                st.info("No hay búsquedas disponibles")
        
        else:
            termino_busqueda = st.text_input("Buscar persona por documento")
            
            if termino_busqueda:
                resultados = buscar_personas(termino_busqueda)
                
                if resultados:
                    df_resultados = pd.DataFrame(resultados)
                    
                    persona_seleccionada = st.selectbox(
                        "Seleccionar Persona",
                        df_resultados['persona_id'].tolist(),
                        format_func=lambda x: df_resultados[df_resultados['persona_id']==x]['documento_encriptado'].values[0]
                    )
                    
                    rol = st.selectbox("Rol en el Caso", ["INVESTIGADO", "RELACIONADO", "TESTIGO"])
                    motivo = st.text_area("Motivo de Inclusión")
                    
                    if st.button("Agregar al Caso", type="primary"):
                        agregar_persona_a_caso(caso_id, persona_seleccionada, rol, motivo)
                        st.success("✅ Persona agregada al caso")
                        st.rerun()
                else:
                    st.warning("No se encontraron resultados")

def pagina_analisis_transaccional():
    st.header("📊 Análisis Transaccional")
    
    if 'caso_actual' not in st.session_state:
        st.warning("⚠️ Debe seleccionar un caso primero")
        return
    
    caso_id = st.session_state['caso_actual']
    caso = obtener_caso(caso_id)
    
    if st.button("Ejecutar Análisis Completo", type="primary"):
        with st.spinner("Analizando..."):
            try:
                analisis = generar_resumen_analisis(caso_id)
                
                tabs = st.tabs([
                    "Principales Ordenantes", "Principales Beneficiarios", 
                    "Concentración", "Frecuencia", "Ventanas Cortas",
                    "Montos Similares", "Pitufeo", "Cadenas", "Circularidad"
                ])
                
                with tabs[0]:
                    if analisis['principales_ordenantes']:
                        st.dataframe(pd.DataFrame(analisis['principales_ordenantes']), use_container_width=True)
                
                with tabs[1]:
                    if analisis['principales_beneficiarios']:
                        st.dataframe(pd.DataFrame(analisis['principales_beneficiarios']), use_container_width=True)
                
                with tabs[2]:
                    if analisis['concentracion_montos']:
                        st.dataframe(pd.DataFrame(analisis['concentracion_montos']), use_container_width=True)
                
                with tabs[3]:
                    if analisis['frecuencia_inusual']:
                        st.dataframe(pd.DataFrame(analisis['frecuencia_inusual']), use_container_width=True)
                
                with tabs[4]:
                    if analisis['ventanas_cortas']:
                        st.dataframe(pd.DataFrame(analisis['ventanas_cortas']), use_container_width=True)
                
                with tabs[5]:
                    if analisis['montos_similares']:
                        st.dataframe(pd.DataFrame(analisis['montos_similares']), use_container_width=True)
                
                with tabs[6]:
                    if analisis['pitufeo']:
                        st.dataframe(pd.DataFrame(analisis['pitufeo']), use_container_width=True)
                
                with tabs[7]:
                    st.write(f"Cadenas detectadas: {len(analisis['cadenas'])}")
                
                with tabs[8]:
                    st.write(f"Ciclos detectados: {len(analisis['circularidad'])}")
                
            except Exception as e:
                st.error(f"❌ Error en análisis: {str(e)}")

def pagina_tipologias():
    st.header("⚠️ Detección de Tipologías")
    
    if 'caso_actual' not in st.session_state:
        st.warning("⚠️ Debe seleccionar un caso primero")
        return
    
    caso_id = st.session_state['caso_actual']
    
    if st.button("Ejecutar Detección de Tipologías", type="primary"):
        with st.spinner("Detectando tipologías..."):
            try:
                resultados = ejecutar_deteccion_tipologias(caso_id)
                st.success(f"✅ Se detectaron {len(resultados)} alertas")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    st.subheader("Tipologías Detectadas")
    
    tipologias = obtener_tipologias_por_caso(caso_id)
    
    if tipologias:
        df = pd.DataFrame(tipologias)
        
        st.dataframe(
            df[['codigo', 'nombre', 'categoria', 'nivel_riesgo', 'nivel_confianza', 'estado']],
            use_container_width=True
        )
        
        st.markdown("---")
        
        detalle = st.selectbox(
            "Ver Detalle de Tipología",
            df['deteccion_id'].tolist(),
            format_func=lambda x: df[df['deteccion_id']==x]['nombre'].values[0]
        )
        
        if detalle:
            tip_detalle = df[df['deteccion_id']==detalle].iloc[0]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Nivel de Riesgo", tip_detalle['nivel_riesgo'])
            with col2:
                st.metric("Confianza", f"{tip_detalle['nivel_confianza']}%")
            with col3:
                st.metric("Estado", tip_detalle['estado'])
            
            st.write(f"**Código:** {tip_detalle['codigo']}")
            st.write(f"**Categoría:** {tip_detalle['categoria']}")
            
            if tip_detalle['evidencias']:
                with st.expander("Ver Evidencias"):
                    st.json(tip_detalle['evidencias'])
    else:
        st.info("No se han detectado tipologías para este caso")

def pagina_analisis_redes():
    st.header("🕸️ Análisis de Redes")
    
    if 'caso_actual' not in st.session_state:
        st.warning("⚠️ Debe seleccionar un caso primero")
        return
    
    caso_id = st.session_state['caso_actual']
    
    if st.button("Generar Análisis de Red", type="primary"):
        with st.spinner("Generando análisis de red..."):
            try:
                reporte = generar_reporte_red(caso_id)
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Nodos", reporte['densidad']['num_nodos'])
                col2.metric("Aristas", reporte['densidad']['num_aristas'])
                col3.metric("Densidad", f"{reporte['densidad']['densidad']:.4f}")
                
                st.markdown("---")
                
                tab1, tab2, tab3 = st.tabs(["Visualización", "Intermediarios", "Componentes"])
                
                with tab1:
                    st.subheader("Grafo Interactivo")
                    grafo_data = reporte['grafo_json']
                    
                    crear_grafo_interactivo(
                        json.dumps(grafo_data['nodes']),
                        json.dumps(grafo_data['links'])
                    )
                
                with tab2:
                    if reporte['intermediarios']:
                        st.subheader("Principales Intermediarios")
                        df_inter = pd.DataFrame(reporte['intermediarios'])
                        st.dataframe(df_inter, use_container_width=True)
                
                with tab3:
                    st.subheader("Componentes de la Red")
                    st.write(f"**Componentes débiles:** {reporte['componentes']['num_componentes_debiles']}")
                    st.write(f"**Componentes fuertes:** {reporte['componentes']['num_componentes_fuertes']}")
                    st.write(f"**Tamaño componente principal:** {reporte['componentes']['componente_principal_tamano']}")
                
            except Exception as e:
                st.error(f"❌ Error en análisis de red: {str(e)}")
                st.exception(e)

def pagina_reportes():
    st.header("📑 Generación de Reportes")
    
    if 'caso_actual' not in st.session_state:
        st.warning("⚠️ Debe seleccionar un caso primero")
        return
    
    caso_id = st.session_state['caso_actual']
    caso = obtener_caso(caso_id)
    
    tipo_reporte = st.selectbox(
        "Tipo de Reporte",
        ["Reporte Ejecutivo PDF", "Transacciones Excel", "Tipologías Excel", "Cronología Excel"]
    )
    
    if st.button("Generar Reporte", type="primary"):
        with st.spinner("Generando reporte..."):
            try:
                if tipo_reporte == "Reporte Ejecutivo PDF":
                    buffer = generar_reporte_ejecutivo(caso_id)
                    st.download_button(
                        label="📥 Descargar PDF",
                        data=buffer,
                        file_name=f"reporte_caso_{caso_id}_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf"
                    )
                
                elif tipo_reporte == "Transacciones Excel":
                    buffer = exportar_transacciones_excel(caso_id)
                    st.download_button(
                        label="📥 Descargar Excel",
                        data=buffer,
                        file_name=f"transacciones_caso_{caso_id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                elif tipo_reporte == "Tipologías Excel":
                    buffer = exportar_tipologias_excel(caso_id)
                    st.download_button(
                        label="📥 Descargar Excel",
                        data=buffer,
                        file_name=f"tipologias_caso_{caso_id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                elif tipo_reporte == "Cronología Excel":
                    buffer = generar_cronologia_transaccional(caso_id)
                    st.download_button(
                        label="📥 Descargar Excel",
                        data=buffer,
                        file_name=f"cronologia_caso_{caso_id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                st.success("✅ Reporte generado exitosamente")
                
            except Exception as e:
                st.error(f"❌ Error al generar reporte: {str(e)}")

if __name__ == "__main__":
    main()
