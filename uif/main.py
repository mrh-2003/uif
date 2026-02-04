import streamlit as st
import pandas as pd
from datetime import datetime
import os
import sys

from database import get_db
from etl import procesar_archivo_ro
from casos import (
    crear_caso, listar_casos, obtener_caso, agregar_persona_a_caso,
    obtener_personas_caso, buscar_personas, actualizar_estado_caso,
    eliminar_persona_de_caso
)
from analisis import generar_resumen_analisis
from tipologias import ejecutar_deteccion_tipologias, obtener_tipologias_por_caso
from redes import generar_reporte_red, exportar_para_visualizacion
from reportes import (
    generar_reporte_ejecutivo, exportar_transacciones_excel,
    exportar_tipologias_excel, generar_cronologia_transaccional
)

st.set_page_config(
    page_title="Sistema UIF - Análisis Financiero",
    page_size="wide",
    initial_sidebar_state="expanded"
)

def main():
    st.title("Sistema de Inteligencia Financiera - UIF")
    
    menu = st.sidebar.selectbox(
        "Navegación",
        ["Carga de Datos", "Gestión de Casos", "Análisis Transaccional", 
         "Detección de Tipologías", "Análisis de Redes", "Reportes"]
    )
    
    if menu == "Carga de Datos":
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

def pagina_carga_datos():
    st.header("Carga de Registros de Operaciones")
    
    archivo = st.file_uploader("Seleccionar archivo Excel RO", type=['xlsx', 'xls'])
    
    if archivo:
        st.info(f"Archivo: {archivo.name}")
        
        if st.button("Procesar Archivo"):
            with st.spinner("Procesando..."):
                try:
                    temp_path = f"/tmp/{archivo.name}"
                    with open(temp_path, "wb") as f:
                        f.write(archivo.getbuffer())
                    
                    resultado = procesar_archivo_ro(temp_path, archivo.name)
                    
                    os.remove(temp_path)
                    
                    st.success("Archivo procesado exitosamente")
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Registros", resultado['total'])
                    col2.metric("Registros Válidos", resultado['validos'])
                    col3.metric("Registros Descartados", resultado['descartados'])
                    
                except Exception as e:
                    st.error(f"Error al procesar archivo: {str(e)}")

def pagina_gestion_casos():
    st.header("Gestión de Casos")
    
    tab1, tab2 = st.tabs(["Crear Caso", "Listar Casos"])
    
    with tab1:
        st.subheader("Crear Nuevo Caso")
        
        nombre_caso = st.text_input("Nombre del Caso")
        descripcion = st.text_area("Descripción")
        
        col1, col2 = st.columns(2)
        with col1:
            prioridad = st.selectbox("Prioridad", ["BAJA", "MEDIA", "ALTA", "URGENTE"])
        with col2:
            tipo_caso = st.selectbox("Tipo", ["INVESTIGACION", "SEGUIMIENTO", "PREVENTIVO"])
        
        if st.button("Crear Caso"):
            if nombre_caso:
                caso_id = crear_caso(nombre_caso, descripcion, prioridad=prioridad, tipo_caso=tipo_caso)
                st.success(f"Caso creado con ID: {caso_id}")
                st.session_state['caso_actual'] = caso_id
            else:
                st.error("Debe ingresar un nombre para el caso")
    
    with tab2:
        st.subheader("Casos Existentes")
        
        casos = listar_casos()
        
        if casos:
            df_casos = pd.DataFrame([dict(c) for c in casos])
            
            caso_seleccionado = st.selectbox(
                "Seleccionar Caso",
                df_casos['caso_id'].tolist(),
                format_func=lambda x: f"ID {x} - {df_casos[df_casos['caso_id']==x]['nombre_caso'].values[0]}"
            )
            
            if caso_seleccionado:
                st.session_state['caso_actual'] = caso_seleccionado
                
                caso = obtener_caso(caso_seleccionado)
                
                st.write(f"**Estado:** {caso['estado']}")
                st.write(f"**Prioridad:** {caso['prioridad']}")
                st.write(f"**Descripción:** {caso['descripcion']}")
                
                st.subheader("Personas en el Caso")
                personas_caso = obtener_personas_caso(caso_seleccionado)
                
                if personas_caso:
                    df_personas = pd.DataFrame([dict(p) for p in personas_caso])
                    st.dataframe(df_personas)
                else:
                    st.info("No hay personas asignadas a este caso")
                
                st.subheader("Agregar Persona al Caso")
                
                termino_busqueda = st.text_input("Buscar persona por documento")
                
                if termino_busqueda:
                    resultados = buscar_personas(termino_busqueda)
                    
                    if resultados:
                        df_resultados = pd.DataFrame([dict(r) for r in resultados])
                        
                        persona_seleccionada = st.selectbox(
                            "Seleccionar Persona",
                            df_resultados['persona_id'].tolist(),
                            format_func=lambda x: df_resultados[df_resultados['persona_id']==x]['documento_encriptado'].values[0]
                        )
                        
                        rol = st.selectbox("Rol en el Caso", ["INVESTIGADO", "RELACIONADO", "TESTIGO"])
                        motivo = st.text_area("Motivo de Inclusión")
                        
                        if st.button("Agregar al Caso"):
                            agregar_persona_a_caso(caso_seleccionado, persona_seleccionada, rol, motivo)
                            st.success("Persona agregada al caso")
                            st.rerun()
                    else:
                        st.warning("No se encontraron resultados")
        else:
            st.info("No hay casos registrados")

def pagina_analisis_transaccional():
    st.header("Análisis Transaccional")
    
    if 'caso_actual' not in st.session_state:
        st.warning("Debe seleccionar un caso primero")
        return
    
    caso_id = st.session_state['caso_actual']
    caso = obtener_caso(caso_id)
    
    st.info(f"Caso Actual: {caso['nombre_caso']}")
    
    if st.button("Ejecutar Análisis Completo"):
        with st.spinner("Analizando..."):
            try:
                analisis = generar_resumen_analisis(caso_id)
                
                st.subheader("Principales Ordenantes")
                if analisis['principales_ordenantes']:
                    df = pd.DataFrame([dict(r) for r in analisis['principales_ordenantes']])
                    st.dataframe(df)
                
                st.subheader("Principales Beneficiarios")
                if analisis['principales_beneficiarios']:
                    df = pd.DataFrame([dict(r) for r in analisis['principales_beneficiarios']])
                    st.dataframe(df)
                
                st.subheader("Concentración de Montos")
                if analisis['concentracion_montos']:
                    df = pd.DataFrame([dict(r) for r in analisis['concentracion_montos']])
                    st.dataframe(df)
                
                st.subheader("Frecuencia Inusual")
                if analisis['frecuencia_inusual']:
                    df = pd.DataFrame([dict(r) for r in analisis['frecuencia_inusual']])
                    st.dataframe(df)
                
                st.subheader("Ventanas Cortas de Tiempo")
                if analisis['ventanas_cortas']:
                    df = pd.DataFrame([dict(r) for r in analisis['ventanas_cortas']])
                    st.dataframe(df)
                
                st.subheader("Montos Similares Repetidos")
                if analisis['montos_similares']:
                    df = pd.DataFrame([dict(r) for r in analisis['montos_similares']])
                    st.dataframe(df)
                
                st.subheader("Pitufeo Detectado")
                if analisis['pitufeo']:
                    df = pd.DataFrame([dict(r) for r in analisis['pitufeo']])
                    st.dataframe(df)
                
                st.subheader("Cadenas de Transferencia")
                if analisis['cadenas']:
                    st.write(f"Cadenas detectadas: {len(analisis['cadenas'])}")
                
                st.subheader("Circularidad")
                if analisis['circularidad']:
                    st.write(f"Ciclos detectados: {len(analisis['circularidad'])}")
                
            except Exception as e:
                st.error(f"Error en análisis: {str(e)}")

def pagina_tipologias():
    st.header("Detección de Tipologías")
    
    if 'caso_actual' not in st.session_state:
        st.warning("Debe seleccionar un caso primero")
        return
    
    caso_id = st.session_state['caso_actual']
    caso = obtener_caso(caso_id)
    
    st.info(f"Caso Actual: {caso['nombre_caso']}")
    
    if st.button("Ejecutar Detección de Tipologías"):
        with st.spinner("Detectando tipologías..."):
            try:
                resultados = ejecutar_deteccion_tipologias(caso_id)
                st.success(f"Se detectaron {len(resultados)} alertas")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    st.subheader("Tipologías Detectadas")
    
    tipologias = obtener_tipologias_por_caso(caso_id)
    
    if tipologias:
        df = pd.DataFrame(tipologias)
        
        df_display = df[['codigo', 'nombre', 'categoria', 'nivel_riesgo', 'nivel_confianza', 'estado']]
        
        st.dataframe(df_display)
        
        detalle = st.selectbox(
            "Ver Detalle",
            df['deteccion_id'].tolist(),
            format_func=lambda x: df[df['deteccion_id']==x]['nombre'].values[0]
        )
        
        if detalle:
            tip_detalle = df[df['deteccion_id']==detalle].iloc[0]
            
            st.subheader("Detalles de la Tipología")
            st.write(f"**Código:** {tip_detalle['codigo']}")
            st.write(f"**Nombre:** {tip_detalle['nombre']}")
            st.write(f"**Categoría:** {tip_detalle['categoria']}")
            st.write(f"**Nivel de Riesgo:** {tip_detalle['nivel_riesgo']}")
            st.write(f"**Nivel de Confianza:** {tip_detalle['nivel_confianza']}%")
            st.write(f"**Estado:** {tip_detalle['estado']}")
            
            if tip_detalle['evidencias']:
                st.subheader("Evidencias")
                st.json(tip_detalle['evidencias'])
    else:
        st.info("No se han detectado tipologías para este caso")

def pagina_analisis_redes():
    st.header("Análisis de Redes")
    
    if 'caso_actual' not in st.session_state:
        st.warning("Debe seleccionar un caso primero")
        return
    
    caso_id = st.session_state['caso_actual']
    caso = obtener_caso(caso_id)
    
    st.info(f"Caso Actual: {caso['nombre_caso']}")
    
    if st.button("Generar Análisis de Red"):
        with st.spinner("Generando análisis de red..."):
            try:
                reporte = generar_reporte_red(caso_id)
                
                st.subheader("Métricas de Densidad")
                col1, col2, col3 = st.columns(3)
                col1.metric("Nodos", reporte['densidad']['num_nodos'])
                col2.metric("Aristas", reporte['densidad']['num_aristas'])
                col3.metric("Densidad", f"{reporte['densidad']['densidad']:.4f}")
                
                st.subheader("Principales Intermediarios")
                if reporte['intermediarios']:
                    df_inter = pd.DataFrame(reporte['intermediarios'])
                    st.dataframe(df_inter)
                
                st.subheader("Componentes de la Red")
                st.write(f"Componentes débiles: {reporte['componentes']['num_componentes_debiles']}")
                st.write(f"Componentes fuertes: {reporte['componentes']['num_componentes_fuertes']}")
                
                st.subheader("Visualización de Red")
                grafo_data = reporte['grafo_json']
                st.json(grafo_data)
                
            except Exception as e:
                st.error(f"Error en análisis de red: {str(e)}")

def pagina_reportes():
    st.header("Generación de Reportes")
    
    if 'caso_actual' not in st.session_state:
        st.warning("Debe seleccionar un caso primero")
        return
    
    caso_id = st.session_state['caso_actual']
    caso = obtener_caso(caso_id)
    
    st.info(f"Caso Actual: {caso['nombre_caso']}")
    
    tipo_reporte = st.selectbox(
        "Tipo de Reporte",
        ["Reporte Ejecutivo PDF", "Transacciones Excel", "Tipologías Excel", "Cronología Excel"]
    )
    
    if st.button("Generar Reporte"):
        with st.spinner("Generando reporte..."):
            try:
                if tipo_reporte == "Reporte Ejecutivo PDF":
                    buffer = generar_reporte_ejecutivo(caso_id)
                    st.download_button(
                        label="Descargar PDF",
                        data=buffer,
                        file_name=f"reporte_caso_{caso_id}_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf"
                    )
                
                elif tipo_reporte == "Transacciones Excel":
                    buffer = exportar_transacciones_excel(caso_id)
                    st.download_button(
                        label="Descargar Excel",
                        data=buffer,
                        file_name=f"transacciones_caso_{caso_id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                elif tipo_reporte == "Tipologías Excel":
                    buffer = exportar_tipologias_excel(caso_id)
                    st.download_button(
                        label="Descargar Excel",
                        data=buffer,
                        file_name=f"tipologias_caso_{caso_id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                elif tipo_reporte == "Cronología Excel":
                    buffer = generar_cronologia_transaccional(caso_id)
                    st.download_button(
                        label="Descargar Excel",
                        data=buffer,
                        file_name=f"cronologia_caso_{caso_id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                st.success("Reporte generado exitosamente")
                
            except Exception as e:
                st.error(f"Error al generar reporte: {str(e)}")

if __name__ == "__main__":
    main()
