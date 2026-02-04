from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from datetime import datetime
import pandas as pd
from io import BytesIO
from database import get_db
from casos import obtener_caso, obtener_personas_caso, obtener_transacciones_caso
from tipologias import obtener_tipologias_por_caso
from redes import generar_reporte_red
import json

def generar_reporte_ejecutivo(caso_id):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=72, leftMargin=72,
                           topMargin=72, bottomMargin=18)
    
    elementos = []
    styles = getSampleStyleSheet()
    
    style_titulo = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#1a1a1a'),
        spaceAfter=30,
        alignment=TA_CENTER
    )
    
    style_subtitulo = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#333333'),
        spaceAfter=12
    )
    
    style_normal = ParagraphStyle(
        'CustomNormal',
        parent=styles['Normal'],
        fontSize=10,
        alignment=TA_JUSTIFY
    )
    
    caso = obtener_caso(caso_id)
    elementos.append(Paragraph("REPORTE DE INTELIGENCIA FINANCIERA", style_titulo))
    elementos.append(Spacer(1, 12))
    
    info_caso = [
        ['Caso:', caso['nombre_caso']],
        ['Fecha Generación:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        ['Estado:', caso['estado']],
        ['Prioridad:', caso['prioridad']]
    ]
    
    tabla_caso = Table(info_caso, colWidths=[2*inch, 4*inch])
    tabla_caso.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.grey),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    elementos.append(tabla_caso)
    elementos.append(Spacer(1, 24))
    
    elementos.append(Paragraph("RESUMEN EJECUTIVO", style_subtitulo))
    
    personas = obtener_personas_caso(caso_id)
    transacciones = obtener_transacciones_caso(caso_id)
    tipologias = obtener_tipologias_por_caso(caso_id)
    
    total_monto = sum([float(t['monto']) for t in transacciones])
    
    resumen_texto = f"""
    Se identificaron {len(personas)} personas involucradas en un total de {len(transacciones)} 
    transacciones por un monto acumulado de S/ {total_monto:,.2f}. El análisis detectó 
    {len(tipologias)} alertas de tipologías de lavado de activos.
    """
    
    elementos.append(Paragraph(resumen_texto, style_normal))
    elementos.append(Spacer(1, 24))
    
    elementos.append(Paragraph("PERSONAS INVOLUCRADAS", style_subtitulo))
    
    datos_personas = [['Documento', 'Rol', 'Operaciones', 'Monto Total']]
    for persona in personas[:10]:
        datos_personas.append([
            persona['documento_encriptado'][:20],
            persona['rol_en_caso'],
            str(persona['total_operaciones']),
            f"S/ {float(persona['monto_total']):,.2f}"
        ])
    
    tabla_personas = Table(datos_personas, colWidths=[2*inch, 1.5*inch, 1*inch, 1.5*inch])
    tabla_personas.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
    ]))
    
    elementos.append(tabla_personas)
    elementos.append(PageBreak())
    
    elementos.append(Paragraph("TIPOLOGÍAS DETECTADAS", style_subtitulo))
    
    if tipologias:
        datos_tipologias = [['Tipología', 'Categoría', 'Riesgo', 'Confianza']]
        for tip in tipologias[:15]:
            datos_tipologias.append([
                tip['nombre'][:40],
                tip['categoria'],
                str(tip['nivel_riesgo']),
                f"{float(tip['nivel_confianza']):.1f}%"
            ])
        
        tabla_tipologias = Table(datos_tipologias, colWidths=[2.5*inch, 1.5*inch, 0.8*inch, 1*inch])
        tabla_tipologias.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
        ]))
        
        elementos.append(tabla_tipologias)
    else:
        elementos.append(Paragraph("No se detectaron tipologías sospechosas.", style_normal))
    
    elementos.append(PageBreak())
    
    elementos.append(Paragraph("ANÁLISIS DE RED", style_subtitulo))
    
    try:
        reporte_red = generar_reporte_red(caso_id)
        
        texto_red = f"""
        La red transaccional está compuesta por {reporte_red['densidad']['num_nodos']} nodos 
        y {reporte_red['densidad']['num_aristas']} aristas, con una densidad de 
        {reporte_red['densidad']['densidad']:.4f}. Se identificaron 
        {len(reporte_red['intermediarios'])} intermediarios clave en la red.
        """
        
        elementos.append(Paragraph(texto_red, style_normal))
        
        if reporte_red['intermediarios']:
            elementos.append(Spacer(1, 12))
            elementos.append(Paragraph("Principales Intermediarios:", style_subtitulo))
            
            datos_intermediarios = [['Documento', 'Score Intermediación', 'Entrada', 'Salida']]
            for inter in reporte_red['intermediarios'][:10]:
                datos_intermediarios.append([
                    inter['documento'][:20],
                    f"{inter['betweenness_score']:.4f}",
                    str(inter['grado_entrada']),
                    str(inter['grado_salida'])
                ])
            
            tabla_inter = Table(datos_intermediarios, colWidths=[2*inch, 1.5*inch, 1*inch, 1*inch])
            tabla_inter.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            elementos.append(tabla_inter)
    except Exception as e:
        elementos.append(Paragraph(f"Error en análisis de red: {str(e)}", style_normal))
    
    elementos.append(PageBreak())
    elementos.append(Paragraph("CONCLUSIONES Y RECOMENDACIONES", style_subtitulo))
    
    conclusiones = generar_conclusiones_automaticas(caso_id, personas, transacciones, tipologias)
    elementos.append(Paragraph(conclusiones, style_normal))
    
    doc.build(elementos)
    buffer.seek(0)
    return buffer

def generar_conclusiones_automaticas(caso_id, personas, transacciones, tipologias):
    conclusiones = []
    
    if len(tipologias) > 5:
        conclusiones.append(f"Se detectaron {len(tipologias)} alertas de tipologías, indicando un patrón complejo de operaciones sospechosas.")
    
    if len(transacciones) > 100:
        conclusiones.append(f"El alto volumen de transacciones ({len(transacciones)}) sugiere actividad transaccional intensiva.")
    
    tipologias_altas = [t for t in tipologias if t['nivel_riesgo'] >= 8]
    if tipologias_altas:
        conclusiones.append(f"Se identificaron {len(tipologias_altas)} tipologías de alto riesgo que requieren investigación prioritaria.")
    
    if not conclusiones:
        conclusiones.append("El análisis no ha identificado patrones altamente sospechosos en las transacciones revisadas.")
    
    return " ".join(conclusiones)

def exportar_transacciones_excel(caso_id):
    transacciones = obtener_transacciones_caso(caso_id)
    
    df = pd.DataFrame([dict(t) for t in transacciones])
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Transacciones', index=False)
    
    output.seek(0)
    return output

def exportar_tipologias_excel(caso_id):
    tipologias = obtener_tipologias_por_caso(caso_id)
    
    df = pd.DataFrame(tipologias)
    
    if 'evidencias' in df.columns:
        df['evidencias'] = df['evidencias'].apply(lambda x: json.dumps(x) if x else '')
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Tipologías', index=False)
    
    output.seek(0)
    return output

def generar_cronologia_transaccional(caso_id):
    with get_db() as db:
        query = """
            SELECT 
                t.fecha_operacion,
                t.hora_operacion,
                po.documento_encriptado as ordenante,
                pb.documento_encriptado as beneficiario,
                t.monto,
                t.descripcion_operacion_sbs,
                t.canal
            FROM transacciones t
            JOIN personas po ON t.ordenante_id = po.persona_id
            JOIN personas pb ON t.beneficiario_id = pb.persona_id
            JOIN casos_personas cp ON (
                t.ordenante_id = cp.persona_id OR 
                t.beneficiario_id = cp.persona_id
            )
            WHERE cp.caso_id = :caso_id
            ORDER BY t.fecha_operacion, t.hora_operacion
        """
        transacciones = db.execute(query, {'caso_id': caso_id}).fetchall()
    
    df = pd.DataFrame([dict(t) for t in transacciones])
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Cronología', index=False)
    
    output.seek(0)
    return output
