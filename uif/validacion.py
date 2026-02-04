import pandas as pd
import numpy as np
from datetime import datetime

class ValidadorRO:
    
    def __init__(self, df):
        self.df = df
        self.errores = []
        self.advertencias = []
    
    def validar_estructura(self, columnas_requeridas):
        columnas_faltantes = set(columnas_requeridas) - set(self.df.columns)
        if columnas_faltantes:
            self.errores.append(f"Columnas faltantes: {columnas_faltantes}")
            return False
        return True
    
    def validar_tipos_datos(self):
        if 'mtotrx' in self.df.columns:
            try:
                pd.to_numeric(self.df['mtotrx'], errors='raise')
            except:
                self.errores.append("Columna 'mtotrx' contiene valores no numéricos")
        
        if 'fec_operacion' in self.df.columns:
            try:
                pd.to_datetime(self.df['fec_operacion'], errors='raise')
            except:
                self.errores.append("Columna 'fec_operacion' contiene fechas inválidas")
        
        return len(self.errores) == 0
    
    def validar_valores_nulos(self):
        campos_criticos = [
            'fec_operacion', 'mtotrx',
            'doc_ordenante_encriptado', 'doc_beneficiario_encriptado'
        ]
        
        for campo in campos_criticos:
            if campo in self.df.columns:
                nulos = self.df[campo].isnull().sum()
                if nulos > 0:
                    self.advertencias.append(f"{campo}: {nulos} valores nulos ({nulos/len(self.df)*100:.1f}%)")
    
    def validar_montos(self):
        if 'mtotrx' in self.df.columns:
            montos = pd.to_numeric(self.df['mtotrx'], errors='coerce')
            
            negativos = (montos < 0).sum()
            if negativos > 0:
                self.errores.append(f"{negativos} transacciones con montos negativos")
            
            ceros = (montos == 0).sum()
            if ceros > 0:
                self.advertencias.append(f"{ceros} transacciones con monto cero")
            
            muy_bajos = (montos < 10).sum()
            if muy_bajos > 0:
                self.advertencias.append(f"{muy_bajos} transacciones con montos menores a 10")
    
    def validar_fechas(self):
        if 'fec_operacion' in self.df.columns:
            fechas = pd.to_datetime(self.df['fec_operacion'], errors='coerce')
            
            futuras = (fechas > datetime.now()).sum()
            if futuras > 0:
                self.advertencias.append(f"{futuras} transacciones con fechas futuras")
            
            muy_antiguas = (fechas < datetime(2000, 1, 1)).sum()
            if muy_antiguas > 0:
                self.advertencias.append(f"{muy_antiguas} transacciones anteriores al año 2000")
    
    def validar_duplicados(self):
        if 'num_registro_interno' in self.df.columns:
            duplicados = self.df['num_registro_interno'].duplicated().sum()
            if duplicados > 0:
                self.advertencias.append(f"{duplicados} registros internos duplicados")
    
    def ejecutar_validacion_completa(self, columnas_requeridas):
        self.validar_estructura(columnas_requeridas)
        self.validar_tipos_datos()
        self.validar_valores_nulos()
        self.validar_montos()
        self.validar_fechas()
        self.validar_duplicados()
        
        return {
            'valido': len(self.errores) == 0,
            'errores': self.errores,
            'advertencias': self.advertencias
        }
    
    def generar_reporte_calidad(self):
        reporte = {
            'total_registros': len(self.df),
            'columnas': len(self.df.columns),
            'memoria_mb': self.df.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        for col in self.df.columns:
            nulos = self.df[col].isnull().sum()
            if nulos > 0:
                reporte[f'nulos_{col}'] = nulos
        
        return reporte
