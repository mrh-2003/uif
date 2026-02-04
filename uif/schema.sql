DROP TABLE IF EXISTS tipologias_detectadas CASCADE;
DROP TABLE IF EXISTS casos_personas CASCADE;
DROP TABLE IF EXISTS transacciones CASCADE;
DROP TABLE IF EXISTS personas CASCADE;
DROP TABLE IF EXISTS registros_operaciones CASCADE;
DROP TABLE IF EXISTS casos CASCADE;
DROP TABLE IF EXISTS catalogos_tipologias CASCADE;

CREATE TABLE casos (
    caso_id SERIAL PRIMARY KEY,
    nombre_caso VARCHAR(200) NOT NULL,
    descripcion TEXT,
    estado VARCHAR(50) DEFAULT 'ACTIVO',
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario_creador VARCHAR(100),
    prioridad VARCHAR(50),
    tipo_caso VARCHAR(100)
);

CREATE TABLE registros_operaciones (
    ro_id SERIAL PRIMARY KEY,
    nombre_archivo VARCHAR(300),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_registros INTEGER,
    registros_validos INTEGER,
    registros_descartados INTEGER,
    usuario_carga VARCHAR(100),
    estado_procesamiento VARCHAR(50),
    observaciones TEXT
);

CREATE TABLE personas (
    persona_id SERIAL PRIMARY KEY,
    tipo_persona VARCHAR(20),
    tipo_documento VARCHAR(20),
    documento_encriptado VARCHAR(100) UNIQUE NOT NULL,
    ciiu_ocupacion VARCHAR(50),
    descripcion_ocupacion VARCHAR(200),
    departamento VARCHAR(50),
    provincia VARCHAR(50),
    distrito VARCHAR(50),
    codigo_ubigeo VARCHAR(50),
    fecha_primera_operacion DATE,
    fecha_ultima_operacion DATE,
    total_operaciones INTEGER DEFAULT 0,
    monto_total NUMERIC(20,2) DEFAULT 0,
    monto_promedio NUMERIC(20,2) DEFAULT 0,
    es_ordenante BOOLEAN DEFAULT FALSE,
    es_beneficiario BOOLEAN DEFAULT FALSE,
    es_ejecutante BOOLEAN DEFAULT FALSE
);

CREATE TABLE transacciones (
    transaccion_id SERIAL PRIMARY KEY,
    ro_id INTEGER REFERENCES registros_operaciones(ro_id),
    busqueda VARCHAR(100),
    flag_tipo_cli_busqueda VARCHAR(20),
    tipo_clasificacion_relacionado VARCHAR(150),
    num_registro_interno VARCHAR(50),
    canal VARCHAR(150),
    codigo_ubigeo VARCHAR(50),
    fecha_operacion DATE NOT NULL,
    hora_operacion VARCHAR(20),
    
    ejecutante_id INTEGER REFERENCES personas(persona_id),
    tipo_ejecutante VARCHAR(20),
    tipo_doc_ejecutante VARCHAR(20),
    doc_ejecutante_encriptado VARCHAR(100),
    
    ordenante_id INTEGER REFERENCES personas(persona_id),
    tipo_ordenante VARCHAR(20),
    tipo_doc_ordenante VARCHAR(20),
    doc_ordenante_encriptado VARCHAR(100),
    ciiu_ordenante VARCHAR(50),
    ocupacion_ordenante VARCHAR(200),
    dep_ordenante VARCHAR(50),
    prov_ordenante VARCHAR(50),
    dist_ordenante VARCHAR(50),
    cuenta_ordenante VARCHAR(100),
    
    beneficiario_id INTEGER REFERENCES personas(persona_id),
    tipo_beneficiario VARCHAR(20),
    tipo_doc_beneficiario VARCHAR(20),
    doc_beneficiario_encriptado VARCHAR(100),
    ciiu_beneficiario VARCHAR(50),
    ocupacion_beneficiario VARCHAR(200),
    dep_beneficiario VARCHAR(50),
    prov_beneficiario VARCHAR(50),
    dist_beneficiario VARCHAR(50),
    cuenta_beneficiario VARCHAR(100),
    
    tipo_operacion_sbs INTEGER,
    descripcion_operacion_sbs VARCHAR(250),
    origen_dinero VARCHAR(300),
    codigo_moneda VARCHAR(20),
    nombre_moneda VARCHAR(100),
    monto NUMERIC(20,2) NOT NULL,
    
    es_sospechosa BOOLEAN DEFAULT FALSE,
    nivel_riesgo INTEGER DEFAULT 0,
    observaciones TEXT
);

CREATE TABLE casos_personas (
    caso_persona_id SERIAL PRIMARY KEY,
    caso_id INTEGER REFERENCES casos(caso_id) ON DELETE CASCADE,
    persona_id INTEGER REFERENCES personas(persona_id),
    rol_en_caso VARCHAR(100),
    fecha_inclusion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    motivo_inclusion TEXT,
    UNIQUE(caso_id, persona_id)
);

CREATE TABLE catalogos_tipologias (
    tipologia_id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    nombre VARCHAR(200) NOT NULL,
    descripcion TEXT,
    categoria VARCHAR(100),
    nivel_riesgo INTEGER,
    parametros JSONB,
    activo BOOLEAN DEFAULT TRUE
);

CREATE TABLE tipologias_detectadas (
    deteccion_id SERIAL PRIMARY KEY,
    caso_id INTEGER REFERENCES casos(caso_id),
    tipologia_id INTEGER REFERENCES catalogos_tipologias(tipologia_id),
    persona_id INTEGER,
    fecha_deteccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nivel_confianza NUMERIC(5,2),
    evidencias JSONB,
    transacciones_relacionadas INTEGER[],
    observaciones TEXT,
    estado VARCHAR(50) DEFAULT 'PENDIENTE'
);

CREATE INDEX idx_transacciones_fecha ON transacciones(fecha_operacion);
CREATE INDEX idx_transacciones_ordenante ON transacciones(ordenante_id);
CREATE INDEX idx_transacciones_beneficiario ON transacciones(beneficiario_id);
CREATE INDEX idx_transacciones_ejecutante ON transacciones(ejecutante_id);
CREATE INDEX idx_transacciones_monto ON transacciones(monto);
CREATE INDEX idx_transacciones_ro ON transacciones(ro_id);
CREATE INDEX idx_personas_documento ON personas(documento_encriptado);
CREATE INDEX idx_casos_personas_caso ON casos_personas(caso_id);
CREATE INDEX idx_casos_personas_persona ON casos_personas(persona_id);
CREATE INDEX idx_tipologias_caso ON tipologias_detectadas(caso_id);
CREATE INDEX idx_tipologias_persona ON tipologias_detectadas(persona_id);

INSERT INTO catalogos_tipologias (codigo, nombre, descripcion, categoria, nivel_riesgo, parametros) VALUES
('TIP001', 'Pitufeo', 'Múltiples transacciones bajo umbral de reporte', 'ESTRUCTURACION', 8, '{"umbral_monto": 10000, "min_operaciones": 5, "ventana_dias": 30}'),
('TIP002', 'Concentración de beneficiarios', 'Múltiples ordenantes hacia un beneficiario', 'CONCENTRACION', 7, '{"min_ordenantes": 5, "ventana_dias": 30}'),
('TIP003', 'Concentración de ordenantes', 'Un ordenante hacia múltiples beneficiarios', 'DISPERSION', 6, '{"min_beneficiarios": 10, "ventana_dias": 30}'),
('TIP004', 'Transacciones circulares', 'Fondos que regresan al origen', 'CIRCULARIDAD', 9, '{"max_saltos": 5}'),
('TIP005', 'Montos similares repetidos', 'Transacciones con montos casi idénticos', 'PATRON', 7, '{"tolerancia_porcentual": 5, "min_repeticiones": 3}'),
('TIP006', 'Transacciones en ventana corta', 'Múltiples operaciones en minutos/horas', 'VELOCIDAD', 8, '{"ventana_horas": 2, "min_operaciones": 5}'),
('TIP007', 'Cadenas de transferencia', 'Transacciones encadenadas A→B→C→D', 'LAYERING', 9, '{"min_eslabones": 3}'),
('TIP008', 'Transferencias inmediatas', 'Fondos transferidos al instante de recibidos', 'PASS_THROUGH', 8, '{"ventana_minutos": 30}'),
('TIP009', 'Frecuencia inusual', 'Incremento súbito en volumen operativo', 'ANOMALIA', 6, '{"factor_incremento": 3}'),
('TIP010', 'Montos redondos', 'Transacciones en cifras exactas', 'PATRON', 5, '{"min_operaciones": 5}');
