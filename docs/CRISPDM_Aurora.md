# 1 Business Understanding

Aurora Tickets es una plataforma de venta de entradas que present que esta experimentando un crecimiento descontrolado.Estos son los principales problemas:
- Alto tráfico web pero baja conversión
- Problemas de rendimiento (errores y latencia)
- Posible tráfico fraudulento o bots
- Falta de observabilidad y analítica avanzada

## Objetivos de negocio

- Mejorar la tasa de conversión
- Identificar eventos con alto interés pero bajo ingreso
- Detectar anomalías o comportamiento sospechoso
- Facilitar la toma de decisiones basada en datos

## KPIs

Para medir el éxito del proyecto, se han definido los siguientes indicadores que se 
visualizarán en el Dashboard de CloudWatch y en el análisis final:

- Conversion rate (sesiones → compra)
- Ingresos por evento
- Ratio interés vs compra
- Número de anomalías detectadas

## Decisiones

Para lograr estos objetivos, se desplegará una arquitectura  en AWS compuesta por:

- Un clúster de Apache Spark(1 Master, 3 Workers) para el procesamiento 
distribuido. 
- Un Data Lake en S3 organizado en capas (Raw, Curated, Analytics). 
- CloudWatch Logs para la ingesta de eventos en tiempo real. 
- RDS (MySQL) para el almacenamiento de los resultados finales. 


# 2 Data Understanding

## Fuente de datos
Los datos son generados por un simulador de tráfico que emula el comportamiento de 
usuarios reales y bots en la web de "Aurora Tickets".

### Clickstream
- Eventos de navegación (client-side)
- Logs del backend (server-side)
### Datos de negocio (CSV)
- **events.csv:** catálogo de eventos
- **campaigns.csv:** campañas marketing (UTM)
- **transactions.csv:** compras

## Estructura Logs

Cada evento de navegación sigue un formato JSON (o semi-estructurado en log) con los siguientes campos:

- **timestamp:** Fecha y hora del evento. 
- **user_id / session_id:** Identificadores únicos para rastrear el embudo de ventas. 
- **method:** Acción realizada (GET/POST). 
- **endpoint:** La página visitada (/home, /tickets, /checkout, 
/pago_confirmado). 
- **status_code:** Respuesta del servidor (200 OK, 404 Not Found, 500 Error). 
- **ticket_type:** Categoría de la entrada si el evento es una selección (A, B o C). 
- **ip_address:** Vital para identificar patrones de bots (muchas peticiones desde una 
sola IP). 
- **user_agent:** Navegador/dispositivo (ayuda a distinguir humanos de scripts).

## Volumen y temporalidad
- ≥ 200.000 eventos
- Simulación de 7 días
- Mezcla de generación masiva + tráfico real

# Data Preparation

## Limpieza de datos
- Eliminación o imputación de nulos
- Corrección de tipos (fechas, numéricos)
- Eliminación de registros inconsistentes
- Filtrado de outliers en ingresos
- Resolución de claves huérfanas

## Transformaciones
- Normalización de esquemas
- Enriquecimiento de clickstream con datos de negocio (joins)
- Generación de métricas intermedias

## Data Lake S3
Estructura:
- raw/ → datos originales
- curated/ → datos limpios y estructurados
- analytics/ → datos agregados finales

## Formato y particionado
- Formato: Parquet
- Particionado por: dt=YYYY-MM-DD

# Modeling

## Arquitectura

- Cluster Spark Standalone (1 master + 3 workers)
- Nodo submit para ejecución de jobs
- Ingesta desde CloudWatch → S3

## Job 1

- Entrada: raw/
- Procesos:
  - Limpieza
  - Normalización
  - Join entre datasets
- Salida: curated/ (Parquet particionado)


## Job 2
- Entrada: curated/
- Procesos:
    - Agregaciones
    - Cálculo de métricas
- Salidas:
    - analytics/ en S3
    - Tablas finales en RDS MySQ

## Productos analíticos

 - Funnel
    - Métricas por día:

        - sesiones totales
        - paso por etapas (list → detail → checkout → purchase)
        - tasa de conversión

- Ranking de eventos
    - Métricas:
        - visitas a detalle
        - compras
        - ingresos
        - ratio interés/compra

- Detección de anomalías
    - Basado en:
        - número de requests por IP
        - errores
        - patrones inusuales
    - Regla:
        - IP con requests > umbral → anomalía

 ## Visaulizacion 

CloudWatch Dashboard con:
- Funnel
- Top eventos
- Errores/latencia
- Anomalías

# Evaluacion

## Limitaciones
- Datos simulados (no reales)
- Reglas de anomalía simples
- Pérdida de información en limpieza

# Deployment

## Arquitectura AWS
- EC2 (6 instancias):
    - 1 Master
    - 3 Workers
    - 1 Submit
    - 1 Web + CloudWatch Agent
- S3 → Data Lake
- CloudWatch → logs + dashboard
- RDS MySQL → métricas finales

## Reproducibilidad

## Estimacion de costes