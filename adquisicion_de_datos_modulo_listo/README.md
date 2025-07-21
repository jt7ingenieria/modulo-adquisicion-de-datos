# Módulo de Adquisición de Datos de Criptomonedas

## Descripción
Este módulo permite la descarga asíncrona de datos históricos OHLCV (Open-High-Low-Close-Volume) de criptomonedas desde el exchange OKX.

## Características principales
- Descarga concurrente de datos para múltiples pares (BTC/USDT, SOL/USDT)
- Manejo robusto de errores con reintentos exponenciales
- Guardado incremental en archivos CSV durante la descarga
- Filtrado por rangos de fechas
- Conversión automática de zonas horarias (UTC)

## Requerimientos
- Python 3.13
- Entorno virtual activado (venv)

## Dependencias
- ccxt
- pandas
- pytz

## Configuración
1. Activar el entorno virtual:
```
cd c:\Users\javie\modulos bot
.\venv\Scripts\activate
```

2. Instalar dependencias:
```
pip install -r requirements.txt
```

## Uso
Ejecutar el script principal:
```
python asynccriptodatafectheralamcenamiento.py
```

Los datos se guardarán en archivos CSV con el formato:
- `okx_[PAR]_1d_incremental.csv` (datos durante la descarga)
- `okx_[PAR]_1d_async_to_[FECHA]_final.csv` (dataset completo)

## Nuevo Modo
La aplicación ahora funciona con un nuevo modo de datos en tiempo real, que permite la adquisición y procesamiento continuo de datos para mejorar la eficiencia y precisión de las operaciones.