# Módulo de Adquisición de Datos de Criptomonedas

## Descripción
Este módulo permite la adquisición de datos históricos de criptomonedas desde diferentes exchanges utilizando la biblioteca CCXT. Implementa técnicas asíncronas para mejorar el rendimiento y permite almacenar los datos en diferentes formatos para su posterior análisis.

## Estructura del Proyecto
```
modulo adquisicion de datos version1.0/
├── README.md                          # Este archivo
├── adquisicion de datos/              # Código principal
│   ├── config.py                      # Configuración del módulo
│   ├── data_fetcher.py                # Lógica de obtención de datos
│   ├── main_app.py                    # Script principal de ejecución
│   └── datos_historicos/              # Carpeta donde se almacenan los datos
├── config0.py                         # Versión anterior de configuración
├── data_fetcher0.py                   # Versión anterior de obtención de datos
└── main_app0.py                       # Versión anterior del script principal
```

## Requisitos
- Python 3.13
- Bibliotecas principales:
  - ccxt
  - pandas
  - pytz
  - asyncio

## Instalación
```bash
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual (Windows)
venv\Scripts\activate

# Instalar dependencias
pip install ccxt pandas pytz
```

## Uso
```powershell
# Activar entorno virtual
.\venv\Scripts\Activate.ps1

# Ejecutar el módulo principal
python "adquisicion de datos\main_app.py"
```

## Configuración
Modifique el archivo `config.py` para:
- Cambiar el exchange (ej: 'okx', 'binance')
- Modificar los símbolos a monitorear
- Ajustar los formatos de salida
- Configurar los intervalos temporales

## Características
- Obtención asíncrona de datos OHLCV (Open, High, Low, Close, Volume)
- Soporte para múltiples exchanges a través de CCXT
- Almacenamiento en formato CSV y JSON
- Manejo de errores y reintentos
- Logging detallado de operaciones