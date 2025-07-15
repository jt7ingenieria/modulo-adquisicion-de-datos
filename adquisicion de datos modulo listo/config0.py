import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Literal

# Configuración centralizada para el fetcher de datos
class Config:
    """
    Clase para gestionar la configuración centralizada del fetcher de datos.
    Modifica estos atributos para cambiar el comportamiento del programa.
    """
    EXCHANGE_ID: str = 'okx'
    TIMEFRAME: str = '1d'
    
    # Calcular la fecha de inicio y fin para un período de un mes
    # Se recomienda no modificar directamente START_DATE y END_DATE aquí,
    # sino ajustar DAYS_AGO si se desea un período diferente.
    _END_DATE_DT: datetime = datetime.now()
    _START_DATE_DT: datetime = _END_DATE_DT - timedelta(days=30) # Un mes atrás
    
    START_DATE: str = _START_DATE_DT.strftime('%Y-%m-%d')
    END_DATE: str = _END_DATE_DT.strftime('%Y-%m-%d')

    SYMBOLS_TO_FETCH: List[str] = ['BTC/USDT', 'SOL/USDT'] # BTC y SOL

    MAX_RETRIES: int = 3
    INITIAL_BACKOFF: int = 2
    EXCHANGE_OPTIONS: Dict[str, Any] = {} # Opciones adicionales para ccxt, ej: {'rateLimit': 1500}

    # CORRECCIÓN: Hacer que OUTPUT_FOLDER sea una ruta absoluta basada en la ubicación de este archivo.
    OUTPUT_FOLDER: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'datos_historicos')
    SAVE_FORMAT: Literal['csv', 'parquet'] = 'csv' # Formato de guardado para el DataFrame final

    # Tipos de datos a descargar. Opciones: 'ohlcv', 'ticker', 'orderbook'
    # Puedes elegir una combinación, por ejemplo: ['ohlcv'] o ['ticker', 'orderbook'] o ['ohlcv', 'ticker', 'orderbook']
    DATA_TYPES_TO_FETCH: List[str] = ['ohlcv', 'ticker', 'orderbook']
    
    @classmethod
    def ensure_output_folder(cls) -> None:
        """
        Crea el directorio de salida si no existe.
        """
        if not os.path.exists(cls.OUTPUT_FOLDER):
            os.makedirs(cls.OUTPUT_FOLDER)
            # No usar logger aquí para evitar dependencia circular de logging en config.py si se importa temprano
            print(f"Directorio de salida creado: {cls.OUTPUT_FOLDER}")

    @classmethod
    def _get_ohlcv_filename(cls, symbol: str, timeframe: str, start_date: str, end_date: str, file_format: str) -> str:
        """Genera el nombre de archivo estandarizado para datos OHLCV finales."""
        sanitized_symbol = symbol.replace('/', '_')
        return os.path.join(
            cls.OUTPUT_FOLDER,
            f"{cls.EXCHANGE_ID}_{sanitized_symbol}_{timeframe}_ohlcv_{start_date}_to_{end_date}.{file_format}"
        )

    @classmethod
    def _get_ticker_filename(cls, symbol: str) -> str:
        """Genera el nombre de archivo estandarizado para datos de ticker."""
        sanitized_symbol = symbol.replace('/', '_')
        return os.path.join(
            cls.OUTPUT_FOLDER,
            f"{cls.EXCHANGE_ID}_{sanitized_symbol}_ticker.json"
        )

    @classmethod
    def _get_orderbook_filename(cls, symbol: str) -> str:
        """Genera el nombre de archivo estandarizado para datos de order book."""
        sanitized_symbol = symbol.replace('/', '_')
        return os.path.join(
            cls.OUTPUT_FOLDER,
            f"{cls.EXCHANGE_ID}_{sanitized_symbol}_orderbook.json"
        )

