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
    TIMEFRAME: str = '1d' # Solo relevante para modo 'historical'
    
    # Calcular la fecha de inicio y fin para un período de un mes
    _END_DATE_DT: datetime = datetime.now()
    _START_DATE_DT: datetime = _END_DATE_DT - timedelta(days=30) # Un mes atrás
    
    START_DATE: str = _START_DATE_DT.strftime('%Y-%m-%d') # Solo relevante para modo 'historical'
    END_DATE: str = _END_DATE_DT.strftime('%Y-%m-%d')     # Solo relevante para modo 'historical'

    SYMBOLS_TO_FETCH: List[str] = ['BTC/USDT', 'SOL/USDT'] # Símbolos para ambos modos

    MAX_RETRIES: int = 3
    INITIAL_BACKOFF: int = 2
    EXCHANGE_OPTIONS: Dict[str, Any] = {} # Opciones adicionales para ccxt, ej: {'rateLimit': 1500}

    OUTPUT_FOLDER: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'datos_historicos')
    SAVE_FORMAT: Literal['csv', 'parquet'] = 'csv' # Formato de guardado para el DataFrame final

    # Nuevo: Modo de recolección de datos. Opciones: 'historical', 'realtime'
    DATA_COLLECTION_MODE: Literal['historical', 'realtime'] = 'historical' 
    
    # Nuevo: Tipos de datos a descargar en modo 'historical'. Opciones: 'ohlcv', 'ticker', 'orderbook'
    # Puedes elegir una combinación, por ejemplo: ['ohlcv'] o ['ticker', 'orderbook'] o ['ohlcv', 'ticker', 'orderbook']
    HISTORICAL_DATA_TYPES_TO_FETCH: List[str] = ['ohlcv', 'ticker', 'orderbook']

    # Nuevo: Duración de la recolección en tiempo real en segundos. Solo relevante para modo 'realtime'.
    REALTIME_COLLECTION_DURATION_SECONDS: int = 60 # Recolectar datos en tiempo real por 60 segundos

    # Nuevo: Tipos de datos a descargar en modo 'realtime'. Opciones: 'trades', 'orderbook' (para websockets)
    REALTIME_DATA_TYPES_TO_FETCH: List[str] = ['trades'] # Puedes añadir 'orderbook' si el exchange lo soporta via watch_order_book
    
    @classmethod
    def ensure_output_folder(cls) -> None:
        """
        Crea el directorio de salida si no existe.
        """
        if not os.path.exists(cls.OUTPUT_FOLDER):
            os.makedirs(cls.OUTPUT_FOLDER)
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
        """Genera el nombre de archivo estandarizado para datos de ticker (snapshot REST)."""
        sanitized_symbol = symbol.replace('/', '_')
        return os.path.join(
            cls.OUTPUT_FOLDER,
            f"{cls.EXCHANGE_ID}_{sanitized_symbol}_ticker_snapshot.json"
        )

    @classmethod
    def _get_orderbook_filename(cls, symbol: str) -> str:
        """Genera el nombre de archivo estandarizado para datos de order book (snapshot REST)."""
        sanitized_symbol = symbol.replace('/', '_')
        return os.path.join(
            cls.OUTPUT_FOLDER,
            f"{cls.EXCHANGE_ID}_{sanitized_symbol}_orderbook_snapshot.json"
        )

    @classmethod
    def _get_realtime_trades_filename(cls, symbol: str) -> str:
        """Genera el nombre de archivo estandarizado para trades en tiempo real."""
        sanitized_symbol = symbol.replace('/', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return os.path.join(
            cls.OUTPUT_FOLDER,
            f"{cls.EXCHANGE_ID}_{sanitized_symbol}_trades_realtime_{timestamp}.jsonl"
        )

    @classmethod
    def _get_realtime_orderbook_filename(cls, symbol: str) -> str:
        """Genera el nombre de archivo estandarizado para order book en tiempo real."""
        sanitized_symbol = symbol.replace('/', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return os.path.join(
            cls.OUTPUT_FOLDER,
            f"{cls.EXCHANGE_ID}_{sanitized_symbol}_orderbook_realtime_{timestamp}.jsonl"
        )
