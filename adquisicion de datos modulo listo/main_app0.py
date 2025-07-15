import asyncio
import logging
import os
import pandas as pd
import json
import time

# Importar la configuración centralizada y el módulo de descarga
from config import Config
from data_fetcher import AsyncCryptoDataFetcher

# Configurar el logger (asegurarse de que la configuración de logging esté en un solo lugar)
# CORRECCIÓN: Se ha corregido el error de sintaxis en la cadena de formato.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main() -> None:
    """
    Función principal para demostrar la descarga concurrente de datos.
    """
    logger.info("--- Iniciando Descarga Concurrente de Datos ---")
    logger.info(f"Exchange: {Config.EXCHANGE_ID}, Timeframe: {Config.TIMEFRAME}, Período: {Config.START_DATE} a {Config.END_DATE}")
    
    start_time: float = time.time()

    # Asegurarse de que la carpeta de salida exista
    Config.ensure_output_folder()
    logger.info(f"Carpeta de salida '{Config.OUTPUT_FOLDER}' asegurada.")

    try:
        fetcher: AsyncCryptoDataFetcher = AsyncCryptoDataFetcher(exchange_id=Config.EXCHANGE_ID, exchange_options=Config.EXCHANGE_OPTIONS)
        
        # --- Obtención de Datos Históricos (OHLCV) ---
        if 'ohlcv' in Config.DATA_TYPES_TO_FETCH:
            ohlcv_results: List[pd.DataFrame] = []
            for symbol in Config.SYMBOLS_TO_FETCH:
                file_extension = 'csv' if Config.SAVE_FORMAT == 'csv' else 'parquet'
                ohlcv_final_path = Config._get_ohlcv_filename(
                    symbol, Config.TIMEFRAME, Config.START_DATE, Config.END_DATE, file_extension
                )
                
                if os.path.exists(ohlcv_final_path):
                    logger.info(f"[{symbol}] Datos OHLCV ya existen en '{ohlcv_final_path}'. Cargando desde disco.")
                    if file_extension == 'csv':
                        data_df = pd.read_csv(ohlcv_final_path, header=None, names=['timestamp','open','high','low','close','volume'], parse_dates=['timestamp'])
                        data_df.set_index('timestamp', inplace=True)
                    else: # parquet
                        data_df = pd.read_parquet(ohlcv_final_path)
                    ohlcv_results.append(data_df)
                else:
                    logger.info(f"[{symbol}] Datos OHLCV no encontrados, iniciando descarga.")
                    incremental_filename: str = os.path.join(
                        Config.OUTPUT_FOLDER,
                        f"{Config.EXCHANGE_ID}_{symbol.replace('/', '_')}_{Config.TIMEFRAME}_incremental.csv"
                    )
                    # Eliminar el archivo incremental si ya existe para asegurar un inicio limpio
                    if os.path.exists(incremental_filename):
                        os.remove(incremental_filename)
                        logger.info(f"Archivo incremental existente '{incremental_filename}' eliminado.")

                    data_df = await fetcher.fetch_historical_data(
                        symbol,
                        Config.TIMEFRAME,
                        Config.START_DATE,
                        end_date_str=Config.END_DATE,
                        max_retries=Config.MAX_RETRIES,
                        initial_backoff=Config.INITIAL_BACKOFF,
                        incremental_save_path=incremental_filename
                    )
                    
                    if not data_df.empty:
                        logger.info(f"\n--- Resumen para {symbol} (DataFrame OHLCV original) ---")
                        logger.info(data_df.head())
                        logger.info(f"NaNs antes de rellenar: {data_df.isnull().sum().sum()}")

                        # --- Manejo de Datos Faltantes ---
                        data_df_filled: pd.DataFrame = fetcher.fill_missing_data(data_df, Config.TIMEFRAME, method='ffill')
                        logger.info(f"\n--- Resumen para {symbol} (DataFrame OHLCV con datos rellenados) ---")
                        logger.info(data_df_filled.head())
                        logger.info(f"NaNs después de rellenar: {data_df_filled.isnull().sum().sum()}")
                        
                        # --- Guardar el DataFrame Final en el formato configurado ---
                        fetcher.save_dataframe(data_df_filled, ohlcv_final_path, format=Config.SAVE_FORMAT)
                        logger.info(f"Datos OHLCV de {symbol} (DataFrame final) guardados en '{ohlcv_final_path}'")
                        ohlcv_results.append(data_df_filled)
                    else:
                        logger.warning(f"\nNo se pudieron obtener datos OHLCV para {symbol}.")
                        ohlcv_results.append(pd.DataFrame()) # Añadir un DataFrame vacío para mantener la correspondencia
        else:
            logger.info("Descarga de datos OHLCV omitida según la configuración.")

        # --- Obtención de Otros Tipos de Datos (Ticker y Order Book) ---
        if 'ticker' in Config.DATA_TYPES_TO_FETCH or 'orderbook' in Config.DATA_TYPES_TO_FETCH:
            logger.info("\n--- Demostración de Obtención y Guardado de Otros Tipos de Datos ---")
            for symbol in Config.SYMBOLS_TO_FETCH:
                # Obtener Ticker
                if 'ticker' in Config.DATA_TYPES_TO_FETCH:
                    ticker_filename = Config._get_ticker_filename(symbol)
                    ticker: Optional[Dict[str, Any]] = None
                    if os.path.exists(ticker_filename):
                        logger.info(f"[{symbol}] Ticker ya existe en '{ticker_filename}'. Cargando desde disco.")
                        with open(ticker_filename, 'r') as f:
                            ticker = json.load(f)
                    else:
                        logger.info(f"[{symbol}] Ticker no encontrado, iniciando descarga.")
                        ticker = await fetcher.fetch_ticker(symbol)
                        if ticker:
                            with open(ticker_filename, 'w') as f:
                                json.dump(ticker, f, indent=4) # Guardar con indentación para legibilidad
                            logger.info(f"Ticker de {symbol} guardado en '{ticker_filename}'")
                    
                    if ticker:
                        logger.info(f"[{symbol}] Último precio del ticker: {ticker.get('last')}")
                    else:
                        logger.warning(f"[{symbol}] No se pudo obtener el ticker.")
                else:
                    logger.info(f"[{symbol}] Descarga de Ticker omitida según la configuración.")
                
                # Obtener Order Book
                if 'orderbook' in Config.DATA_TYPES_TO_FETCH:
                    order_book_filename = Config._get_orderbook_filename(symbol)
                    order_book: Optional[Dict[str, Any]] = None
                    if os.path.exists(order_book_filename):
                        logger.info(f"[{symbol}] Libro de órdenes ya existe en '{order_book_filename}'. Cargando desde disco.")
                        with open(order_book_filename, 'r') as f:
                            order_book = json.load(f)
                    else:
                        logger.info(f"[{symbol}] Libro de órdenes no encontrado, iniciando descarga.")
                        order_book = await fetcher.fetch_order_book(symbol, limit=10) # Obtener las 10 mejores bids/asks
                        if order_book:
                            with open(order_book_filename, 'w') as f:
                                json.dump(order_book, f, indent=4) # Guardar con indentación para legibilidad
                            logger.info(f"Libro de órdenes de {symbol} guardado en '{order_book_filename}'")
                    
                    if order_book:
                        logger.info(f"[{symbol}] Mejor oferta (bid): {order_book['bids'][0][0]} / Mejor demanda (ask): {order_book['asks'][0][0]}")
                    else:
                        logger.warning(f"[{symbol}] No se pudo obtener el libro de órdenes.")
                else:
                    logger.info(f"[{symbol}] Descarga de Libro de Órdenes omitida según la configuración.")
        else:
            logger.info("Descarga de Ticker y Libro de Órdenes omitida según la configuración.")


    except ValueError as e:
        logger.error(f"Error de configuración: {e}")
    except Exception as e:
        logger.error(f"Ocurrió un error en la ejecución principal: {e}")

    end_time: float = time.time()
    logger.info(f"\n--- Tiempo total de ejecución: {end_time - start_time:.2f} segundos ---")


if __name__ == "__main__":
    asyncio.run(main())
