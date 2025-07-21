import asyncio
import logging
import os
import pandas as pd
import json
import time
from typing import List, Dict, Any, Optional

# Importar la configuración centralizada y los módulos de descarga y procesamiento
from config import Config
from data_fetcher import AsyncCryptoDataFetcher
from data_processor import DataProcessor

# Configurar el logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import argparse

async def main() -> None:
    """
    Función principal para demostrar la descarga concurrente de datos,
    incluyendo modos histórico y en tiempo real, y procesamiento de datos.
    """
    logger.info("--- Iniciando Aplicación de Recolección y Procesamiento de Datos ---")
    logger.info(f"Modo de Recolección: {Config.DATA_COLLECTION_MODE}")
    logger.info(f"Exchange: {Config.EXCHANGE_ID}, Símbolos: {Config.SYMBOLS_TO_FETCH}")
    
    start_time: float = time.time()

    # Asegurarse de que la carpeta de salida exista
    Config.ensure_output_folder()
    logger.info(f"Carpeta de salida '{Config.OUTPUT_FOLDER}' asegurada.")

    try:
        fetcher: AsyncCryptoDataFetcher = AsyncCryptoDataFetcher(
            exchange_id=Config.EXCHANGE_ID,
            exchange_options=Config.EXCHANGE_OPTIONS
        )
        
        # --- Lógica para el modo de recolección de datos ---
        if Config.DATA_COLLECTION_MODE == 'historical':
            logger.info(f"\n--- Modo Histórico: Período {Config.START_DATE} a {Config.END_DATE} ---")
            
            # --- Obtención y Procesamiento de Datos Históricos (OHLCV) ---
            if 'ohlcv' in Config.HISTORICAL_DATA_TYPES_TO_FETCH: # <--- Usando HISTORICAL_DATA_TYPES_TO_FETCH
                logger.info("\n--- Procesando Datos OHLCV Históricos ---")
                data_processor: Optional[DataProcessor] = None
                if Config.APPLY_NORMALIZATION:
                    data_processor = DataProcessor(method=Config.SCALING_METHOD)
                    # Intentar cargar el escalador si existe
                    if os.path.exists(Config.SCALER_SAVE_PATH):
                        try:
                            data_processor.load_scaler(Config.SCALER_SAVE_PATH)
                            logger.info(f"Escalador cargado desde '{Config.SCALER_SAVE_PATH}'.")
                        except Exception as e:
                            logger.warning(f"No se pudo cargar el escalador existente: {e}. Se entrenará uno nuevo.")
                            data_processor = DataProcessor(method=Config.SCALING_METHOD) # Re-inicializar si falla la carga

                for symbol in Config.SYMBOLS_TO_FETCH:
                    file_extension = Config.SAVE_FORMAT
                    ohlcv_final_path = Config._get_ohlcv_filename(
                        symbol, Config.TIMEFRAME, Config.START_DATE, Config.END_DATE, file_extension
                    )
                    ohlcv_scaled_path = Config._get_ohlcv_filename(
                        symbol, Config.TIMEFRAME, Config.START_DATE, Config.END_DATE, file_extension, suffix="_scaled"
                    )

                    data_df: pd.DataFrame = pd.DataFrame()

                    # Cargar desde disco si ya existe
                    if os.path.exists(ohlcv_final_path) and not Config.APPLY_NORMALIZATION:
                        logger.info(f"[{symbol}] Datos OHLCV ya existen en '{ohlcv_final_path}'. Cargando desde disco.")
                        if file_extension == 'csv':
                            data_df = pd.read_csv(ohlcv_final_path, index_col='timestamp', parse_dates=True)
                        else: # parquet
                            data_df = pd.read_parquet(ohlcv_final_path)
                    elif os.path.exists(ohlcv_scaled_path) and Config.APPLY_NORMALIZATION and Config.SAVE_NORMALIZED_ONLY_IF_APPLIED:
                         logger.info(f"[{symbol}] Datos OHLCV escalados ya existen en '{ohlcv_scaled_path}'. Cargando desde disco.")
                         if file_extension == 'csv':
                            data_df = pd.read_csv(ohlcv_scaled_path, index_col='timestamp', parse_dates=True)
                         else: # parquet
                            data_df = pd.read_parquet(ohlcv_scaled_path)
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
                        logger.info(f"\n--- Resumen para {symbol} (DataFrame OHLCV original/descargado) ---")
                        logger.info(data_df.head())
                        logger.info(f"NaNs antes de rellenar: {data_df.isnull().sum().sum()}")

                        # --- Manejo de Datos Faltantes ---
                        data_df_filled: pd.DataFrame = fetcher.fill_missing_data(data_df, Config.TIMEFRAME, method='ffill')
                        logger.info(f"\n--- Resumen para {symbol} (DataFrame OHLCV con datos rellenados) ---")
                        logger.info(data_df_filled.head())
                        logger.info(f"NaNs después de rellenar: {data_df_filled.isnull().sum().sum()}")
                        
                        # --- Normalización/Escalado ---
                        if Config.APPLY_NORMALIZATION and data_processor:
                            if not hasattr(data_processor.scaler, 'data_min_') and not hasattr(data_processor.scaler, 'mean_'):
                                # Si el escalador no ha sido ajustado (no se cargó o es nuevo), ajustarlo
                                logger.info(f"Ajustando y transformando datos OHLCV para {symbol}...")
                                data_df_scaled = data_processor.fit_transform(data_df_filled, Config.COLUMNS_TO_SCALE)
                                data_processor.save_scaler(Config.SCALER_SAVE_PATH)
                            else:
                                # Si el escalador ya fue ajustado (cargado), solo transformar
                                logger.info(f"Transformando datos OHLCV para {symbol} con escalador existente...")
                                data_df_scaled = data_processor.transform(data_df_filled, Config.COLUMNS_TO_SCALE)
                            
                            logger.info(f"\n--- Resumen para {symbol} (DataFrame OHLCV escalado) ---")
                            logger.info(data_df_scaled.head())

                            # Guardar datos
                            if not Config.SAVE_NORMALIZED_ONLY_IF_APPLIED:
                                fetcher.save_dataframe(data_df_filled, ohlcv_final_path, format=Config.SAVE_FORMAT)
                                logger.info(f"Datos OHLCV de {symbol} (original) guardados en '{ohlcv_final_path}'")
                            
                            fetcher.save_dataframe(data_df_scaled, ohlcv_scaled_path, format=Config.SAVE_FORMAT)
                            logger.info(f"Datos OHLCV de {symbol} (escalados) guardados en '{ohlcv_scaled_path}'")
                        else:
                            # Si no se aplica normalización, simplemente guardar el DataFrame rellenado
                            fetcher.save_dataframe(data_df_filled, ohlcv_final_path, format=Config.SAVE_FORMAT)
                            logger.info(f"Datos OHLCV de {symbol} (final) guardados en '{ohlcv_final_path}'")
                    else:
                        logger.warning(f"\nNo se pudieron obtener datos OHLCV para {symbol}.")
            else:
                logger.info("Descarga de datos OHLCV omitida según la configuración.")

            # --- Obtención de Otros Tipos de Datos Históricos (Ticker y Order Book) ---
            if 'ticker' in Config.HISTORICAL_DATA_TYPES_TO_FETCH or 'orderbook' in Config.HISTORICAL_DATA_TYPES_TO_FETCH: # <--- Usando HISTORICAL_DATA_TYPES_TO_FETCH
                logger.info("\n--- Demostración de Obtención y Guardado de Otros Tipos de Datos Históricos ---")
                for symbol in Config.SYMBOLS_TO_FETCH:
                    # Obtener Ticker
                    if 'ticker' in Config.HISTORICAL_DATA_TYPES_TO_FETCH: # <--- Usando HISTORICAL_DATA_TYPES_TO_FETCH
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
                    if 'orderbook' in Config.HISTORICAL_DATA_TYPES_TO_FETCH: # <--- Usando HISTORICAL_DATA_TYPES_TO_FETCH
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
                logger.info("Descarga de Ticker y Libro de Órdenes históricos omitida según la configuración.")

        elif Config.DATA_COLLECTION_MODE == 'realtime':
            logger.info(f"\n--- Modo Tiempo Real: Recolectando por {Config.REALTIME_COLLECTION_DURATION_SECONDS} segundos ---")
            
            realtime_tasks = []
            for symbol in Config.SYMBOLS_TO_FETCH:
                if 'trades' in Config.REALTIME_DATA_TYPES_TO_FETCH: # <--- Usando REALTIME_DATA_TYPES_TO_FETCH
                    realtime_tasks.append(
                        collect_realtime_data(fetcher, symbol, 'trades', Config.REALTIME_COLLECTION_DURATION_SECONDS)
                    )
                if 'orderbook' in Config.REALTIME_DATA_TYPES_TO_FETCH: # <--- Usando REALTIME_DATA_TYPES_TO_FETCH
                    realtime_tasks.append(
                        collect_realtime_data(fetcher, symbol, 'orderbook', Config.REALTIME_COLLECTION_DURATION_SECONDS)
                    )
            
            if realtime_tasks:
                await asyncio.gather(*realtime_tasks)
            else:
                logger.warning("No se configuraron tipos de datos para la recolección en tiempo real.")

        else:
            logger.error(f"Modo de recolección de datos no soportado: {Config.DATA_COLLECTION_MODE}")

    except ValueError as e:
        logger.error(f"Error de configuración: {e}")
    except Exception as e:
        logger.error(f"Ocurrió un error en la ejecución principal: {e}")

    end_time: float = time.time()
    logger.info(f"\n--- Tiempo total de ejecución: {end_time - start_time:.2f} segundos ---")

async def collect_realtime_data(fetcher: AsyncCryptoDataFetcher, symbol: str, data_type: str, duration: int) -> None:
    """
    Recolecta datos en tiempo real (trades u orderbook) para un símbolo dado
    durante una duración específica y los guarda incrementalmente.
    """
    realtime_filename = Config._get_realtime_filename(symbol, data_type)
    logger.info(f"[{symbol}] Iniciando recolección de {data_type} en tiempo real. Guardando en '{realtime_filename}'")

    start_time = time.time()
    
    # Abrir el archivo en modo append para escritura incremental
    with open(realtime_filename, 'a') as f:
        if data_type == 'trades':
            # Crear copia modificable de las opciones
            exchange_opts = fetcher.exchange_options.copy()
            
            # Manejar defaultType anidándolo dentro de options
            if 'defaultType' in exchange_opts:
                if 'options' not in exchange_opts:
                    exchange_opts['options'] = {}
                exchange_opts['options']['defaultType'] = exchange_opts['defaultType']
                del exchange_opts['defaultType']
            
            exchange_instance = fetcher.exchange_class(**exchange_opts)
            
        elif data_type == 'orderbook':
            # Crear copia modificable de las opciones
            exchange_opts = fetcher.exchange_options.copy()
            
            # Manejar defaultType anidándolo dentro de options
            if 'defaultType' in exchange_opts:
                if 'options' not in exchange_opts:
                    exchange_opts['options'] = {}
                exchange_opts['options']['defaultType'] = exchange_opts['defaultType']
                del exchange_opts['defaultType']
            
            exchange_instance = fetcher.exchange_class(**exchange_opts)
            if not exchange_instance.has.get('watchTrades'):
                logger.warning(f"El exchange {fetcher.exchange_id} no soporta watchTrades. Saltando recolección de trades para {symbol}.")
                await exchange_instance.close() # Asegurarse de cerrar si se abre
                return
            
            try:
                while time.time() - start_time < duration:
                    trades = await exchange_instance.watch_trades(symbol)
                    if trades:
                        for trade in trades:
                            f.write(json.dumps(trade) + '\n')
                            # logger.debug(f"[{symbol}] Trade en tiempo real: {trade['price']}")
                    await asyncio.sleep(0.1) # Pequeña pausa para evitar sobrecargar la CPU
            except Exception as e: # Captura cualquier excepción, incluyendo NetworkError y ExchangeError
                logger.error(f"[{symbol}] Error en stream de trades: {e}. Deteniendo recolección.")
            finally:
                await exchange_instance.close() # Asegurarse de cerrar la conexión del exchange
        
        elif data_type == 'orderbook':
            exchange_instance = fetcher.exchange_class(**fetcher.exchange_options)
            if not exchange_instance.has.get('watchOrderBook'):
                logger.warning(f"El exchange {fetcher.exchange_id} no soporta watchOrderBook. Saltando recolección de orderbook para {symbol}.")
                await exchange_instance.close() # Asegurarse de cerrar si se abre
                return

            try:
                while time.time() - start_time < duration:
                    order_book = await exchange_instance.watch_order_book(symbol)
                    if order_book:
                        f.write(json.dumps(order_book) + '\n')
                        # logger.debug(f"[{symbol}] Order Book en tiempo real (bid: {order_book['bids'][0][0]}, ask: {order_book['asks'][0][0]})")
                    await asyncio.sleep(0.1) # Pequeña pausa
            except Exception as e: # Captura cualquier excepción, incluyendo NetworkError y ExchangeError
                logger.error(f"[{symbol}] Error en stream de orderbook: {e}. Deteniendo recolección.")
            finally:
                await exchange_instance.close() # Asegurarse de cerrar la conexión del exchange
        else:
            logger.warning(f"Tipo de datos en tiempo real no soportado: {data_type} para {symbol}.")
    
    logger.info(f"[{symbol}] Recolección de {data_type} en tiempo real finalizada. Datos guardados en '{realtime_filename}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Selecciona el modo de recolección de datos: 'historical' o 'realtime'")
    parser.add_argument('--mode', '-m', choices=['historical', 'realtime'], default=Config.DATA_COLLECTION_MODE, help="Modo de recolección de datos")
    args = parser.parse_args()
    Config.DATA_COLLECTION_MODE = args.mode
    asyncio.run(main())

