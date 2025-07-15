import ccxt.async_support as ccxt_async
import pandas as pd
from datetime import datetime
import asyncio
import time
import logging
import pytz
import os
import json
from typing import Optional, Dict, List, Any, Literal

# Configurar el logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AsyncCryptoDataFetcher:
    """
    Una clase asíncrona para adquirir datos históricos (OHLCV) de criptomonedas
    y otros tipos de datos utilizando la librería ccxt.async_support.

    Permite la obtención concurrente de datos para múltiples símbolos.

    Atributos:
        exchange_id (str): El ID del exchange (ej. 'binance', 'coinbasepro').
        exchange_options (Dict[str, Any]): Opciones adicionales para configurar el exchange de ccxt.
    """

    def __init__(self, exchange_id: str, exchange_options: Optional[Dict[str, Any]] = None) -> None:
        """
        Inicializa el AsyncCryptoDataFetcher.

        Args:
            exchange_id (str): El ID del exchange.
            exchange_options (Optional[Dict[str, Any]]): Un diccionario de opciones para configurar el exchange de ccxt.
                                     Por ejemplo: {'rateLimit': 2000, 'proxy': 'http://localhost:8080'}
        Raises:
            ValueError: Si el exchange no es soportado por ccxt.
        """
        try:
            self.exchange_class: Any = getattr(ccxt_async, exchange_id)
        except AttributeError:
            raise ValueError(f"El exchange '{exchange_id}' no es soportado por ccxt.")
        self.exchange_id: str = exchange_id
        self.exchange_options: Dict[str, Any] = exchange_options if exchange_options is not None else {}

    async def _validate_market(self, exchange: ccxt_async.Exchange, symbol: str) -> None:
        """
        Valida si el mercado (símbolo) existe en el exchange.
        Los mercados se cargan directamente del exchange.

        Args:
            exchange (ccxt.async_support.Exchange): Instancia del exchange de ccxt.
            symbol (str): El par de trading a validar.

        Raises:
            ValueError: Si el símbolo no es encontrado en el exchange.
            ccxt.async_support.BaseError: Si ocurre un error al cargar los mercados.
        """
        logger.info(f"Cargando mercados para {self.exchange_id}...")
        try:
            await exchange.load_markets()
            if symbol not in exchange.markets:
                raise ValueError(f"El símbolo '{symbol}' no fue encontrado en {self.exchange_id}.")
        except ccxt_async.BaseError as e:
            logger.error(f"Error al cargar los mercados de {self.exchange_id}: {e}")
            raise

    async def fetch_historical_data(self,
                                    symbol: str,
                                    timeframe: str,
                                    start_date_str: str,
                                    end_date_str: Optional[str] = None,
                                    timezone: str = 'UTC',
                                    max_retries: int = 5,
                                    initial_backoff: int = 1,
                                    incremental_save_path: Optional[str] = None) -> pd.DataFrame:
        """
        Obtiene todos los datos OHLCV para un único símbolo desde una fecha de inicio hasta una fecha de fin.

        Args:
            symbol (str): El par de trading a obtener.
            timeframe (str): El intervalo de tiempo de las velas.
            start_date_str (str): La fecha de inicio en formato 'YYYY-MM-DD'.
            end_date_str (Optional[str]): La fecha de fin en formato 'YYYY-MM-DD'. Si es None, descarga hasta la fecha actual.
            timezone (str): La zona horaria de las fechas de inicio/fin (ej. 'UTC', 'America/New_York').
                            Por defecto es 'UTC'.
            max_retries (int): Número máximo de reintentos para errores de red.
            initial_backoff (int): Tiempo de espera inicial en segundos para el backoff exponencial.
            incremental_save_path (Optional[str]): Ruta del archivo CSV donde se guardarán los datos
                                                    incrementalmente. Si es None, no se guarda incrementalmente.

        Returns:
            pd.DataFrame: Un DataFrame con los datos OHLCV, indexado por fecha.
                          Retorna un DataFrame vacío si no hay datos.
        """
        exchange: ccxt_async.Exchange = self.exchange_class(**self.exchange_options)
        try:
            await self._validate_market(exchange, symbol)

            if not exchange.has['fetchOHLCV']:
                logger.warning(f"El exchange {self.exchange_id} no soporta la obtención de datos OHLCV.")
                return pd.DataFrame()

            logger.info(f"[{symbol}] Iniciando la descarga de datos OHLCV...")

            # --- Manejo de Zona Horaria y Fechas de Inicio/Fin ---
            try:
                tz: pytz.BaseTzInfo = pytz.timezone(timezone)
            except pytz.UnknownTimeZoneError:
                logger.error(f"[{symbol}] Zona horaria desconocida: '{timezone}'. Usando 'UTC' por defecto.")
                tz = pytz.utc

            try:
                # Procesar fecha de inicio
                start_datetime_naive: datetime = datetime.strptime(start_date_str, '%Y-%m-%d')
                localized_start_datetime: datetime = tz.localize(start_datetime_naive, is_dst=None)
                utc_start_datetime: datetime = localized_start_datetime.astimezone(pytz.utc)
                since: int = exchange.parse8601(utc_start_datetime.isoformat())
            except ValueError as e:
                logger.error(f"[{symbol}] Error al parsear la fecha de inicio '{start_date_str}': {e}. Asegúrese de que el formato sea 'YYYY-MM-DD'.")
                return pd.DataFrame()

            until: int = exchange.milliseconds() # Por defecto, hasta ahora
            utc_end_datetime: Optional[datetime] = None 
            if end_date_str:
                try:
                    # Procesar fecha de fin
                    end_datetime_naive: datetime = datetime.strptime(end_date_str, '%Y-%m-%d')
                    # Para la fecha de fin, queremos incluir todo el día, por lo que avanzamos al inicio del día siguiente
                    localized_end_datetime: datetime = tz.localize(end_datetime_naive, is_dst=None)
                    utc_end_datetime = localized_end_datetime.astimezone(pytz.utc)
                    until = exchange.parse8601(utc_end_datetime.isoformat())
                except ValueError as e:
                    logger.error(f"[{symbol}] Error al parsear la fecha de fin '{end_date_str}': {e}. Asegúrese de que el formato sea 'YYYY-MM-DD'.")
                    return pd.DataFrame()
            # --- Fin del Manejo de Zona Horaria y Fechas de Inicio/Fin ---

            all_ohlcv: List[List[float]] = []
            
            current_backoff: int = initial_backoff
            retries: int = 0
            is_first_chunk: bool = True # Bandera para controlar la cabecera del CSV incremental

            # La condición del bucle ahora considera la fecha de fin
            while since < until:
                try:
                    limit: int = 500 # Ajustado el límite a 500 velas por solicitud
                    ohlcv: List[List[float]] = await exchange.fetch_ohlcv(symbol, timeframe, since, limit)
                    
                    if not ohlcv:
                        break

                    all_ohlcv.extend(ohlcv) # Se sigue recopilando para el DataFrame final

                    # --- Lógica de Almacenamiento Incremental (solo CSV) ---
                    if incremental_save_path:
                        chunk_df: pd.DataFrame = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                        chunk_df['timestamp'] = pd.to_datetime(chunk_df['timestamp'], unit='ms')
                        chunk_df.set_index('timestamp', inplace=True)
                        
                        # Escribir la cabecera solo si el archivo no existe o es la primera escritura
                        header: bool = not os.path.exists(incremental_save_path) or is_first_chunk
                        chunk_df.to_csv(incremental_save_path, mode='a', header=header)
                        is_first_chunk = False # Después de la primera escritura, no se escribe más la cabecera
                        logger.info(f"[{symbol}] Guardado incremental de {len(ohlcv)} velas en '{incremental_save_path}'")
                    # --- Fin Lógica de Almacenamiento Incremental ---

                    last_timestamp: float = ohlcv[-1][0]
                    since = int(last_timestamp + 1) # Asegurar que since sea un entero
                    
                    first_date: str = datetime.utcfromtimestamp(ohlcv[0][0] / 1000).strftime('%Y-%m-%d %H:%M:%S UTC')
                    logger.info(f"[{symbol}] Obtenidas {len(ohlcv)} velas desde {first_date}")

                    # Resetear reintentos y backoff si la operación fue exitosa
                    retries = 0
                    current_backoff = initial_backoff

                except ccxt_async.NetworkError as e:
                    retries += 1
                    if retries <= max_retries:
                        logger.warning(f"[{symbol}] Error de red ({e}), reintentando en {current_backoff}s... (Intento {retries}/{max_retries})")
                        await asyncio.sleep(current_backoff)
                        current_backoff *= 2 # Backoff exponencial
                    else:
                        logger.error(f"[{symbol}] Fallo después de {max_retries} reintentos por error de red: {e}")
                        break # Salir del bucle si se exceden los reintentos
                except ccxt_async.ExchangeError as e:
                    logger.error(f"[{symbol}] Error del exchange: {e}")
                    break
                except Exception as e: # Captura cualquier otra excepción inesperada
                    logger.error(f"[{symbol}] Ocurrió un error inesperado: {e}")
                    break
        finally:
            # Es crucial cerrar la sesión del exchange para liberar recursos
            await exchange.close()

        if not all_ohlcv:
            logger.info(f"[{symbol}] No se obtuvieron datos OHLCV.")
            return pd.DataFrame()

        df: pd.DataFrame = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df = df[~df.index.duplicated(keep='first')]

        # Filtrar el DataFrame para asegurar que los datos no excedan la fecha de fin
        if end_date_str and utc_end_datetime:
            end_timestamp_pd: pd.Timestamp = pd.to_datetime(utc_end_datetime).tz_localize(None)
            df = df[df.index.tz_localize(None) < end_timestamp_pd]
            logger.info(f"[{symbol}] Datos OHLCV filtrados hasta {end_date_str}. Total de velas: {len(df)}")
        
        logger.info(f"[{symbol}] Descarga de OHLCV completada. Total de velas: {len(df)}")
        return df

    async def fetch_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene la información del ticker para un símbolo (snapshot REST).
        
        Args:
            symbol (str): El par de trading a obtener.

        Returns:
            Optional[Dict[str, Any]]: Un diccionario con la información del ticker, o None si falla.
        """
        exchange: ccxt_async.Exchange = self.exchange_class(**self.exchange_options)
        try:
            await self._validate_market(exchange, symbol)
            if not exchange.has['fetchTicker']:
                logger.warning(f"El exchange {self.exchange_id} no soporta la obtención de tickers (REST).")
                return None
            
            logger.info(f"[{symbol}] Obteniendo ticker (snapshot REST)...")
            ticker: Dict[str, Any] = await exchange.fetch_ticker(symbol)
            logger.info(f"[{symbol}] Ticker obtenido: {ticker.get('last', 'N/A')}")
            return ticker
        except ccxt_async.BaseError as e:
            logger.error(f"[{symbol}] Error al obtener ticker (snapshot REST): {e}")
            return None
        except Exception as e:
            logger.error(f"[{symbol}] Error inesperado al obtener ticker (snapshot REST): {e}")
            return None
        finally:
            await exchange.close()

    async def fetch_order_book(self, symbol: str, limit: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Obtiene el libro de órdenes (Order Book) para un símbolo (snapshot REST).
        
        Args:
            symbol (str): El par de trading a obtener.
            limit (Optional[int]): Límite de entradas del libro de órdenes (ej. 100).

        Returns:
            Optional[Dict[str, Any]]: Un diccionario con el libro de órdenes, o None si falla.
        """
        exchange: ccxt_async.Exchange = self.exchange_class(**self.exchange_options)
        try:
            await self._validate_market(exchange, symbol)
            if not exchange.has['fetchOrderBook']:
                logger.warning(f"El exchange {self.exchange_id} no soporta la obtención del libro de órdenes (REST).")
                return None
            
            logger.info(f"[{symbol}] Obteniendo libro de órdenes (snapshot REST)...")
            order_book: Dict[str, Any] = await exchange.fetch_order_book(symbol, limit=limit)
            logger.info(f"[{symbol}] Libro de órdenes obtenido (asks: {len(order_book.get('asks', []))}, bids: {len(order_book.get('bids', []))})")
            return order_book
        except ccxt_async.BaseError as e:
            logger.error(f"[{symbol}] Error al obtener libro de órdenes (snapshot REST): {e}")
            return None
        except Exception as e:
            logger.error(f"[{symbol}] Error inesperado al obtener libro de órdenes (snapshot REST): {e}")
            return None
        finally:
            await exchange.close()

    async def watch_trades(self, symbol: str, duration_seconds: int, save_path: str) -> None:
        """
        Recolecta trades en tiempo real para un símbolo durante una duración específica
        y los guarda incrementalmente en un archivo JSONL.

        Args:
            symbol (str): El par de trading a observar.
            duration_seconds (int): Duración de la recolección en segundos.
            save_path (str): Ruta del archivo JSONL donde se guardarán los trades.
        """
        exchange: ccxt_async.Exchange = self.exchange_class(**self.exchange_options)
        try:
            await self._validate_market(exchange, symbol)
            if not exchange.has['watchTrades']:
                logger.warning(f"El exchange {self.exchange_id} no soporta la observación de trades (WebSocket).")
                return

            logger.info(f"[{symbol}] Iniciando recolección de trades en tiempo real por {duration_seconds} segundos...")
            end_time = time.time() + duration_seconds

            with open(save_path, 'a') as f:
                while time.time() < end_time:
                    try:
                        trades = await exchange.watch_trades(symbol)
                        for trade in trades:
                            f.write(json.dumps(trade) + '\n')
                        logger.info(f"[{symbol}] Recolectados {len(trades)} trades. Tiempo restante: {int(end_time - time.time())}s")
                    except ccxt_async.NetworkError as e:
                        logger.warning(f"[{symbol}] Error de red en WebSocket ({e}), reintentando...")
                        await asyncio.sleep(1) # Esperar un poco antes de reintentar
                    except ccxt_async.ExchangeError as e:
                        logger.error(f"[{symbol}] Error del exchange en WebSocket: {e}")
                        break # Salir si hay un error persistente del exchange
                    except Exception as e:
                        logger.error(f"[{symbol}] Ocurrió un error inesperado en watch_trades: {e}")
                        break
        except Exception as e:
            logger.error(f"[{symbol}] Error al configurar watch_trades: {e}")
        finally:
            await exchange.close()
            logger.info(f"[{symbol}] Recolección de trades en tiempo real finalizada.")

    async def watch_order_book(self, symbol: str, duration_seconds: int, save_path: str, limit: Optional[int] = None) -> None:
        """
        Recolecta actualizaciones del libro de órdenes en tiempo real para un símbolo durante una duración específica
        y las guarda incrementalmente en un archivo JSONL.

        Args:
            symbol (str): El par de trading a observar.
            duration_seconds (int): Duración de la recolección en segundos.
            save_path (str): Ruta del archivo JSONL donde se guardarán las actualizaciones del libro de órdenes.
            limit (Optional[int]): Límite de entradas del libro de órdenes a observar (ej. 100).
        """
        exchange: ccxt_async.Exchange = self.exchange_class(**self.exchange_options)
        try:
            await self._validate_market(exchange, symbol)
            if not exchange.has['watchOrderBook']:
                logger.warning(f"El exchange {self.exchange_id} no soporta la observación del libro de órdenes (WebSocket).")
                return

            logger.info(f"[{symbol}] Iniciando recolección de libro de órdenes en tiempo real por {duration_seconds} segundos...")
            end_time = time.time() + duration_seconds

            with open(save_path, 'a') as f:
                while time.time() < end_time:
                    try:
                        order_book = await exchange.watch_order_book(symbol, limit=limit)
                        # Guardar una copia profunda para evitar que el objeto cambie después de ser serializado
                        f.write(json.dumps(order_book) + '\n')
                        logger.info(f"[{symbol}] Actualización de libro de órdenes recibida. Tiempo restante: {int(end_time - time.time())}s")
                    except ccxt_async.NetworkError as e:
                        logger.warning(f"[{symbol}] Error de red en WebSocket ({e}), reintentando...")
                        await asyncio.sleep(1)
                    except ccxt_async.ExchangeError as e:
                        logger.error(f"[{symbol}] Error del exchange en WebSocket: {e}")
                        break
                    except Exception as e:
                        logger.error(f"[{symbol}] Ocurrió un error inesperado en watch_order_book: {e}")
                        break
        except Exception as e:
            logger.error(f"[{symbol}] Error al configurar watch_order_book: {e}")
        finally:
            await exchange.close()
            logger.info(f"[{symbol}] Recolección de libro de órdenes en tiempo real finalizada.")

    def save_dataframe(self, df: pd.DataFrame, path: str, format: Literal['csv', 'parquet'] = 'csv') -> None:
        """
        Guarda un DataFrame en el formato especificado (CSV o Parquet).

        Args:
            df (pd.DataFrame): El DataFrame a guardar.
            path (str): La ruta completa del archivo de salida (incluyendo el nombre del archivo).
            format (Literal['csv', 'parquet']): El formato de archivo para guardar.
        
        Raises:
            ValueError: Si el formato no es soportado.
        """
        logger.info(f"Guardando DataFrame en '{path}' con formato '{format}'...")
        if format == 'csv':
            df.to_csv(path)
        elif format == 'parquet':
            try:
                df.to_parquet(path)
            except ImportError:
                logger.error("La librería 'pyarrow' es necesaria para guardar en formato Parquet. Instálela con 'pip install pyarrow'.")
                raise
        else:
            raise ValueError(f"Formato de guardado no soportado: {format}. Use 'csv' o 'parquet'.")
        logger.info(f"DataFrame guardado exitosamente en '{path}'.")

    def fill_missing_data(self, df: pd.DataFrame, timeframe: str, method: str = 'ffill') -> pd.DataFrame:
        """
        Detecta y rellena los huecos en un DataFrame OHLCV, asegurando un índice de tiempo continuo.

        Args:
            df (pd.DataFrame): El DataFrame OHLCV con 'timestamp' como índice.
            timeframe (str): El intervalo de tiempo de las velas (ej. '1d', '4h', '1h', '1m').
                             Necesario para generar el rango de fechas continuo.
            method (str): El método para rellenar los valores NaN. Opciones:
                          'ffill': Rellena hacia adelante con el último valor válido.
                          'bfill': Rellena hacia atrás con el siguiente valor válido.
                          'linear': Interpolación lineal.
                          'mean': Rellena con la media de la columna (menos recomendado para series temporales).

        Returns:
            pd.DataFrame: El DataFrame con un índice de tiempo continuo y los huecos rellenados.
        """
        if df.empty:
            logger.warning("DataFrame vacío, no se pueden rellenar datos faltantes.")
            return df

        logger.info(f"Rellenando datos faltantes en el DataFrame usando el método '{method}'...")

        # Convertir timeframe a un offset de Pandas para generar el rango de fechas
        timeframe_map: Dict[str, str] = {
            '1m': '1min', '3m': '3min', '5m': '5min', '15m': '15min', '30m': '30min',
            '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '8h': '8H', '12h': '12H',
            '1d': '1D', '3d': '3D', '1w': '1W', '1M': '1M'
        }
        pd_timeframe = timeframe_map.get(timeframe, None)

        if pd_timeframe is None:
            logger.warning(f"Timeframe '{timeframe}' no reconocido para generar un índice de tiempo continuo. Saltando la generación de índice continuo.")
            # Si no se puede generar un índice continuo, solo rellenar los NaNs existentes
            if method == 'ffill':
                df = df.fillna(method='ffill')
            elif method == 'bfill':
                df = df.fillna(method='bfill')
            elif method == 'linear':
                df = df.interpolate(method='linear')
            elif method == 'mean':
                df = df.fillna(df.mean(numeric_only=True))
            else:
                logger.warning(f"Método de relleno '{method}' no soportado para datos sin índice continuo. No se rellenaron los NaNs.")
            return df

        # Generar un rango de fechas completo
        full_index: pd.DatetimeIndex = pd.date_range(
            start=df.index.min(),
            end=df.index.max(),
            freq=pd_timeframe
        )
        
        # Reindexar el DataFrame para incluir todas las fechas del rango
        df_reindexed: pd.DataFrame = df.reindex(full_index)

        # Rellenar los valores faltantes (NaNs)
        if method == 'ffill':
            df_filled: pd.DataFrame = df_reindexed.fillna(method='ffill')
        elif method == 'bfill':
            df_filled: pd.DataFrame = df_reindexed.fillna(method='bfill')
        elif method == 'linear':
            df_filled: pd.DataFrame = df_reindexed.interpolate(method='linear')
        elif method == 'mean':
            df_filled: pd.DataFrame = df_reindexed.fillna(df_reindexed.mean(numeric_only=True))
        else:
            logger.warning(f"Método de relleno '{method}' no soportado. Usando 'ffill' por defecto.")
            df_filled: pd.DataFrame = df_reindexed.fillna(method='ffill')

        # Para las columnas OHLC, si el primer valor es NaN y se usó ffill, o el último es NaN y se usó bfill,
        # puede que queden NaNs. Podemos intentar rellenar los restantes con el método opuesto o interpolación.
        # Por simplicidad, un ffill seguido de bfill es robusto para la mayoría de los casos.
        df_filled = df_filled.fillna(method='ffill').fillna(method='bfill')
        
        logger.info(f"Relleno de datos faltantes completado. Filas antes: {len(df)}, Filas después: {len(df_filled)}")
        return df_filled
