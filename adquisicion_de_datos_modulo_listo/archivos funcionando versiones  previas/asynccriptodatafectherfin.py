import ccxt.async_support as ccxt_async
import pandas as pd
from datetime import datetime
import asyncio
import time
import logging
import pytz

# Configurar el logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AsyncCryptoDataFetcher:
    """
    Una clase asíncrona para adquirir datos históricos (OHLCV) de criptomonedas
    utilizando la librería ccxt.async_support.

    Permite la obtención concurrente de datos para múltiples símbolos.

    Atributos:
        exchange_id (str): El ID del exchange (ej. 'binance', 'coinbasepro').
        exchange_options (dict): Opciones adicionales para configurar el exchange de ccxt.
    """

    def __init__(self, exchange_id: str, exchange_options: dict = None):
        """
        Inicializa el AsyncCryptoDataFetcher.

        Args:
            exchange_id (str): El ID del exchange.
            exchange_options (dict): Un diccionario de opciones para configurar el exchange de ccxt.
                                     Por ejemplo: {'rateLimit': 2000, 'proxy': 'http://localhost:8080'}
        Raises:
            ValueError: Si el exchange no es soportado por ccxt.
        """
        try:
            self.exchange_class = getattr(ccxt_async, exchange_id)
        except AttributeError:
            raise ValueError(f"El exchange '{exchange_id}' no es soportado por ccxt.")
        self.exchange_id = exchange_id
        self.exchange_options = exchange_options if exchange_options is not None else {}

    async def _validate_market(self, exchange: ccxt_async.Exchange, symbol: str) -> None:
        """Valida si el mercado (símbolo) existe en el exchange."""
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
                                    end_date_str: str = None, # Nuevo parámetro para la fecha de fin
                                    timezone: str = 'UTC',
                                    max_retries: int = 5,
                                    initial_backoff: int = 1) -> pd.DataFrame:
        """
        Obtiene todos los datos OHLCV para un único símbolo desde una fecha de inicio hasta una fecha de fin.

        Args:
            symbol (str): El par de trading a obtener.
            timeframe (str): El intervalo de tiempo de las velas.
            start_date_str (str): La fecha de inicio en formato 'YYYY-MM-DD'.
            end_date_str (str, opcional): La fecha de fin en formato 'YYYY-MM-DD'. Si es None, descarga hasta la fecha actual.
            timezone (str): La zona horaria de las fechas de inicio/fin (ej. 'UTC', 'America/New_York').
                            Por defecto es 'UTC'.
            max_retries (int): Número máximo de reintentos para errores de red.
            initial_backoff (int): Tiempo de espera inicial en segundos para el backoff exponencial.

        Returns:
            pd.DataFrame: Un DataFrame con los datos OHLCV, indexado por fecha.
                          Retorna un DataFrame vacío si no hay datos.
        """
        exchange = self.exchange_class(**self.exchange_options)
        try:
            await self._validate_market(exchange, symbol)

            if not exchange.has['fetchOHLCV']:
                logger.warning(f"El exchange {self.exchange_id} no soporta la obtención de datos OHLCV.")
                return pd.DataFrame()

            logger.info(f"[{symbol}] Iniciando la descarga de datos...")

            # --- Manejo de Zona Horaria y Fechas de Inicio/Fin ---
            try:
                tz = pytz.timezone(timezone)
            except pytz.UnknownTimeZoneError:
                logger.error(f"[{symbol}] Zona horaria desconocida: '{timezone}'. Usando 'UTC' por defecto.")
                tz = pytz.utc

            try:
                # Procesar fecha de inicio
                start_datetime_naive = datetime.strptime(start_date_str, '%Y-%m-%d')
                localized_start_datetime = tz.localize(start_datetime_naive, is_dst=None)
                utc_start_datetime = localized_start_datetime.astimezone(pytz.utc)
                since = exchange.parse8601(utc_start_datetime.isoformat())
            except ValueError as e:
                logger.error(f"[{symbol}] Error al parsear la fecha de inicio '{start_date_str}': {e}. Asegúrese de que el formato sea 'YYYY-MM-DD'.")
                return pd.DataFrame()

            until = exchange.milliseconds() # Por defecto, hasta ahora
            if end_date_str:
                try:
                    # Procesar fecha de fin
                    end_datetime_naive = datetime.strptime(end_date_str, '%Y-%m-%d')
                    # Para la fecha de fin, queremos incluir todo el día, por lo que avanzamos al inicio del día siguiente
                    localized_end_datetime = tz.localize(end_datetime_naive, is_dst=None)
                    utc_end_datetime = localized_end_datetime.astimezone(pytz.utc)
                    until = exchange.parse8601(utc_end_datetime.isoformat())
                except ValueError as e:
                    logger.error(f"[{symbol}] Error al parsear la fecha de fin '{end_date_str}': {e}. Asegúrese de que el formato sea 'YYYY-MM-DD'.")
                    return pd.DataFrame()
            # --- Fin del Manejo de Zona Horaria y Fechas de Inicio/Fin ---

            all_ohlcv = []
            
            current_backoff = initial_backoff
            retries = 0

            # La condición del bucle ahora considera la fecha de fin
            while since < until:
                try:
                    limit = 1000
                    # Asegurarse de no solicitar datos más allá de 'until'
                    # Algunos exchanges pueden no soportar 'until' directamente en fetch_ohlcv,
                    # por lo que el filtrado posterior es crucial.
                    ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since, limit)
                    
                    if not ohlcv:
                        break

                    all_ohlcv.extend(ohlcv)
                    last_timestamp = ohlcv[-1][0]
                    since = last_timestamp + 1
                    
                    first_date = datetime.utcfromtimestamp(ohlcv[0][0] / 1000).strftime('%Y-%m-%d %H:%M:%S UTC')
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
            logger.info(f"[{symbol}] No se obtuvieron datos.")
            return pd.DataFrame()

        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df = df[~df.index.duplicated(keep='first')]

        # Filtrar el DataFrame para asegurar que los datos no excedan la fecha de fin
        if end_date_str:
            # Convertir la fecha de fin a un timestamp de pandas para el filtrado
            end_timestamp_pd = pd.to_datetime(utc_end_datetime).tz_localize(None)
            df = df[df.index.tz_localize(None) < end_timestamp_pd]
            logger.info(f"[{symbol}] Datos filtrados hasta {end_date_str}. Total de velas: {len(df)}")
        
        logger.info(f"[{symbol}] Descarga completada. Total de velas: {len(df)}")
        return df

async def main():
    """
    Función principal para demostrar la descarga concurrente de datos.
    """
    # --- Parámetros de Configuración ---
    EXCHANGE_ID = 'binance'
    TIMEFRAME = '1d'
    START_DATE = '2023-01-01'
    # Nuevo: Fecha de fin opcional
    END_DATE = '2023-03-31' # Descargar datos solo hasta el 31 de marzo de 2023

    EXCHANGE_OPTIONS = {} # Por ahora, sin opciones específicas

    # Lista de símbolos para descargar concurrentemente
    symbols_to_fetch = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT']

    logger.info("--- Iniciando Descarga Concurrente de Datos ---")
    start_time = time.time()

    try:
        fetcher = AsyncCryptoDataFetcher(exchange_id=EXCHANGE_ID, exchange_options=EXCHANGE_OPTIONS)
        
        # Crear una tarea de descarga para cada símbolo, usando la nueva fecha de fin
        tasks = [
            fetcher.fetch_historical_data(
                symbol,
                TIMEFRAME,
                START_DATE,
                end_date_str=END_DATE, # Usar el nuevo parámetro
                max_retries=3,
                initial_backoff=2
            )
            for symbol in symbols_to_fetch
        ]
        
        # Ejecutar todas las tareas en paralelo y esperar a que terminen
        results = await asyncio.gather(*tasks)
        
        # Procesar los resultados
        for symbol, data_df in zip(symbols_to_fetch, results):
            if not data_df.empty:
                logger.info(f"\n--- Resumen para {symbol} ---")
                logger.info(data_df.head())
                
                # Guardar en CSV
                output_filename = f"{EXCHANGE_ID}_{symbol.replace('/', '_')}_{TIMEFRAME}_async_to_{END_DATE}.csv"
                data_df.to_csv(output_filename)
                logger.info(f"Datos de {symbol} guardados en '{output_filename}'")
            else:
                logger.warning(f"\nNo se pudieron obtener datos para {symbol}.")

    except ValueError as e:
        logger.error(f"Error de configuración: {e}")
    except Exception as e:
        logger.error(f"Ocurrió un error en la ejecución principal: {e}")

    end_time = time.time()
    logger.info(f"\n--- Tiempo total de ejecución: {end_time - start_time:.2f} segundos ---")


if __name__ == "__main__":
    # Ejecutar el bucle de eventos de asyncio
    asyncio.run(main())
