import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import joblib # Para guardar y cargar el escalador entrenado
import os
import logging
from typing import List, Literal, Any, Optional

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Clase para procesar datos, incluyendo normalización y escalado.
    Permite aplicar diferentes métodos de escalado y guardar/cargar el escalador.
    """
    def __init__(self, method: Literal['minmax', 'standard'] = 'minmax'):
        """
        Inicializa el procesador de datos con un método de escalado específico.

        Args:
            method (Literal['minmax', 'standard']): El método de escalado a usar.
                                                    'minmax' para MinMaxScaler (0-1).
                                                    'standard' para StandardScaler (media 0, varianza 1).
        """
        self.method = method
        if method == 'minmax':
            self.scaler: Any = MinMaxScaler()
        elif method == 'standard':
            self.scaler: Any = StandardScaler()
        else:
            raise ValueError(f"Método de escalado no soportado: {method}. Use 'minmax' o 'standard'.")
        logger.info(f"DataProcessor inicializado con método de escalado: {method}")

    def fit_transform(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Ajusta el escalador a los datos y luego los transforma.
        Este método debe usarse en los datos de entrenamiento para aprender los parámetros del escalador.

        Args:
            df (pd.DataFrame): El DataFrame de entrada.
            columns (List[str]): Lista de nombres de columnas a escalar.

        Returns:
            pd.DataFrame: El DataFrame con las columnas especificadas escaladas.
        """
        if df.empty:
            logger.warning("DataFrame vacío, no se puede ajustar ni transformar.")
            return df

        if not all(col in df.columns for col in columns):
            missing_cols = [col for col in columns if col not in df.columns]
            logger.error(f"Columnas a escalar no encontradas en el DataFrame: {missing_cols}")
            raise ValueError("Algunas columnas especificadas para escalado no existen en el DataFrame.")

        logger.info(f"Ajustando y transformando columnas: {columns} usando {self.method} scaler.")
        df_copy = df.copy()
        df_copy[columns] = self.scaler.fit_transform(df_copy[columns])
        logger.info("Escalado completado.")
        return df_copy

    def transform(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Transforma los datos usando un escalador previamente ajustado.
        Este método debe usarse en datos nuevos (ej. datos de validación, prueba o tiempo real)
        para aplicar la misma transformación aprendida.

        Args:
            df (pd.DataFrame): El DataFrame de entrada.
            columns (List[str]): Lista de nombres de columnas a escalar.

        Returns:
            pd.DataFrame: El DataFrame con las columnas especificadas escaladas.
        """
        if df.empty:
            logger.warning("DataFrame vacío, no se puede transformar.")
            return df
        
        if not hasattr(self.scaler, 'data_min_') and not hasattr(self.scaler, 'mean_'):
            logger.error("El escalador no ha sido ajustado (fit) aún. Use 'fit_transform' primero o cargue un escalador.")
            raise RuntimeError("El escalador no ha sido ajustado.")

        if not all(col in df.columns for col in columns):
            missing_cols = [col for col in columns if col not in df.columns]
            logger.error(f"Columnas a escalar no encontradas en el DataFrame: {missing_cols}")
            raise ValueError("Algunas columnas especificadas para escalado no existen en el DataFrame.")

        logger.info(f"Transformando columnas: {columns} usando {self.method} scaler.")
        df_copy = df.copy()
        df_copy[columns] = self.scaler.transform(df_copy[columns])
        logger.info("Transformación completada.")
        return df_copy

    def inverse_transform(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Invierte la transformación de las columnas escaladas.
        Útil para convertir los valores predichos de vuelta a su escala original.

        Args:
            df (pd.DataFrame): El DataFrame de entrada con las columnas escaladas.
            columns (List[str]): Lista de nombres de columnas a invertir.

        Returns:
            pd.DataFrame: El DataFrame con las columnas especificadas en su escala original.
        """
        if df.empty:
            logger.warning("DataFrame vacío, no se puede invertir la transformación.")
            return df

        if not hasattr(self.scaler, 'data_min_') and not hasattr(self.scaler, 'mean_'):
            logger.error("El escalador no ha sido ajustado (fit) aún. No se puede invertir la transformación.")
            raise RuntimeError("El escalador no ha sido ajustado.")

        if not all(col in df.columns for col in columns):
            missing_cols = [col for col in columns if col not in df.columns]
            logger.error(f"Columnas a invertir no encontradas en el DataFrame: {missing_cols}")
            raise ValueError("Algunas columnas especificadas para inversión no existen en el DataFrame.")

        logger.info(f"Invirtiendo transformación de columnas: {columns} usando {self.method} scaler.")
        df_copy = df.copy()
        df_copy[columns] = self.scaler.inverse_transform(df_copy[columns])
        logger.info("Inversión de transformación completada.")
        return df_copy

    def save_scaler(self, path: str) -> None:
        """
        Guarda el escalador entrenado en un archivo.

        Args:
            path (str): La ruta completa del archivo donde se guardará el escalador (ej. 'scaler.pkl').
        """
        try:
            joblib.dump(self.scaler, path)
            logger.info(f"Escalador guardado exitosamente en '{path}'.")
        except Exception as e:
            logger.error(f"Error al guardar el escalador en '{path}': {e}")
            raise

    def load_scaler(self, path: str) -> None:
        """
        Carga un escalador previamente entrenado desde un archivo.

        Args:
            path (str): La ruta completa del archivo desde donde se cargará el escalador.
        """
        if not os.path.exists(path):
            logger.error(f"Archivo de escalador no encontrado en '{path}'.")
            raise FileNotFoundError(f"Archivo de escalador no encontrado: {path}")
        try:
            self.scaler = joblib.load(path)
            logger.info(f"Escalador cargado exitosamente desde '{path}'.")
            # Asegurarse de que el método del escalador cargado coincida con el esperado
            if isinstance(self.scaler, MinMaxScaler):
                self.method = 'minmax'
            elif isinstance(self.scaler, StandardScaler):
                self.method = 'standard'
            else:
                logger.warning(f"Tipo de escalador desconocido cargado desde '{path}'. Asumiendo método 'minmax'.")
                self.method = 'minmax' # Fallback
        except Exception as e:
            logger.error(f"Error al cargar el escalador desde '{path}': {e}")
            raise