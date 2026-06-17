# plugins/operators/data_quality_operator.py
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import os

class DataQualityOperator(BaseOperator):
    """
    Vérifie la qualité des données selon les critères spécifiés.
    Echec si moins de 95% des lignes passent les contrôles.
    """

    # --- Ajout pour que Airflow remplace {{ ds }} dans input_path ---
    template_fields = ('input_path',)

    @apply_defaults
    def __init__(
        self,
        input_path: str,
        critical_columns: list,
        numeric_columns: list = None,
        date_columns: list = None,
        threshold: float = 0.95,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_path = input_path
        self.critical_columns = critical_columns
        self.numeric_columns = numeric_columns or []
        self.date_columns = date_columns or []
        self.threshold = threshold

    def execute(self, context):
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Le fichier n'existe pas : {self.input_path}")

        df = pd.read_parquet(self.input_path)

        total_rows = len(df)
        passed_rows = pd.Series(True, index=df.index)

        # 1. Absence de valeurs nulles dans les colonnes critiques
        for col in self.critical_columns:
            if col not in df.columns:
                raise ValueError(f"Colonne critique manquante : {col}")
            passed_rows &= df[col].notnull()

        # 2. Cohérence des valeurs numériques (>=0)
        for col in self.numeric_columns:
            if col in df.columns:
                passed_rows &= df[col].apply(lambda x: pd.notnull(x) and x >= 0)

        # 3. Format des dates
        for col in self.date_columns:
            if col in df.columns:
                passed_rows &= pd.to_datetime(df[col], errors='coerce').notnull()

        # 4. Absence de doublons
        passed_rows &= ~df.duplicated(subset=self.critical_columns, keep=False)

        # Calcul du pourcentage de lignes valides
        valid_ratio = passed_rows.sum() / total_rows
        self.log.info(f"Lignes valides : {passed_rows.sum()}/{total_rows} ({valid_ratio*100:.2f}%)")

        if valid_ratio < self.threshold:
            raise ValueError(f"Data Quality check échoué : {valid_ratio*100:.2f}% < {self.threshold*100}%")
        else:
            self.log.info("Data Quality check réussi ✅")
