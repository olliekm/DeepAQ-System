from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults
import pandas as pd

class SarimaxForecaster:
    """Class for SARIMAX forecasting.
    """
    def __init__(self, order=(2,1,2), seasonal_order=(1,0,1,24)):
        self.order = order
        self.seasonal_order = seasonal_order
        self.results = None
        self.history_index_tz

    def load(self, path):
        """Load a SARIMAX model from a file."""
        self.results = SARIMAXResults.load(path)
        self.history_index_tz = (
            self.results.model.data.endog_names,
        )
        return self

    def save(self, path):
        """Save the SARIMAX model to a file."""
        if self.results is None:
            raise ValueError("No results to save. Fit the model first.")
        self.results.save(path)

    def append(self, new_series: pd.Series):
        """Append new data to the model's history.

        Args:
            new_series (pd.Series): New data to append.
        """
        if self.results is None:
            raise ValueError("No results to append to. Fit the model first.")
        
        new_series = new_series.astype(float).interpolate(limit_direction='both')

        self.results.model.data.append(new_series)

    def forecast(self, steps=1):
        """Forecast future values.

        Args:
            steps (int): Number of steps to forecast.
        
        Returns:
            pd.Series: Forecasted values.
        """
        if self.results is None:
            raise ValueError("No results to forecast from. Fit the model first.")
        
        forecast = self.results.get_forecast(steps=steps)
        return forecast.predicted_mean, forecast.conf_int()

    def refit(self, full_series: pd.Series, disp=False):
        """Refit the model with new data.

        Args:
            new_series (pd.Series): New data to fit the model.
        """
        if self.results is None:
            raise ValueError("No results to refit. Fit the model first.")
        
        new_series = new_series.astype(float).interpolate(limit_direction='both')
        
        # Refit the model with the new data
        self.results = SARIMAX(full_series, order=self.order, seasonal_order=self.seasonal_order).fit(disp=disp)
        
    
