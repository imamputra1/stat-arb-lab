from core.shared import Result, Ok, Err

class SignalGenerator:
    def generate(self, df):
        return df

class StrategyRegistry:
    pass

def get_signal_strategy(name, params):
    return SignalGenerator()