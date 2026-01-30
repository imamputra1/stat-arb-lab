import logging
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    pass


logger = logging.getLogger("LogReturnsTranformer")

class LogReturnsTranformer:
    def __init__(self, target_columns: Optional[List[str]] = None, replace_zeros: bool = True, epsilon: float = 1e-9):
        self.target_columns = target_columns
        self.replace_zeros = replace_zeros
        self.epsilon = epsilon        
