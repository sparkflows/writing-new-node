from typing import List

from fire.output.output import Output


class Outputs:
    def __init__(self, outputs: List[Output] = []):
        self.outputs = outputs
