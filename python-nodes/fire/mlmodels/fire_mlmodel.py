import copy

from fire.workflowengine.jobcontext import *
from pyspark.sql import DataFrame
from keras.models import Sequential

from sklearn.linear_model.base import *
import uuid
import operator

class ModelType(Enum):
    SKLEARN = 0
    TENSORFLOW = 1
    KERAS = 2


class FireMLModel:
    def __init__(self, type: ModelType, sklearnModel, kerasModel: Sequential):
        self.type = type
        self.sklearnModel = sklearnModel
        self.kerasModel = kerasModel

        self.uuid = str(uuid.uuid1())  # CREATE RANDOM UUID  TTTTTTTTTTTTTTTTTTT


