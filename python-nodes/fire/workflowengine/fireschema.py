from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from fire.output.output import *


class FireSchema:
    def __init__(self):
        self.colNames = []
        self.colTypes = []
        self.colFormats = []
        self.mlTypes = []

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    @staticmethod
    def load(d: dict):
        return FireSchema(**d)

    @staticmethod
    def createFireSchema(structType: StructType):
        fields = structType.fields
        colNames = []
        colTypes = []
        colFormats = []
        mlTypes = []

        for field in fields:
            colNames.append(field.name)

            temp = FireSchema.getType(field.dataType)
            colTypes.append(temp)

            colFormats.append("")
            mlTypes.append("")

        fireSchema = FireSchema()
        fireSchema.colNames = colNames
        fireSchema.colTypes = colTypes
        fireSchema.colFormats = colFormats
        fireSchema.mlTypes = mlTypes

        return fireSchema

    @staticmethod
    def getType(dataType: DataType):
        if isinstance(dataType, IntegerType):
            return "INTEGER"
        if isinstance(dataType, BooleanType):
            return "BOOLEAN"
        if isinstance(dataType, DateType):
            return "DATE"
        if isinstance(dataType, DoubleType):
            return "DOUBLE"
        if isinstance(dataType, FloatType):
            return "FLOAT"
        if isinstance(dataType, LongType):
            return "LONG"
        if isinstance(dataType, StringType):
            return "STRING"
        if isinstance(dataType, ByteType):
            return "BYTE"
        if isinstance(dataType, BinaryType):
            return "BINARY"
        if isinstance(dataType, DecimalType):
            return "DECIMAL"

        return "STRING"

    @staticmethod
    def getSparkSQLType(type: str):

        if type == 'INTEGER':
            return IntegerType()

        if type == 'BOOLEAN':
            return BooleanType()

        if type == 'DATE':
            return DateType()

        if type == 'DECIMAL':
            # DecimalType is causing problem in displaying in the UI for interactive workflow execution
            #decimalType = DecimalType(19, 2)
            # return decimalType
            return DoubleType()

        if type == 'DOUBLE':
            return DoubleType()

        if type == 'FLOAT':
            return FloatType()

        if type == 'LONG':
            return LongType()

        if type == 'STRING':
            return StringType()

        if type == 'BYTE':
            return ByteType()

        if type == 'BINARY':
            return BinaryType()

        if type == 'MAP':
            return MapType()

        if type == 'ARRAY':
            return ArrayType()

        if type == 'TIMESTAMP':
            return TimestampType()

        if type == 'STRUCT':
            fields = StructField[1]
            fields[0] = StructField()
            return StructType(fields)

    def getColIdx(self, col: str):
        for i in range(len(self.colNames)):
            if(self.colNames[i] == col):
                return i

        return -1

    def getSchemaForColumns(self, givencols: list):

        goodColumns = []
        result = FireSchema()

        for i in range(len(givencols)):
            colIdx = self.getColIdx(givencols[i])

            if(colIdx >= 0):
                goodColumns.append(givencols[i])

        colnamesarr = []
        coltypearr = []
        colformatarr = []
        colmltypesarr = []

        for j in range(len(goodColumns)):
            colIdx = self.getColIdx(givencols[j])

            colnamesarr.append(self.colNames[colIdx])
            coltypearr.append(self.colTypes[colIdx])
            colformatarr.append(self.colFormats[colIdx])
            colmltypesarr.append(self.mlTypes[colIdx])

        result.colNames = colnamesarr
        result.colTypes = coltypearr
        result.colFormats = colformatarr
        result.mlTypes = colmltypesarr

        return result

    def toSparkSQLStructType(self):

        fieldsarr = []

        i: int = 0
        for fieldName in self.colNames:
            typ = self.getSparkSQLType(self.colTypes[i])
            fieldsarr.append(StructField(fieldName, typ, True))
            i += 1

        schema = StructType(fieldsarr)

        return schema
