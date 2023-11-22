{-# LANGUAGE OverloadedStrings #-}

module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

import qualified Data.Aeson as Aeson
import Data.Aeson.Types (Parser, typeMismatch, withObject, (.:), (.=), withArray)
import qualified Data.Vector as V
import qualified Data.HashMap.Strict as HM
import Control.Applicative ((<|>))

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  | FloatType
  deriving (Show, Eq)

data Column = Column String ColumnType
  deriving (Show, Eq)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | FloatValue Float
  | NullValue
  deriving (Show, Eq)

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq)

-- ToJSON instances
instance Aeson.ToJSON ColumnType where
  toJSON IntegerType = Aeson.String "IntegerType"
  toJSON StringType  = Aeson.String "StringType"
  toJSON BoolType    = Aeson.String "BoolType"
  toJSON FloatType   = Aeson.String "FloatType"

instance Aeson.ToJSON Column where
  toJSON (Column name columnType) = Aeson.object ["name" Aeson..= name, "type" Aeson..= columnType]

instance Aeson.ToJSON Value where
  toJSON (IntegerValue i) = Aeson.object ["IntegerValue" Aeson..= i]
  toJSON (StringValue s)  = Aeson.object ["StringValue" Aeson..= s]
  toJSON (BoolValue b)    = Aeson.object ["BoolValue" Aeson..= b]
  toJSON (FloatValue f)   = Aeson.object ["FloatValue" Aeson..= f]
  toJSON NullValue        = Aeson.Null

instance Aeson.ToJSON DataFrame where
  toJSON (DataFrame columns rows) = Aeson.object ["columns" Aeson..= columns, "rows" Aeson..= rows]
  
-- FromJSON instances
instance Aeson.FromJSON DataFrame where
  parseJSON = withObject "DataFrame" $ \obj -> do
    columns <- obj .: "columns"
    rows    <- obj .: "rows"
    parsedRows <- mapM parseRow (V.toList rows)
    return $ DataFrame columns parsedRows
    where
      parseRow :: Aeson.Value -> Parser Row
      parseRow = withArray "Row" $ \arr ->
        mapM parseValue (V.toList arr)

      parseValue val = case val of
        Aeson.Null       -> return NullValue  -- Handle Null here
        Aeson.Object obj -> parseComplexValue obj
        _                -> typeMismatch "Value" val

instance Aeson.FromJSON Column where
  parseJSON = withObject "Column" $ \obj -> do
    name <- obj .: "name"
    columnType <- obj .: "type"
    return $ Column name columnType
    
instance Aeson.FromJSON ColumnType where
  parseJSON = Aeson.withText "ColumnType" $ \t -> case t of
    "IntegerType" -> return IntegerType
    "StringType"  -> return StringType
    "BoolType"    -> return BoolType
    "FloatType"   -> return FloatType
    _             -> fail "Invalid ColumnType"

instance Aeson.FromJSON Value where
  parseJSON Aeson.Null = return NullValue  -- Handle Null here
  parseJSON (Aeson.Object obj) = parseComplexValue obj
  parseJSON val = typeMismatch "Value" val

parseComplexValue :: Aeson.Object -> Parser Value
parseComplexValue obj =
    (IntegerValue <$> obj .: "IntegerValue") <|>
    (StringValue <$> obj .: "StringValue") <|>
    (BoolValue <$> obj .: "BoolValue") <|>
    (FloatValue <$> obj .: "FloatValue") <|>
    parseNullValue obj
  where
    parseNullValue :: Aeson.Object -> Parser Value
    parseNullValue o = case Aeson.fromJSON (Aeson.Object o) of
        Aeson.Success Aeson.Null -> return NullValue
        _ -> fail "Expected a Value"