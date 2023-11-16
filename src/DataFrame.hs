{-# LANGUAGE OverloadedStrings #-}

module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

import qualified Data.Aeson as Aeson

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

-- Here are your ToJSON instances
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