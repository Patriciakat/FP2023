{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    serializeDataFrame,
    saveDataFrame
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Aeson (encode)
import qualified Data.ByteString.Lazy as B
import Data.Time (UTCTime)

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName B.ByteString next  -- SaveFile now expects a ByteString
  | GetTime (UTCTime -> next)
  deriving (Functor)

type Execution = Free ExecutionAlgebra

-- Function to serialize a DataFrame to JSON ByteString
serializeDataFrame :: DataFrame -> B.ByteString
serializeDataFrame = encode

-- Adds an action to the Execution to save a DataFrame
saveDataFrame :: TableName -> DataFrame -> Execution ()
saveDataFrame tableName df = liftF $ SaveFile tableName (serializeDataFrame df) ()

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"
