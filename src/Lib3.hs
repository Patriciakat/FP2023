{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    serializeDataFrame,
    deserializeDataFrame,  -- Ensure this is defined and exported
    readDataFrameFile,     -- Custom function to read a DataFrame from a file within the Execution monad
    readDataFrame,         -- Standard IO function for reading a DataFrame from a file
    saveDataFrame,
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Aeson (encode, decode)
import qualified Data.ByteString.Lazy as B
import Data.Time (UTCTime)

type TableName = String
type FileContent = B.ByteString
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName FileContent next  -- SaveFile expects a ByteString which is FileContent
  | ReadFile FilePath (Maybe DataFrame -> next)  -- Add a constructor for reading a file into a DataFrame
  | GetTime (UTCTime -> next)
  deriving (Functor)

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

-- Function to read a DataFrame from a JSON file within the Execution
readDataFrameFile :: FilePath -> Execution (Maybe DataFrame)
readDataFrameFile filePath = liftF $ ReadFile filePath id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"

------------------------------------------------------ Data Serialization to JSON format ----------------------------------------------

-- Function to serialize a DataFrame to JSON ByteString
serializeDataFrame :: DataFrame -> B.ByteString
serializeDataFrame = encode

-- Adds an action to the Execution to save a DataFrame
saveDataFrame :: TableName -> DataFrame -> Execution ()
saveDataFrame tableName df = liftF $ SaveFile tableName (serializeDataFrame df) ()

------------------------------------------------------ Data (De)serialization from JSON format ----------------------------------------

-- Function to deserialize a JSON ByteString back to a DataFrame
deserializeDataFrame :: B.ByteString -> Maybe DataFrame
deserializeDataFrame = decode

-- Function to read a DataFrame from a JSON file
readDataFrame :: FilePath -> IO (Maybe DataFrame)
readDataFrame filePath = do
  jsonContent <- B.readFile filePath  -- Read the file content as a ByteString
  return (deserializeDataFrame jsonContent)