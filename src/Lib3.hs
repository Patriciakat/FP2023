{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GADTs #-}

module Lib3
  ( executeSql,
    Execution,
    readDataFrameFile,
    ExecutionAlgebra(..),
    serializeDataFrame,
    deserializeDataFrame,
    readDataFrame,
    saveDataFrame,
    getTime
  )
where

import Lib2 (MyParsedStatement(..), Condition, parseStatement, executeStatement, Value, isValidType)
import DataFrame (DataFrame(..), Column)
import Control.Monad.Free (Free (..), liftF)
import Data.Aeson (encode, decode)
import qualified Data.ByteString.Lazy as B
import Data.Time (UTCTime, getCurrentTime)

type TableName = String
type FileContent = B.ByteString
type ErrorMessage = String

-- Algebra defining different execution steps for the Free monad
data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName FileContent next
  | ExecuteSqlStatement MyParsedStatement (Either ErrorMessage DataFrame -> next)
  | GetTime (UTCTime -> next)
  deriving (Functor)

type Execution = Free ExecutionAlgebra

-- Function to execute SQL commands, handling different types of statements
executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    case parseStatement sql of
        Right parsedStatement -> 
            liftF $ ExecuteSqlStatement parsedStatement id
        Left errorMsg -> 
            return $ Left errorMsg

-- Function to run the Execution monad
runExecuteIO :: Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next

-- Function to handle each step of the Free monad
runStep :: ExecutionAlgebra a -> IO a
runStep (LoadFile tableName next) = do
    fileContent <- B.readFile ("db/" ++ tableName ++ ".json")
    return $ next fileContent
runStep (SaveFile tableName content next) = do
    B.writeFile ("db/" ++ tableName ++ ".json") content
    return next
runStep (ExecuteSqlStatement statement next) = do
    result <- executeStatement statement
    return $ next result
runStep (GetTime next) = do
    currentTime <- getCurrentTime
    return $ next currentTime

-- Serialization function for DataFrame
serializeDataFrame :: DataFrame -> B.ByteString
serializeDataFrame = encode

-- Deserialization function for DataFrame
deserializeDataFrame :: B.ByteString -> Maybe DataFrame
deserializeDataFrame = decode

-- Function to save DataFrame to a file, ensuring proper file handling
saveDataFrame :: TableName -> DataFrame -> Execution ()
saveDataFrame tableName df = liftF $ SaveFile tableName (serializeDataFrame df) ()

-- IO wrapper function to read DataFrame from a file
readDataFrame :: FilePath -> IO (Maybe DataFrame)
readDataFrame filePath = do
  jsonContent <- B.readFile filePath
  let maybeDataFrame = deserializeDataFrame jsonContent
  return maybeDataFrame

-- Function to read DataFrame from a file using Free monad
readDataFrameFile :: FilePath -> Execution (Maybe DataFrame)
readDataFrameFile filePath = liftF $ LoadFile filePath (\content -> id (deserializeDataFrame content))

-- Function to get the current time
getTime :: Execution UTCTime
getTime = liftF $ GetTime id