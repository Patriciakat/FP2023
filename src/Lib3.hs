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
    insertIntoTable,
    getTime
  )
where

import Lib2 (MyParsedStatement(..), Condition, parseStatement, executeStatement, meetAllConditions, Value, isValidType)
import DataFrame (DataFrame(..), Column, Value)
import Control.Monad.Free (Free (..), liftF)
import Data.Aeson (encode, decode)
import qualified Data.ByteString.Lazy as B
import Data.Time (UTCTime)

type TableName = String
type FileContent = B.ByteString
type ErrorMessage = String

-- Algebra defining different execution steps for the Free monad
data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName FileContent next
  | ReadFile FilePath (Maybe DataFrame -> next)
  | DeleteFromTable TableName [Condition] next
  | InsertIntoTable TableName [Value] next
  | GetTime (UTCTime -> next)
  deriving (Functor)

type Execution = Free ExecutionAlgebra

-- Function to execute SQL commands, handling different types of statements
executeSql :: String -> [(TableName, DataFrame)] -> Execution (Either ErrorMessage DataFrame)
executeSql sql db = do
    case parseStatement sql of
        Right (DeleteFrom tableName conditions) -> do
            let maybeCurrentDF = lookup tableName db
            case maybeCurrentDF of
                Just currentDF -> do
                    let updatedDF = deleteRows currentDF conditions
                    saveDataFrame tableName updatedDF
                    return $ Right updatedDF
                Nothing -> return $ Left "Table not found"

        Right (InsertInto tableName values) -> do
            let maybeDF = lookup tableName db
            case maybeDF of
                Just (DataFrame columns oldRows) -> do
                    let newRow = values
                    if length values == length columns && all Lib2.isValidType (zip columns values) then do
                        let updatedDF = DataFrame columns (oldRows ++ [newRow])
                        saveDataFrame tableName updatedDF
                        return $ Right updatedDF
                    else
                        return $ Left "Mismatched column count or types in INSERT INTO statement"
                Nothing -> return $ Left "Table not found"

        Right otherStatement -> 
            return $ executeStatement otherStatement db

        Left errorMsg -> return $ Left errorMsg
        
-- Function to run the Execution monad
runExecuteIO :: Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next

-- Serialization function for DataFrame
serializeDataFrame :: DataFrame -> B.ByteString
serializeDataFrame = encode

-- Function to save DataFrame to a file, ensuring proper file handling
saveDataFrame :: TableName -> DataFrame -> Execution ()
saveDataFrame tableName df = liftF $ SaveFile tableName (serializeDataFrame df) ()

-- Function to actually write the serialized data to a file
-- Ensures file is properly opened, written to, and closed
writeDataFrameToFile :: TableName -> B.ByteString -> IO ()
writeDataFrameToFile tableName content = 
  B.writeFile ("db/" ++ tableName ++ ".json") content
  
runStep :: ExecutionAlgebra a -> IO a
runStep (SaveFile tableName content next) = do
    writeDataFrameToFile tableName content
    return next
  
-- Deserialization function for DataFrame
deserializeDataFrame :: B.ByteString -> Maybe DataFrame
deserializeDataFrame = decode

-- Function to read DataFrame from a file using Free monad
readDataFrameFile :: FilePath -> Execution (Maybe DataFrame)
readDataFrameFile filePath = liftF $ ReadFile filePath id

-- IO wrapper function to read DataFrame from a file
readDataFrame :: FilePath -> IO (Maybe DataFrame)
readDataFrame filePath = do
  jsonContent <- B.readFile filePath
  let maybeDataFrame = deserializeDataFrame jsonContent
  return maybeDataFrame
  
-- Function to delete rows from a table
deleteFromTable :: TableName -> [Lib2.Condition] -> Execution (Either ErrorMessage DataFrame)
deleteFromTable tableName conditions = do
    fileContent <- liftF $ LoadFile tableName id
    let maybeCurrentDF = deserializeDataFrame fileContent
    case maybeCurrentDF of
        Just currentDF -> do
            let updatedDF = deleteRows currentDF conditions
            saveDataFrame tableName updatedDF -- Add this line to save the updated DataFrame
            return $ Right updatedDF
        Nothing -> return $ Left "Error: Table data could not be loaded."

-- Helper function to execute delete operation
executeDelete :: MyParsedStatement -> [(TableName, DataFrame)] -> Either ErrorMessage (TableName, DataFrame)
executeDelete (DeleteFrom tableName conditions) db =
    case lookup tableName db of
        Just df -> let updatedDF = deleteRows df conditions in Right (tableName, updatedDF)
        Nothing -> Left "Table not found"
executeDelete _ _ = Left "Invalid statement for deletion"

-- Function to delete rows from a DataFrame based on conditions
deleteRows :: DataFrame -> [Condition] -> DataFrame
deleteRows (DataFrame cols rows) conditions = DataFrame cols (filter (not . meetAllConditions cols conditions) rows)

-- Function to filter rows from a DataFrame based on conditions
filterRows :: DataFrame -> [Condition] -> DataFrame
filterRows (DataFrame cols rows) conditions = DataFrame cols (filter (not . meetAllConditions cols conditions) rows)

-- Function to get the current time
getTime :: Execution UTCTime
getTime = liftF $ GetTime id

-- Function to insert values into a table
insertIntoTable :: TableName -> [Value] -> Execution ()
insertIntoTable tableName values = liftF $ InsertIntoTable tableName values ()