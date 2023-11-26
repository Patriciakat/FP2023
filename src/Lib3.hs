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
    getTime,
    runExecuteIO,
    runTestExecuteIO,
    initialInMemoryDB
  )
where

import Lib2 (MyParsedStatement(..), Condition, parseStatement, executeStatement, Value, isValidType)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..))
import Control.Monad.Free (Free (..), liftF)
import Data.Aeson (encode, decode)
import qualified Data.ByteString.Lazy as B
import Data.Time (UTCTime, getCurrentTime)
import Data.Time.Format (formatTime, defaultTimeLocale)
import Data.Maybe (mapMaybe, fromMaybe, maybeToList, catMaybes)
import Data.List (elemIndex, findIndex)

import qualified InMemoryTables as IMT

type TableName = String
type FileContent = B.ByteString
type ErrorMessage = String
type InMemoryDB = [(TableName, DataFrame)]

-- Algebra defining different execution steps for the Free monad
data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName FileContent next
  | ExecuteSqlStatement MyParsedStatement (Either ErrorMessage DataFrame -> next)
  | GetTime (UTCTime -> next)
  | HandleNow (Either ErrorMessage DataFrame -> next)
  | ExecuteSqlStatementInMemory MyParsedStatement (Either ErrorMessage DataFrame -> next)
  deriving (Functor)

type Execution = Free ExecutionAlgebra

-- Function to execute SQL commands, handling different types of statements
executeSql :: Bool -> String -> Execution (Either ErrorMessage DataFrame)
executeSql isTest sql = do
  case parseStatement sql of
    Right parsedStatement ->
      if parsedStatement == Now
      then liftF $ HandleNow id 
      else liftF $ (if isTest then ExecuteSqlStatementInMemory else ExecuteSqlStatement) parsedStatement id
    Left errorMsg ->
      return $ Left errorMsg

-- Function to run the Execution monad
runExecuteIO :: Bool -> Execution r -> IO r
runExecuteIO isTest execution =
  if isTest
  then let (result, _) = runTestExecuteIO execution initialInMemoryDB
       in return result
  else runProductionExecuteIO execution
  where
    runProductionExecuteIO :: Execution r -> IO r
    runProductionExecuteIO (Pure r) = return r
    runProductionExecuteIO (Free step) = do
      next <- runProductionStep step
      runProductionExecuteIO next
    
    runProductionStep :: ExecutionAlgebra a -> IO a
    runProductionStep (LoadFile tableName next) = do
        fileContent <- B.readFile ("db/" ++ tableName ++ ".json")
        return $ next fileContent
    runProductionStep (SaveFile tableName content next) = do
        B.writeFile ("db/" ++ tableName ++ ".json") content
        return next
    runProductionStep (ExecuteSqlStatement statement next) = do
      result <- executeStatement statement
      return $ next result
    runProductionStep (GetTime next) = do
        currentTime <- getCurrentTime
        return $ next currentTime   
    runProductionStep (HandleNow next) = do
      currentTime <- getCurrentTime
      let formattedTime = formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" currentTime
      let timeDataFrame = DataFrame [Column "current_time" StringType] [[StringValue formattedTime]]
      return $ next (Right timeDataFrame)

-- Function to run a command using the test interpreter
runTestExecuteIO :: Execution r -> InMemoryDB -> (r, InMemoryDB)
runTestExecuteIO (Pure r) db = (r, db)
runTestExecuteIO (Free step) db =
  let (next, newDb) = runTestStep step db in
  runTestExecuteIO next newDb

-- Test Interpreter for the Free Monad DSL
runTestStep :: ExecutionAlgebra a -> InMemoryDB -> (a, InMemoryDB)
runTestStep (ExecuteSqlStatementInMemory statement next) db =
  case statement of
    SelectFrom columns tableName -> 
      let result = executeSelectInMemory columns tableName db
      in (next result, db)
    SelectAll tableName -> 
      let result = executeSelectAllInMemory tableName db
      in (next result, db)
    -- Add similar cases for InsertInto, Update, DeleteFrom, etc.
    _ -> (next (Left "Operation not supported in test DSL"), db)

-- Initial in-memory database
initialInMemoryDB :: InMemoryDB
initialInMemoryDB = 
  [ ("employees", snd IMT.tableEmployees),
    ("invalid1", snd IMT.tableInvalid1),
    ("invalid2", snd IMT.tableInvalid2),
    ("long_strings", snd IMT.tableLongStrings),
    ("flags", snd IMT.tableWithNulls),
    ("departments", snd IMT.tableDepartment)
  ]

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

------------------------------------executes for test DSL for each query implementation---------------------------------

--execute for in memory select from

executeSelectInMemory :: [String] -> String -> InMemoryDB -> Either ErrorMessage DataFrame
executeSelectInMemory columns tableName db = 
  case lookup tableName db of
    Just (DataFrame allColumns allRows) ->
      if columns == ["*"]
      then Right (DataFrame allColumns allRows)  -- If selecting all columns, return them as is.
      else 
        -- First, ensure the columns requested exist in allColumns, preserving the order.
        let selectedColumns = filter (\(Column name _) -> name `elem` columns) allColumns
            -- Then, find the index of each selected column in the original allColumns list.
            selectedIndices = mapMaybe (\(Column name _) -> findIndex (\(Column nameAll _) -> name == nameAll) allColumns) selectedColumns
            -- Now, use the indices to extract the corresponding values from each row.
            selectedRows = map (\row -> [row !! idx | idx <- selectedIndices]) allRows
        in Right (DataFrame selectedColumns selectedRows)
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
    
--execute for in memory select all

executeSelectAllInMemory :: String -> InMemoryDB -> Either ErrorMessage DataFrame
executeSelectAllInMemory tableName db = 
  case lookup tableName db of
    Just df -> Right df
    Nothing -> Left $ "Table " ++ tableName ++ " not found"