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
import Data.Maybe (mapMaybe, fromMaybe, maybeToList, catMaybes, fromJust, mapMaybe)
import Data.List (elemIndex, findIndex, find, transpose)

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
    SelectMin columns tableName ->
      let result = executeSelectMinInMemory columns tableName db
      in (next result, db)
    SelectWithMin minCols otherCols tableName ->
      let result = executeSelectWithMinInMemory minCols otherCols tableName db
      in (next result, db)
    SelectAvg columns tableName ->
      let result = executeSelectAvgInMemory columns tableName db
      in (next result, db)
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
  
  
-------------------------------------------------helper functions-------------------------------------------------------


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

-- Helper function to find the minimum value in a list of Maybe Values
minValue :: [Maybe Value] -> Value
minValue values = 
  let numericValues = catMaybes values
  in case numericValues of
      (IntegerValue x : xs) -> IntegerValue $ minimum $ x : map (\(IntegerValue i) -> i) xs
      (StringValue s : xs) -> StringValue $ minimum $ s : map (\(StringValue str) -> str) xs
      _ -> IntegerValue 0
      
-- Function to get column name from a Column
columnName :: Column -> String
columnName (Column name _) = name

-- Function to find the column type by name
columnTypeByName :: String -> [Column] -> ColumnType
columnTypeByName colName columns = 
    case find (\(Column name _) -> name == colName) columns of
        Just (Column _ colType) -> colType
        Nothing -> error $ "Column not found: " ++ colName
        
findMinRowIndices :: [Value] -> [Column] -> [[Value]] -> [Int]
findMinRowIndices minValues allColumns allRows = 
  let columnIndexMap = zip (map (\(Column colName _) -> colName) allColumns) [0..]
      -- Find indices for each column where minValues were found
      minValIndices = map (\minVal -> fromMaybe (-1) $ findIndex (\(Column _ colType) -> matchesMinValue colType minVal) allColumns) minValues
      -- Filter rows where each minVal is present at the respective column index
      matchingRows = filter (\row -> all (\(idx, val) -> (row !! idx) == val) (zip minValIndices minValues)) allRows
  in map fst $ filter (not . null . snd) $ zip [0..] matchingRows

-- Helper function to match a Value with a ColumnType
matchesMinValue :: ColumnType -> Value -> Bool
matchesMinValue colType val =
  case (colType, val) of
    (IntegerType, IntegerValue _) -> True
    (StringType, StringValue _) -> True
    -- Add other type matches as necessary
    _ -> False

-- Select rows with minimum values
selectRowsWithMinValues :: [Int] -> [[Value]] -> [[Value]]
selectRowsWithMinValues rowIndices allRows =
  [allRows !! rowIndex | rowIndex <- rowIndices]
  
-- Helper function to extract numeric value from Value type
numericValue :: Value -> Maybe Float
numericValue (IntegerValue int) = Just $ fromIntegral int
numericValue (StringValue str) = Just $ read str
numericValue _ = Nothing


------------------------------------executes for test DSL for each query implementation---------------------------------


-- Execute for in memory select from

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
    
-- Execute for in memory select all

executeSelectAllInMemory :: String -> InMemoryDB -> Either ErrorMessage DataFrame
executeSelectAllInMemory tableName db = 
  case lookup tableName db of
    Just df -> Right df
    Nothing -> Left $ "Table " ++ tableName ++ " not found"

-- Execute for in memory MIN function
executeSelectMinInMemory :: [String] -> String -> InMemoryDB -> Either ErrorMessage DataFrame
executeSelectMinInMemory columns tableName db = 
  case lookup tableName db of
    Just (DataFrame allColumns allRows) ->
      let minValues = map (\column -> 
                    let columnValues = map (\row -> lookup column (zip (map (\(Column colName _) -> colName) allColumns) row)) allRows
                    in minValue columnValues) columns
      in Right $ DataFrame (map (\c -> Column ("min(" ++ c ++ ")") IntegerType) columns) [minValues]
    Nothing -> Left $ "Table " ++ tableName ++ " not found"

-- Execute for in memory MIN function and other columns
executeSelectWithMinInMemory :: [String] -> [String] -> String -> InMemoryDB -> Either ErrorMessage DataFrame
executeSelectWithMinInMemory minCols otherCols tableName db = 
  case lookup tableName db of
    Just (DataFrame allColumns allRows) ->
      let columnIndexMap = zip (map (\(Column colName _) -> colName) allColumns) [0..]
          minValIndices = map (\minCol -> fromMaybe (-1) $ lookup minCol columnIndexMap) minCols
          otherColIndices = map (\otherCol -> fromMaybe (-1) $ lookup otherCol columnIndexMap) otherCols

          minValues = map (\idx -> minimum $ map (!! idx) allRows) minValIndices
          matchingRow = find (\row -> all (\(idx, val) -> (row !! idx) == val) (zip minValIndices minValues)) allRows

      in case matchingRow of
           Just row -> Right $ DataFrame 
                        (map (\c -> Column ("min(" ++ c ++ ")") IntegerType) minCols ++ 
                         map (\c -> let Just (Column _ colType) = find (\(Column colName _) -> colName == c) allColumns
                                    in Column c colType) otherCols) 
                        [[row !! idx | idx <- minValIndices ++ otherColIndices]]
           Nothing -> Left "No matching row found"
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
    
-- Execute for in memory AVG function

executeSelectAvgInMemory :: [String] -> String -> InMemoryDB -> Either ErrorMessage DataFrame
executeSelectAvgInMemory columns tableName db = 
  case lookup tableName db of
    Just (DataFrame allColumns allRows) ->
      let avgValues = map (\column -> 
                    let columnIndex = findIndex (\(Column colName _) -> colName == column) allColumns
                    in case columnIndex of
                        Just idx -> 
                            let colValues = catMaybes $ map (\row -> numericValue (row !! idx)) allRows
                                avgValue = if null colValues 
                                           then 0 
                                           else sum colValues / fromIntegral (length colValues)
                            in FloatValue avgValue
                        Nothing -> error $ "Column " ++ column ++ " not found") columns
      in Right $ DataFrame (map (\c -> Column ("avg(" ++ c ++ ")") FloatType) columns) [avgValues]
    Nothing -> Left $ "Table " ++ tableName ++ " not found"