{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE FlexibleContexts #-}

module Lib2
  ( parseStatement,
    executeStatement,
    convertConditionValueToValue,
    meetAllConditions,
    isValidType,
    MyParsedStatement(..),
    Condition(..),
    ConditionValue(..),
    DataFrame(..), 
    Value(..)
  )
where

import DataFrame
import DataFrame (DataFrame, Column, ColumnType (..), Value (..))
import Lib1 (renderDataFrameAsTable)
import qualified Text.Parsec as P
import Text.Parsec ((<?>))
import Data.Aeson (encode, decode)
import Data.Char (toUpper, toLower)
import Data.List (findIndex, isPrefixOf, isInfixOf, elemIndex, find)
import Data.Maybe (isJust, mapMaybe, catMaybes, fromJust)
import qualified Data.ByteString.Lazy as B

type ErrorMessage = String
type TableName = String
type Database = [(TableName, DataFrame)]

--------------------------------------------------- Data models ----------------------------------------------

data MyParsedStatement
    = SelectFrom { selectedColumns :: [String], fromTable :: TableName }
    | SelectMin { columns :: [String], fromTable :: TableName }
    | SelectWithMin { minColumns :: [String], otherColumns :: [String], fromTable :: TableName }
    | SelectAvg { columns :: [String], fromTable :: TableName }
    | SelectWithConditions { selectedColumns :: [String], fromTable :: TableName, conditions :: [Condition] }
    | DeleteFrom { fromTable :: TableName, conditions :: [Condition] }
    | SelectAll { fromTable :: TableName }
    deriving (Show, Eq)
 
data Condition
    = EqualsCondition String ConditionValue
    | NotEqualsCondition String ConditionValue
    | GreaterThanCondition String ConditionValue
    | LessThanCondition String ConditionValue
    | GreaterThanOrEqualCondition String ConditionValue
    | LessThanOrEqualCondition String ConditionValue
    deriving (Show, Eq)

data ConditionValue = IntegerConditionValue Int | StringConditionValue String deriving (Show, Eq)
    
    
--------------------------------------------------- Parsers ----------------------------------------------------


-- task 3, column list
    
selectParser :: P.Parsec String () MyParsedStatement
selectParser = do
    _ <- caseInsensitiveString "SELECT"
    _ <- P.many P.space
    selectType <- P.try (do
                        _ <- P.char '*'
                        _ <- P.many P.space
                        _ <- caseInsensitiveString "FROM"
                        _ <- P.many P.space
                        tableName <- P.many1 (P.alphaNum P.<|> P.char '_')
                        return $ SelectAll tableName)
                  P.<|> (do
                        cols <- columnsListParser
                        _ <- P.many P.space
                        _ <- caseInsensitiveString "FROM"
                        _ <- P.many P.space
                        tablename <- P.many1 (P.alphaNum P.<|> P.char '_')
                        return $ SelectFrom cols tablename)
    return selectType
        
--task 3, MIN function
        
minParser :: P.Parsec String () MyParsedStatement
minParser = do
    _ <- caseInsensitiveString "SELECT"
    _ <- P.many P.space
    _ <- caseInsensitiveString "MIN("
    columns <- columnsListParser
    _ <- P.string ")"
    _ <- P.many P.space
    _ <- caseInsensitiveString "FROM"
    _ <- P.many P.space
    tablename <- P.many1 (P.alphaNum P.<|> P.char '_')
    return $ SelectMin columns tablename
    
--task 3, MIN function with other columns, e.g.: SELECT MIN(id), surname FROM employees etc.
    
minWithOtherColumnsParser :: P.Parsec String () MyParsedStatement
minWithOtherColumnsParser = do
    _ <- caseInsensitiveString "SELECT"
    _ <- P.many P.space
    _ <- caseInsensitiveString "MIN("
    minCols <- columnsListParser
    _ <- P.string ")"
    _ <- P.many P.space
    _ <- P.char ',' >> P.many P.space
    otherCols <- columnsListParser
    _ <- P.many P.space
    _ <- caseInsensitiveString "FROM"
    _ <- P.many P.space
    tablename <- P.many1 (P.alphaNum P.<|> P.char '_')
    return $ SelectWithMin minCols otherCols tablename
    
--task 3, AVG function

avgParser :: P.Parsec String () MyParsedStatement
avgParser = do
    _ <- caseInsensitiveString "SELECT"
    _ <- P.many P.space
    _ <- caseInsensitiveString "AVG("
    columns <- columnsListParser
    _ <- P.string ")"
    _ <- P.many P.space
    _ <- caseInsensitiveString "FROM"
    _ <- P.many P.space
    tablename <- P.many1 (P.alphaNum P.<|> P.char '_')
    return $ SelectAvg columns tablename
    
--task 3, WHERE with AND, e.g.: SELECT id, surname FROM employees WHERE id=1 AND name="Vi" (AND is not required, and it still works)
--task 3, WHERE int =/</>/<=/>=/!=, e.g.: SELECT id, name, surname WHERE name="Ed" AND id (=/</>/<=/>=/!=) <some_number>

convertValueToConditionValue :: Value -> ConditionValue
convertValueToConditionValue (IntegerValue int) = IntegerConditionValue (fromIntegral int)
convertValueToConditionValue (StringValue str) = StringConditionValue str

valueParser = P.try (P.many1 P.digit >>= \digits -> return $ IntegerValue (read digits))
         P.<|> (P.between (P.char '\'') (P.char '\'') (P.many (P.noneOf "'")) >>= \str -> return $ StringValue str)
         P.<|> (P.between (P.char '"') (P.char '"') (P.many (P.noneOf "\"")) >>= \str -> return $ StringValue str)
        

conditionParser :: P.Parsec String () Condition
conditionParser = P.choice
    [ P.try tryNotEqualsCondition
    , P.try tryEqualsCondition
    , P.try tryGreaterThanCondition
    , P.try tryLessThanCondition
    , P.try tryGreaterThanOrEqualCondition
    , P.try tryLessThanOrEqualCondition
    ]
    
  where
    tryEqualsCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string "="
        _ <- P.many P.space
        value <- valueParser
        let condValue = convertValueToConditionValue value
        return $ EqualsCondition column condValue
    
    tryNotEqualsCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string "!="
        _ <- P.many P.space
        value <- valueParser
        let condValue = convertValueToConditionValue value
        return $ NotEqualsCondition column condValue
    
    tryGreaterThanCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string ">"
        _ <- P.many P.space
        value <- valueParser
        let condValue = convertValueToConditionValue value
        return $ GreaterThanCondition column condValue
    
    tryGreaterThanOrEqualCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string ">="
        _ <- P.many P.space
        value <- valueParser
        let condValue = convertValueToConditionValue value
        return $ GreaterThanOrEqualCondition column condValue
    
    tryLessThanCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string "<"
        _ <- P.many P.space
        value <- valueParser
        let condValue = convertValueToConditionValue value
        return $ LessThanCondition column condValue
    
    tryLessThanOrEqualCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string "<="
        _ <- P.many P.space
        value <- valueParser
        let condValue = convertValueToConditionValue value
        return $ LessThanOrEqualCondition column condValue
         
selectWithWhereParser :: P.Parsec String () MyParsedStatement
selectWithWhereParser = do
    selectStmt <- selectParser
    _ <- P.many P.space
    _ <- caseInsensitiveString "WHERE"
    _ <- P.many P.space
    conditions <- conditionParser `P.sepBy1` (P.many P.space >> caseInsensitiveString "AND" >> P.many P.space)
    case selectStmt of
        SelectAll tableName -> return $ SelectWithConditions ["*"] tableName conditions
        _ -> return $ SelectWithConditions (selectedColumns selectStmt) (fromTable selectStmt) conditions
        
-- Delete parser
        
deleteParser :: P.Parsec String () MyParsedStatement
deleteParser = do
    _ <- caseInsensitiveString "DELETE FROM"
    _ <- P.many P.space
    tableName <- P.many1 (P.alphaNum P.<|> P.char '_')
    _ <- P.many P.space
    _ <- caseInsensitiveString "WHERE"
    _ <- P.many P.space
    conditions <- conditionParser `P.sepBy1` (P.many P.space >> caseInsensitiveString "AND" >> P.many P.space)
    return $ DeleteFrom tableName conditions

-- Parses user input into an entity representing a parsed statement
parseStatement :: String -> Either ErrorMessage MyParsedStatement
parseStatement input
    | "DELETE FROM" `isPrefixOf` (map toUpper input) = 
            case P.parse deleteParser "" input of
                Left err -> Left $ "Parse Error: " ++ show err
                Right stmt -> Right stmt
    | "SELECT AVG(" `isPrefixOf` (map toUpper input) = 
        case P.parse avgParser "" input of
            Left err -> Left $ "Parse Error: " ++ show err
            Right stmt -> Right stmt
    | "SELECT MIN(" `isPrefixOf` (map toUpper input) = 
            case P.parse (P.try minWithOtherColumnsParser P.<|> minParser) "" input of
                Left err -> Left $ "Parse Error: " ++ show err
                Right stmt -> Right stmt
    | "WHERE" `isInfixOf` (map toUpper input) = 
            case P.parse selectWithWhereParser "" input of
                Left err -> Left $ "Parse Error: " ++ show err
                Right stmt -> Right stmt
    | "SELECT" `isPrefixOf` (map toUpper input) = 
        case P.parse selectParser "" input of
            Left err -> Left $ "Parse Error: " ++ show err
            Right stmt -> Right stmt
    | otherwise = Left "Invalid SQL command"
      
        
--------------------------------------------------- Executes ----------------------------------------------------    
    
    
-- Executes a parsed statement. Produces a DataFrame. Uses
-- json files are source of data.

--execute for MIN function

executeStatement :: MyParsedStatement -> IO (Either ErrorMessage DataFrame)
executeStatement (SelectMin columns tableName) = do
    result <- readDataFrame (tableName ++ ".json")
    case result of
        Just (DataFrame allColumns allRows) -> do
            let minValues = map (\column -> 
                        let columnIndex = elemIndex column (map columnName allColumns) in
                        case columnIndex of
                            Just idx -> 
                                let colValues = map (\row -> case row !! idx of 
                                                                  StringValue str -> read str :: Int 
                                                                  IntegerValue int -> fromIntegral int
                                                                  _ -> error "Unsupported value type.") allRows
                                    minValue = minimum colValues
                                in IntegerValue (fromIntegral minValue)
                            Nothing -> error $ "Column " ++ column ++ " not found") columns
            return $ Right $ DataFrame (map (\c -> Column c IntegerType) columns) [minValues]
        Nothing -> return $ Left "Table not found"
  
--execute for SELECT column, column, ... FROM tablename (column list)        
        
executeStatement (SelectFrom selectedCols tableName) = do
    result <- readDataFrame (tableName ++ ".json")
    case result of
        Just (DataFrame allColumns allRows) ->
            let allColumnNames = map columnName allColumns
                finalSelectedCols = if selectedCols == ["*"]
                                    then allColumnNames
                                    else selectedCols
                validIndices = mapMaybe (`elemIndex` allColumnNames) finalSelectedCols
                newRows = map (\row -> map (row !!) validIndices) allRows
                newCols = map (allColumns !!) validIndices
            in return $ Right $ DataFrame newCols newRows
        Nothing -> return $ Left "Table not found"
        
--execute for SELECT * FROM tablename

executeStatement (SelectAll tableName) = do
    result <- readDataFrame (tableName ++ ".json")
    case result of
        Just df -> return $ Right df
        Nothing -> return $ Left "Table not found"
        
--execute for MIN function and other columns, e.g.: SELECT MIN(column), column, column, ... FROM tablename
        
executeStatement (SelectWithMin minCols otherCols tableName) = do
    result <- readDataFrame (tableName ++ ".json")
    case result of
        Just (DataFrame allColumns allRows) -> do
            let minValues = map (\column -> 
                        let columnIndex = elemIndex column (map columnName allColumns) in
                        case columnIndex of
                            Just idx -> 
                                let colValues = mapMaybe (\row -> case row !! idx of 
                                                                  StringValue str -> Just (read str :: Int) 
                                                                  IntegerValue int -> Just (fromIntegral int)
                                                                  _ -> Nothing) allRows
                                in if null colValues
                                   then IntegerValue 0
                                   else IntegerValue (fromIntegral $ minimum colValues)
                            Nothing -> error $ "Column " ++ column ++ " not found") minCols
            if null allRows
            then return $ Right $ DataFrame (map (\c -> Column c IntegerType) minCols) []
            else do
                let minRowIndices = map (\col -> fromJust $ elemIndex col (map columnName allColumns)) minCols
                let minRow = head $ filter (\row -> all (\(idx, minVal) -> row !! idx == minVal) (zip minRowIndices minValues)) allRows
                let otherValues = map (\col -> minRow !! (fromJust $ elemIndex col (map columnName allColumns))) otherCols
                return $ Right $ DataFrame (map (\c -> Column c IntegerType) minCols ++ map (\c -> Column c (fromJust $ columnTypeByName c allColumns)) otherCols) [minValues ++ otherValues]
        Nothing -> return $ Left "Table not found"
        
--execute for AVG function

executeStatement (SelectAvg columns tableName) = do
    result <- readDataFrame (tableName ++ ".json")
    case result of
        Just (DataFrame allColumns allRows) -> do
            let avgValues = map (\column -> 
                        let columnIndex = elemIndex column (map columnName allColumns) in
                        case columnIndex of
                            Just idx -> 
                                let colValues = map (\row -> case row !! idx of 
                                                                  StringValue str -> read str :: Float 
                                                                  IntegerValue int -> fromIntegral int
                                                                  _ -> error "Unsupported value type.") allRows
                                    avgValue = sum colValues / fromIntegral (length colValues)
                                in FloatValue avgValue
                            Nothing -> error $ "Column " ++ column ++ " not found") columns
            return $ Right $ DataFrame (map (\c -> Column c FloatType) columns) [avgValues]
        Nothing -> return $ Left "Table not found"
        
--execute for WHERE with AND

executeStatement (SelectWithConditions selectedCols tableName conditions) = do
    result <- readDataFrame (tableName ++ ".json")
    case result of
        Just df -> do
            let DataFrame allColumns allRows = df
                finalSelectedCols = if selectedCols == ["*"]
                                    then map columnName allColumns
                                    else selectedCols
                validIndices = if selectedCols == ["*"]
                               then [0 .. length allColumns - 1]
                               else mapMaybe (\colName -> findIndex ((== colName) . columnName) allColumns) finalSelectedCols
                newRows = map (\row -> map (row !!) validIndices) (filter (meetAllConditions allColumns conditions) allRows)
                newCols = if selectedCols == ["*"]
                          then allColumns
                          else map (allColumns !!) validIndices
            if null newRows
            then return $ Left "No rows found matching the conditions"
            else return $ Right $ DataFrame newCols newRows
        Nothing -> return $ Left "Table not found"
     
--execute for delete

executeStatement (DeleteFrom tableName conditions) = do
    result <- readDataFrame (tableName ++ ".json")
    case result of
        Just df -> do
            let updatedDF = applyDeleteConditions conditions df
            saveDataFrame tableName updatedDF
            return $ Right updatedDF
        Nothing -> return $ Left "Table not found"
executeStatement _ = 
    return $ Left "This type of statement is not supported"

-------------------------------------------------- Helper functions -------------------------------------------------- 
 
instance Ord Value where
    compare (IntegerValue int1) (IntegerValue int2) = compare int1 int2
    compare (StringValue str1) (StringValue str2) = compare str1 str2

columnName :: Column -> String
columnName (Column name _) = name

getColumnNamesForTable :: TableName -> IO (Either ErrorMessage [String])
getColumnNamesForTable tableName = do
    result <- readDataFrame (tableName ++ ".json") 
    case result of
        Just (DataFrame cols _) -> return $ Right $ map columnName cols
        Nothing -> return $ Left "Table not found or JSON error"

columnNameParser :: P.Parsec String () String
columnNameParser = P.many1 (P.alphaNum P.<|> P.char '_')

columnsListParser :: P.Parsec String () [String]
columnsListParser = columnNameParser `P.sepBy1` (P.char ',' >> P.many P.space)

elemAt :: Int -> [a] -> Maybe a
elemAt idx xs
  | idx < 0 || idx >= length xs = Nothing
  | otherwise                   = Just (xs !! idx)
  
columnTypeByName :: String -> [Column] -> Maybe ColumnType
columnTypeByName colName cols = columnType <$> find (\(Column name _) -> name == colName) cols

columnType :: Column -> ColumnType
columnType (Column _ t) = t

-- Helper function to check if the provided value matches the column type
isValidType :: (Column, Value) -> Bool
isValidType (Column _ colType, val) =
    case (colType, val) of
        (IntegerType, IntegerValue _) -> True
        (StringType, StringValue _)   -> True
        (BoolType, BoolValue _)       -> True
        (FloatType, FloatValue _)     -> True
        _                             -> False

-- helper functions for WHERE with AND
-- conditions functions for WHERE int =/</>/<=/>=/!=

applyConditions :: [Condition] -> DataFrame -> DataFrame
applyConditions conditions (DataFrame cols rows) = DataFrame cols (filter (meetAllConditions cols conditions) rows)

convertConditionValueToValue :: ConditionValue -> Value
convertConditionValueToValue (IntegerConditionValue int) = IntegerValue (toInteger int)
convertConditionValueToValue (StringConditionValue str) = StringValue str

meetAllConditions :: [Column] -> [Condition] -> Row -> Bool
meetAllConditions columns conditions row = all (\cond -> meetCondition columns cond row) conditions

meetCondition :: [Column] -> Condition -> Row -> Bool
meetCondition columns (EqualsCondition colName condValue) row =
    case elemIndex colName (map columnName columns) of
        Just idx -> (row !! idx) == convertConditionValueToValue condValue
        Nothing -> False

meetCondition columns (NotEqualsCondition colName condValue) row =
    case elemIndex colName (map columnName columns) of
        Just idx -> (row !! idx) /= convertConditionValueToValue(condValue)
        Nothing -> False

meetCondition columns (GreaterThanCondition colName condValue) row =
    case elemIndex colName (map columnName columns) of
        Just idx -> (row !! idx) > convertConditionValueToValue condValue
        Nothing -> False

meetCondition columns (LessThanCondition colName condValue) row =
    case elemIndex colName (map columnName columns) of
        Just idx -> (row !! idx) < convertConditionValueToValue condValue
        Nothing -> False

meetCondition columns (GreaterThanOrEqualCondition colName condValue) row =
    case elemIndex colName (map columnName columns) of
        Just idx -> (row !! idx) >= convertConditionValueToValue condValue
        Nothing -> False

meetCondition columns (LessThanOrEqualCondition colName condValue) row =
    case elemIndex colName (map columnName columns) of
        Just idx -> (row !! idx) <= convertConditionValueToValue condValue
        Nothing -> False

-- helper function for case insensitiveness for all sql keywords: select, from, min, avg, where, etc.

caseInsensitiveString :: String -> P.Parsec String () String
caseInsensitiveString s = P.try (mapM caseInsensitiveChar s)
    where
        caseInsensitiveChar c = P.char (toLower c) P.<|> P.char (toUpper c)
        
-- Data frame reader and save for .json files
        
readDataFrame :: FilePath -> IO (Maybe DataFrame)
readDataFrame fileName = do
  let filePath = "db/" ++ fileName -- Prefix the file name with the directory
  jsonContent <- B.readFile filePath
  return $ decode jsonContent
  
saveDataFrame :: TableName -> DataFrame -> IO ()
saveDataFrame tableName df = do
    let jsonContent = encode df
    let filePath = "db/" ++ tableName ++ ".json"
    B.writeFile filePath jsonContent
    
--Delete helper functions

applyDeleteConditions :: [Condition] -> DataFrame -> DataFrame
applyDeleteConditions conditions (DataFrame cols rows) = 
    DataFrame cols (filter (not . meetAllConditions cols conditions) rows)