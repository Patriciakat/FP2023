{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE FlexibleContexts #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement(..),
    Condition(..),
    ConditionValue(..),
    DataFrame(..), 
    Value(..)
  )
where

import DataFrame
import InMemoryTables (TableName)
import Lib1 (renderDataFrameAsTable)
import qualified Text.Parsec as P
import Text.Parsec ((<?>))
import Data.Char (toUpper, toLower)
import Data.List (isPrefixOf, isInfixOf, elemIndex, find)
import Data.Maybe (isJust, mapMaybe, catMaybes, fromJust)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors

--------------------------------------------------- Data models ----------------------------------------------

data ParsedStatement
    = ShowTables
    | ShowTable TableName
    | SelectFrom { selectedColumns :: [String], fromTable :: TableName }
    | SelectMin { columns :: [String], fromTable :: TableName }
    | SelectWithMin { minColumns :: [String], otherColumns :: [String], fromTable :: TableName }
    | SelectAvg { columns :: [String], fromTable :: TableName }
    | SelectWithConditions { selectedColumns :: [String], fromTable :: TableName, conditions :: [Condition] }
    | StatementWithFilters { columns :: [String], database :: String, filters :: [String] }
    | StatementWithoutFilters { columns :: [String], database :: String }
    | StatementSelectAll { database :: String }
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


-- Features:
-- - Basic column selection (e.g., SELECT column1, column2 FROM table)
-- - Wildcard selection (e.g., SELECT * FROM table)
-- - SHOW TABLES, SHOW TABLE <tablename>
-- - Column list, MIN function with/without columns, AVG function, WHERE with AND

-- task 1, "SHOW TABLES"

showTablesParser :: P.Parsec String () ParsedStatement
showTablesParser = do
    _ <- caseInsensitiveString "SHOW" <?> "SHOW keyword"
    _ <- P.many P.space
    _ <- caseInsensitiveString "TABLES" <?> "TABLES keyword"
    return ShowTables

-- task 2, "SHOW TABLE name"

showTableParser :: P.Parsec String () ParsedStatement
showTableParser = do
    _ <- caseInsensitiveString "SHOW" <?> "SHOW keyword"
    _ <- P.many P.space
    _ <- caseInsensitiveString "TABLE" <?> "TABLE keyword"
    _ <- P.many P.space
    tableName <- P.many1 (P.alphaNum P.<|> P.char '_') <?> "Table name"
    return $ ShowTable tableName

-- task 3, column list
    
selectParser :: P.Parsec String () ParsedStatement
selectParser = do
    _ <- caseInsensitiveString "SELECT"
    _ <- P.many P.space
    selectType <- P.try (do
                        _ <- P.char '*'
                        _ <- P.many P.space
                        _ <- caseInsensitiveString "FROM"
                        _ <- P.many P.space
                        tablename <- P.many1 (P.alphaNum P.<|> P.char '_')
                        return $ StatementSelectAll tablename)
                  P.<|> (do
                        cols <- columnsListParser
                        _ <- P.many P.space
                        _ <- caseInsensitiveString "FROM"
                        _ <- P.many P.space
                        tablename <- P.many1 (P.alphaNum P.<|> P.char '_')
                        return $ SelectFrom cols tablename)
    return selectType
        
--task 3, MIN function
        
minParser :: P.Parsec String () ParsedStatement
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
    
minWithOtherColumnsParser :: P.Parsec String () ParsedStatement
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

avgParser :: P.Parsec String () ParsedStatement
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
    tryLessThanCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string "<"
        _ <- P.many P.space
        value <- valueParser
        return $ LessThanCondition column value

    tryEqualsCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string "="
        _ <- P.many P.space
        value <- valueParser
        return $ EqualsCondition column value

    tryNotEqualsCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string "!="
        _ <- P.many P.space
        value <- valueParser
        return $ NotEqualsCondition column value

    tryGreaterThanCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string ">"
        _ <- P.many P.space
        value <- valueParser
        return $ GreaterThanCondition column value

    tryGreaterThanOrEqualCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string ">="
        _ <- P.many P.space
        value <- valueParser
        return $ GreaterThanOrEqualCondition column value

    tryLessThanOrEqualCondition = do
        column <- columnNameParser
        _ <- P.many P.space
        _ <- P.string "<="
        _ <- P.many P.space
        value <- valueParser
        return $ LessThanOrEqualCondition column value

    
valueParser :: P.Parsec String () ConditionValue
valueParser = P.try (P.many1 P.digit >>= \digits -> return $ IntegerConditionValue (read digits))
         P.<|> (P.char '"' >> P.manyTill P.anyChar (P.char '"') >>= \str -> return $ StringConditionValue str)
         
selectWithWhereParser :: P.Parsec String () ParsedStatement
selectWithWhereParser = do
    selectStmt <- selectParser  -- Use the select parser you already have
    _ <- P.many P.space
    _ <- caseInsensitiveString "WHERE"
    _ <- P.many P.space
    conditions <- conditionParser `P.sepBy1` (P.many P.space >> caseInsensitiveString "AND" >> P.many P.space)
    return $ SelectWithConditions (selectedColumns selectStmt) (fromTable selectStmt) conditions


-- Parses user input into an entity representing a parsed statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
    | "SHOW" `isPrefixOf` (map toUpper input) = 
                case P.parse (P.try showTablesParser P.<|> showTableParser) "" input of
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
-- InMemoryTables.databases a source of data.


-- execute for "SHOW TABLES"

executeStatement :: ParsedStatement -> Database -> Either ErrorMessage DataFrame
executeStatement ShowTables db = 
    let tableNames = map fst db 
    in Right $ DataFrame [Column "Tables" StringType] (map (\name -> [StringValue name]) tableNames)

-- execute for "SHOW TABLE name"

executeStatement (ShowTable tableName) db = 
    case lookup tableName db of
        Just (DataFrame columns _) -> 
            let columnNames = map columnName columns
            in Right $ DataFrame [Column "Columns" StringType] (map (\name -> [StringValue name]) columnNames)
        Nothing -> Left "Table not found"

--execute for MIN function

executeStatement (SelectMin columns tableName) db = 
    case lookup tableName db of
        Just (DataFrame allColumns allRows) -> 
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
            in Right $ DataFrame (map (\c -> Column c IntegerType) columns) [minValues]
        Nothing -> Left "Table not found"
  
--execute for SELECT column, column, ... FROM tablename (column list)        
        
executeStatement (SelectFrom selectedCols tableName) db = 
    case lookup tableName db of
        Just (DataFrame allColumns allRows) ->
            let validIndices = mapMaybe (\col -> 
                                         let colType = columnTypeByName col allColumns in
                                         case colType of 
                                             Just t -> elemIndex (Column col t) allColumns
                                             Nothing -> Nothing) selectedCols
                newRows = map (\row -> map (row !!) validIndices) allRows
                newCols = map (allColumns !!) validIndices
            in Right $ DataFrame newCols newRows
        Nothing -> Left "Table not found"
        
--execute for MIN function and other columns, e.g.: SELECT MIN(column), column, column, ... FROM tablename
        
executeStatement (SelectWithMin minCols otherCols tableName) db = 
    case lookup tableName db of
        Just (DataFrame allColumns allRows) -> 
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
                            Nothing -> error $ "Column " ++ column ++ " not found") minCols

                -- Find the row with the minimum value for the minCols column
                minRow = head $ filter (\row -> (case row !! (fromJust $ elemIndex (head minCols) (map columnName allColumns)) of
                                                     IntegerValue int -> int == (case head minValues of
                                                                                      IntegerValue minValue -> minValue
                                                                                      _ -> error "Unexpected value type.")
                                                     _ -> False)) allRows
                otherValues = map (\col -> minRow !! (fromJust $ elemIndex col (map columnName allColumns))) otherCols
                
            in Right $ DataFrame (map (\c -> Column c IntegerType) minCols ++ map (\c -> Column c (fromJust $ columnTypeByName c allColumns)) otherCols) [minValues ++ otherValues]
        Nothing -> Left "Table not found"
        
--execute for AVG function

executeStatement (SelectAvg columns tableName) db = 
    case lookup tableName db of
        Just (DataFrame allColumns allRows) -> 
            let avgValues = map (\column -> 
                        let columnIndex = elemIndex column (map columnName allColumns) in
                        case columnIndex of
                            Just idx -> 
                                let colValues = map (\row -> case row !! idx of 
                                                                  StringValue str -> read str :: Float 
                                                                  IntegerValue int -> fromIntegral int
                                                                  _ -> error "Unsupported value type.") allRows
                                    avgValue = sum colValues / (fromIntegral (length colValues))
                                in FloatValue avgValue  -- Ensuring it's wrapped in FloatValue
                            Nothing -> error $ "Column " ++ column ++ " not found") columns
            in Right $ DataFrame (map (\c -> Column c FloatType) columns) [avgValues]
        Nothing -> Left "Table not found"
        
--execute for WHERE with AND

executeStatement (SelectWithConditions selectedCols tablename conditions) db =
    case lookup tablename db of
        Just df -> do
            let dfWithConditionsApplied = applyConditions conditions df
            let DataFrame allColumns allRows = dfWithConditionsApplied

            -- If selectedCols is "*", select all columns
            let finalSelectedCols = if selectedCols == ["*"]
                                    then map (\(Column name _) -> name) allColumns
                                    else selectedCols

            -- Extract only the columns specified in the SELECT statement
            let validIndices = mapMaybe (\col ->
                                         let colType = columnTypeByName col allColumns in
                                         case colType of
                                             Just t -> elemIndex (Column col t) allColumns
                                             Nothing -> Nothing) finalSelectedCols
            let newRows = map (\row -> map (row !!) validIndices) allRows
            let newCols = map (allColumns !!) validIndices

            Right $ DataFrame newCols newRows
        Nothing -> Left "Table not found"
        
--execute for SELECT * FROM tablename

executeStatement (StatementSelectAll tableName) db = 
    case lookup tableName db of
        Just df -> Right df
        Nothing -> Left "Table not found"

executeStatement _ _ = Left "Statement not supported or invalid"


-------------------------------------------------- Helper functions -------------------------------------------------- 
 
instance Ord Value where
    compare (IntegerValue int1) (IntegerValue int2) = compare int1 int2
    compare (StringValue str1) (StringValue str2) = compare str1 str2

convertConditionValueToValue :: ConditionValue -> Value
convertConditionValueToValue (IntegerConditionValue int) = IntegerValue (toInteger int)
convertConditionValueToValue (StringConditionValue str) = StringValue str

columnName :: Column -> String
columnName (Column name _) = name

-- extracts the type from a Column data structure
columnType :: Column -> ColumnType
columnType (Column _ t) = t

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

-- helper functions for WHERE with AND

applyConditions :: [Condition] -> DataFrame -> DataFrame
applyConditions conditions (DataFrame cols rows) = DataFrame cols (filter (meetAllConditions cols conditions) rows)

meetAllConditions :: [Column] -> [Condition] -> Row -> Bool
meetAllConditions columns conditions row = all (\cond -> meetCondition columns cond row) conditions

meetCondition :: [Column] -> Condition -> Row -> Bool
meetCondition columns (EqualsCondition colName condValue) row =
    case elemIndex colName (map columnName columns) of
        Just idx -> (row !! idx) == convertConditionValueToValue condValue
        Nothing -> False

-- conditions functions for WHERE int =/</>/<=/>=/!=

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