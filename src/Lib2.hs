{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE FlexibleContexts #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame
import InMemoryTables (TableName)
import Lib1 (renderDataFrameAsTable)
import qualified Text.Parsec as P
import Text.Parsec ((<?>))
import Data.Char (toUpper, toLower)
import Data.List (isPrefixOf, elemIndex, find)
import Data.Maybe (isJust, mapMaybe, catMaybes, fromJust)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
    = ShowTables
    | ShowTable TableName
    | SelectFrom { selectedColumns :: [String], fromTable :: TableName }
    | SelectMin { columns :: [String], fromTable :: TableName }
    | SelectWithMin { minColumns :: [String], otherColumns :: [String], fromTable :: TableName }
    | SelectAvg { columns :: [String], fromTable :: TableName }
    | StatementWithFilters { columns :: [String], database :: String, filters :: [String] }
    | StatementWithoutFilters { columns :: [String], database :: String }
    | StatementSelectAll { database :: String }
    deriving (Show, Eq)
    
    
--------------------------------------------------- Parser ----------------------------------------------------


-- Features:
-- - Basic column selection (e.g., SELECT column1, column2 FROM table)
-- - Wildcard selection (e.g., SELECT * FROM table)

columnNameParser :: P.Parsec String () String
columnNameParser = P.many1 (P.alphaNum P.<|> P.char '_')
columnsListParser :: P.Parsec String () [String]
columnsListParser = columnNameParser `P.sepBy1` (P.char ',' >> P.many P.space)

-- task 3, column list
    
selectParser :: P.Parsec String () ParsedStatement
selectParser = do
    _ <- caseInsensitiveString "SELECT" <?> "SELECT keyword"
    _ <- P.many P.space
    selectType <- (P.try (P.char '*' >> return (StatementSelectAll "")) <?> "Wildcard '*'")
               P.<|> ((columnsListParser >>= \cols -> return (SelectFrom cols "")) <?> "Column list")
    _ <- P.many P.space
    _ <- caseInsensitiveString "FROM" <?> "FROM keyword"
    _ <- P.many P.space
    tablename <- P.many1 (P.alphaNum P.<|> P.char '_') <?> "Table name"
    case selectType of
        StatementSelectAll _ -> return $ StatementSelectAll tablename
        SelectFrom cols _   -> return $ SelectFrom cols tablename
        
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

-- Parses user input into an entity representing a parsed statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
    | "SELECT AVG(" `isPrefixOf` (map toUpper input) = 
        case P.parse avgParser "" input of
            Left err -> Left $ "Parse Error: " ++ show err
            Right stmt -> Right stmt
    | "SELECT MIN(" `isPrefixOf` (map toUpper input) = 
            case P.parse (P.try minWithOtherColumnsParser P.<|> minParser) "" input of
                Left err -> Left $ "Parse Error: " ++ show err
                Right stmt -> Right stmt
    | "SELECT" `isPrefixOf` (map toUpper input) = 
        case P.parse selectParser "" input of
            Left err -> Left $ "Parse Error: " ++ show err
            Right stmt -> Right stmt
    | otherwise = Left "Invalid SQL command"

        
        
--------------------------------------------------- Execute ----------------------------------------------------    
    
    
-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.

--execute for MIN function

executeStatement :: ParsedStatement -> Database -> Either ErrorMessage DataFrame
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
        
--execute for SELECT * FROM tablename

executeStatement (StatementSelectAll tableName) db = 
    case lookup tableName db of
        Just df -> Right df
        Nothing -> Left "Table not found"

executeStatement _ _ = Left "Statement not supported or invalid"


-------------------------------------------------- helper functions -------------------------------------------------- 


-- iterates through a string until reaches a specific element
-- egz.:
-- iterateUntil "grabber hands" 'n'
-- >> "grabber ha"
-- note that it doesn't return the specified element on the end
iterateUntil :: String -> Char -> [Char]
iterateUntil [] _ = []
iterateUntil (x:xs) a
  |x /= a = x : iterateUntil xs a
  |x == a = []

-- splits a string into words by using a specific element
-- egz.:
-- splitString "the quick brown     fox jumped     over the lazy dog" ' ' 
-- >> ["the","quick","brown","fox","jumped","over","the","lazy","dog"]
splitString :: String -> Char -> [String]
splitString [] _ = []
splitString statement a
 | head statement == a = (splitString . tail $ statement) a
 | otherwise = (iterateUntil statement a) : splitString (drop ( (1 + ) . length $ iterateUntil statement a)  statement) a

-- separate a certain element from from string
-- egz.:
-- separateElement ",my,spoon,,is too,big," ','
-- >> ["," "my" "," "spoon" "," "," "is too" "," "big" ","]
separateElement :: String -> Char -> [String]
separateElement [] _ = []
separateElement (x:xs) a 
 | x == a = (x : []) : (separateElement xs a)
 | elem a (x:xs) = (iterateUntil (x:xs) a) : (a : []) : separateElement (drop ( (1 + ) . length $ iterateUntil (x:xs) a)  (x:xs) ) a
 | otherwise = (x:xs) : []

-- splits a query into words and separates commas
-- egz.: 
-- splitToComponents "grass, grass,brass,,sash"
-- >> ["grass" "," "grass" "," "brass" "," "," "sash"]
splitToComponents :: String -> [String]
splitToComponents [] = []
splitToComponents query = 
  let 
    words = splitString query ' '
    y = head words
    ys = tail words

    temp :: [String] -> [String]
    temp [] = []
    temp (x:[]) = separateElement x ','
    temp (x:xs) = separateElement x ',' ++ temp xs
  in 
    separateElement y ',' ++ temp ys

-- separateElement but only separates ';' in the last word
-- separateLastSemicolon ["select","*","from","data;"]
-- >> ["select","*","from","data",";"]
separateLastSemicolon :: [String] -> [String]
separateLastSemicolon [] = []
separateLastSemicolon (x : []) = separateElement x ';' ++ [] 
separateLastSemicolon (x : xs) = x : separateLastSemicolon xs

toLowerCase :: Char -> Char
toLowerCase c
 | 'A' <= c && c <= 'Z' = toEnum (fromEnum c + 32)
 | otherwise = c
 
columnName :: Column -> String
columnName (Column name _) = name

-- Extracts the type from a Column data structure
columnType :: Column -> ColumnType
columnType (Column _ t) = t

elemAt :: Int -> [a] -> Maybe a
elemAt idx xs
  | idx < 0 || idx >= length xs = Nothing
  | otherwise                   = Just (xs !! idx)
  
columnTypeByName :: String -> [Column] -> Maybe ColumnType
columnTypeByName colName cols = columnType <$> find (\(Column name _) -> name == colName) cols

caseInsensitiveString :: String -> P.Parsec String () String
caseInsensitiveString s = P.try (mapM caseInsensitiveChar s)
    where
        caseInsensitiveChar c = P.char (toLower c) P.<|> P.char (toUpper c)
  
-- Test Functions
testColumnNameParser :: String -> Either P.ParseError String
testColumnNameParser = P.parse columnNameParser ""

testColumnsListParser :: String -> Either P.ParseError [String]
testColumnsListParser = P.parse columnsListParser ""