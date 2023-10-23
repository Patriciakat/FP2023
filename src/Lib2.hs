{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame
import DataFrame (DataFrame(..))
import Data.List (elemIndex)
import Data.Maybe (isJust, mapMaybe)
import InMemoryTables (TableName)
import qualified Text.Parsec as P
import Data.Maybe (isJust, mapMaybe, catMaybes)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
    = ShowTables
    | ShowTable TableName
    | SelectFrom { selectedColumns :: [String], fromTable :: TableName }
    | StatementWithFilters { columns :: [String], database :: String, filters :: [String] }
    | StatementWithoutFilters { columns :: [String], database :: String }
    | StatementSelectAll { database :: String }
    deriving (Show, Eq)
    
selectStarParser :: P.Parsec String () ParsedStatement
selectStarParser = do
    _ <- P.string "SELECT"
    _ <- P.spaces
    _ <- P.char '*'
    _ <- P.spaces
    _ <- P.string "FROM"
    _ <- P.spaces
    tablename <- P.many1 (P.alphaNum P.<|> P.char '_')
    return $ StatementSelectAll tablename

    
elemAt :: Int -> [a] -> Maybe a
elemAt idx xs
  | idx < 0 || idx >= length xs = Nothing
  | otherwise                   = Just (xs !! idx)
    
parseSelectStar :: String -> Either P.ParseError ParsedStatement
parseSelectStar = P.parse selectStarParser ""

-- Parses user input into an entity representing a parsed statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
    case parseSelectStar input of
        Left err -> Left $ show err
        Right stmt -> Right stmt
    
-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement (SelectFrom columns tableName) db =
    case lookup tableName db of
        Just (DataFrame allColumns allRows) -> 
            let indices = catMaybes $ map (\col -> elemIndex col (map columnName allColumns)) columns
            in if length indices == length columns  -- Ensuring all columns are found
               then let selectedCols = map (\idx -> allColumns !! idx) indices
                        selectedRows = map (\row -> [ row !! idx | idx <- indices ]) allRows
                    in Right $ DataFrame selectedCols selectedRows
               else Left "Some columns not found"
        Nothing -> Left "Table not found"

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