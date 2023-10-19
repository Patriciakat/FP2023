{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame)
import InMemoryTables (TableName)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = StatementWithFilters { columns :: [String], database :: String, filters :: [String] } --for: select _____ from _____ where _____
  | StatementWithoutFilters { columns :: [String], database :: String }                   --for: select _____ from _____
  | StatementSelectAll { database :: String }                                             --for: select * from _____
  | StatementShowTableName {statement :: String, name :: String}
  | StatementShowTables { statement :: String}

-- Parses user input into an entity representing a parsed statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement _ = Left "Not implemented: parseStatement"
-- parseStatement statement = case components of                         --sitos savo parasytos parseStatement dalies visiskai netestavau
--   last components /= ";" = Left "Missing ';' at the end"
--   length components == 2 -> if map toLowerCase y == "showtables" then Right StatementShowTables "showTables" else Left "Invalid query"
--   length components == 3 -> if map toLowerCase y == "showtable" then Right StatementShowTableName "showTableName" (head ys) else Left "Invalid query"
--   _ -> do --IDK
--   where
--     components = separateLastSemicolon . splitToComponents $ statement
--     y = head components
--     ys = tail components

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"


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
-- splitToComponents "gass, grass,brass,,sash"
-- >> ["gass" "," "grass" "," "brass" "," "," "sash"]
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