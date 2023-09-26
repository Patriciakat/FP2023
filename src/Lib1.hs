{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

toLowerCase :: Char -> Char
toLowerCase c
 | 'A' <= c && c <= 'Z' = toEnum (fromEnum c + 32)
 | otherwise = c

comparer :: a -> [b] -> Boll 
comparer :: a

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName [] _ = Nothing
findTableByName _ [] = Nothing
findTableByName (x:xs) name 
  | fst x == dataFrameName = Just (snd x)
  | otherwise = findTableByName xs dataFrameName
    where 
      dataFrameName = map toLowerCase name



-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement "" = Left "Error - empty input"
parseSelectAllStatement input
  | length input' < 15 = Left "Error - invalid input"
  | map toLowerCase (take 14 input') /= "select * from " = Left "Error - invalid input"
  | otherwise = Right (drop 14 input')
    where
      input' = case last input of
        ';' -> take (length input - 1) input
        _ -> input

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame columns rows)
  | 

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
