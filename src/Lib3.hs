{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    serializeDataFrame,
    deserializeDataFrame,
    readDataFrameFile,
    readDataFrame,
    saveDataFrame,
  )
where

import qualified Lib2
import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Aeson (encode, decode)
import qualified Data.ByteString.Lazy as B
import Data.Time (UTCTime)

type TableName = String
type FileContent = B.ByteString
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName FileContent next
  | ReadFile FilePath (Maybe DataFrame -> next)
  | GetTime (UTCTime -> next)
  deriving (Functor)

type Execution = Free ExecutionAlgebra

-- Function to execute SQL commands
executeSql :: String -> [(TableName, DataFrame)] -> Execution (Either ErrorMessage DataFrame)
executeSql sql db = do
    currentTime <- getTime  -- Get current time within the Execution monad
    return $ case Lib2.parseStatement sql of
        Right statement -> Lib2.executeStatement statement db
        Left errorMsg -> Left errorMsg

-------------------------------------------- Serialization functions----------------------------------------------------

serializeDataFrame :: DataFrame -> B.ByteString
serializeDataFrame = encode

saveDataFrame :: TableName -> DataFrame -> Execution ()
saveDataFrame tableName df = liftF $ SaveFile tableName (serializeDataFrame df) ()
  
-------------------------------------------- Deserialization functions--------------------------------------------------

deserializeDataFrame :: B.ByteString -> Maybe DataFrame
deserializeDataFrame = decode

readDataFrameFile :: FilePath -> Execution (Maybe DataFrame)
readDataFrameFile filePath = liftF $ ReadFile filePath id

readDataFrame :: FilePath -> IO (Maybe DataFrame)
readDataFrame filePath = do
  jsonContent <- B.readFile filePath
  return (deserializeDataFrame jsonContent)

getTime :: Execution UTCTime
getTime = liftF $ GetTime id